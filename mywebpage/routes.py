#https://learn.microsoft.com/en-us/training/entra-external-identities/1-introduction
#https://www.facebook.com/JTCguitar
#https://www.facebook.com/guitarsalon



# await redis.publish(
#     "chatbot:pending_allocations",
#     json.dumps({
#         "type": "pending_allocation",
#         "tab_mode": tab_mode,
#         "user_id": user_id,
#         "message": user_message
#         "timestamp": created_at,
#         "latitude": latitude,
#         "longitude": longitude,
#         "location": location,
#     })
# )

# await redis.setex(
#     f"pending:{user_id}:{temp_message_id}",
#     60,  # seconds to live
#     json.dumps({
#         "user_id": user_id,
#         "org_id": client_id,
#         "message": user_message,
#         "tab_mode": tab_mode,
#         "timestamp": created_at,
#         "latitude": latitude,
#         "longitude": longitude,
#         "location": location,
#     })
# )

# FastAPI
from fastapi import FastAPI, APIRouter, Query, Depends, Request, Form, HTTPException, status, File, UploadFile
from fastapi.responses import RedirectResponse, JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi_login import LoginManager
from fastapi_login.exceptions import InvalidCredentialsException
from azure.storage.blob.aio import BlobServiceClient
# Imports from mywebpage
from mywebpage.datatransformation_v2 import datatransformation_for_chartjs
from mywebpage.datatransformation_v2_weekly import datatransformation_for_chartjs_weekly
from mywebpage.datatransformation_detaileduserdata import datatransformation_for_chartjs_detailed, build_coords_from_sources_async
from jwt.algorithms import RSAAlgorithm
from mywebpage.chats import fetch_topic_classification_counts
# Socket.IO (your ASGI version, not flask-socketio)
from mywebpage.socketio_app import sio  
from urllib.parse import parse_qs
# Database session
from mywebpage.db import async_session_scope
from sqlalchemy import text, select, desc
from mywebpage.concurrency import run_cpu_task
# Mail (replace flask_mail → fastapi-mail)
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig, MessageType
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from urllib.parse import quote
import os
import secrets
import json
import jwt
from sqlalchemy.future import select
import asyncio
import logging
from opentelemetry.sdk._logs import LoggingHandler
import uuid
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature
import httpx
from fastapi import HTTPException
from sqlalchemy.orm import selectinload
from fastapi.responses import PlainTextResponse
from mywebpage.elephantsql import Client, Subscription, SubscriptionPrice, ClientBehaviorHistory, ClientConfigHistory, Role, User, OrgEventLog, update_client_mode, enrich_event_with_local_timestamp
from mywebpage.chats import fetch_chat_messages
from datetime import datetime, timedelta 
from mywebpage.mainpulation_weeklyreport import user_querry_forquickreview, locationranking, longitude_latitude, longitude_latitude_detailed, fetch_chat_messages_weekly
import json
import os
from sqlalchemy import func, delete, update
from sqlalchemy.exc import SQLAlchemyError
from itsdangerous import SignatureExpired, BadSignature
from typing import Optional, List
import jwt
from mywebpage.security import CsrfProtect
import json
from urllib.parse import urlencode
import secrets
import re
import time
from sqlalchemy.orm import joinedload
from datetime import datetime, timezone
import pytz
from pathlib import Path
import stripe


s = URLSafeTimedSerializer("your-secret-key")  # same as in invite_user,  # creates signed and tamper-proof url for registration for new users



stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")
BASE_DIR = Path(__file__).resolve().parent  # <- go up one level to match fastapi_app.py   !!! BASE_DIR = mywebpage/
print(BASE_DIR / "static")  # C:\Users\vbanai\Documents\Programming\Dezsi porject\ChatFrontEnd\FinalAdminPage_FASTAPI\mywebpage\static
templates = Jinja2Templates(directory=BASE_DIR / "templates")

router = APIRouter()
# API Router is kind of a route container
# instead: 
# @fastapi_app.get("/")
# def index():
    
# we do:
# @router.get("/")
# def index():

# in this way we can split app into files, cleaner

FLASH_EXPIRE_SECONDS = 60



# GMAIL configuration  in fastapi using pydanticv2
class MailSettings(BaseSettings):
    MAIL_USERNAME: str = os.environ.get("MAIL_USERNAME")
    MAIL_PASSWORD: str = os.environ.get("MAIL_PASSWORD")
    MAIL_FROM: str = "banaiviktor11@gmail.com"
    MAIL_PORT: int = 587
    MAIL_SERVER: str = "smtp.gmail.com"
    MAIL_STARTTLS: bool = True        # renamed field
    MAIL_SSL_TLS: bool = False        # renamed field
    USE_CREDENTIALS: bool = True      # keep credentials flag

mail_settings = MailSettings()

conf = ConnectionConfig(
    MAIL_USERNAME=mail_settings.MAIL_USERNAME,
    MAIL_PASSWORD=mail_settings.MAIL_PASSWORD,
    MAIL_FROM=mail_settings.MAIL_FROM,
    MAIL_PORT=mail_settings.MAIL_PORT,
    MAIL_SERVER=mail_settings.MAIL_SERVER,
    MAIL_STARTTLS=mail_settings.MAIL_STARTTLS,
    MAIL_SSL_TLS=mail_settings.MAIL_SSL_TLS,
    USE_CREDENTIALS=mail_settings.USE_CREDENTIALS,
)


async def send_email(
    subject: str,
    recipients: list[str],
    body: str
):
    message = MessageSchema(
        subject=subject,
        recipients=recipients,
        body=body,
        subtype=MessageType.plain  # or "html" if you want HTML emails
    )
    print(message)
    fm = FastMail(conf)
    await fm.send_message(message)

fast_mail = FastMail(conf)



SECRET = os.environ.get("SECRET_KEY", secrets.token_urlsafe(32))

#---------------   OAuth STATE ---------------

STATE_KEY_PREFIX = "oauth_state:"
SESSION_KEY_PREFIX = "user_session:"
SESSION_TTL = 24*3600   # 24 hour
SESSION_TTL_COOKIE=24*3600


async def save_oauth_state(redis, state: str, session_id: str):
    if redis:
        # Store the state for 5 minutes
        await redis.setex(f"{STATE_KEY_PREFIX}{session_id}", 300, state)

async def load_oauth_state(redis, session_id: str) -> str | None:
    if redis:
        return await redis.get(f"{STATE_KEY_PREFIX}{session_id}")
    return None


#---------------   OAuth STATE ---------------



# Azure AD configuration
CLIENT_ID = os.environ.get("ENTRA_CLIENT_ID")
CLIENT_SECRET = os.environ.get("ENTRA_SECRET_VALUE")
TENANT_ID = os.environ.get("ENTRA_TENANT_ID")
AUTHORITY = f'https://login.microsoftonline.com/{TENANT_ID}'
#REDIRECT_URI = 'http://localhost:5000/auth/callback'  # Keep this consistent
REDIRECT_URI = 'https://c3a8-2001-4c4e-1e05-8300-71df-3500-5a95-da19.ngrok-free.app/auth/callback'

SCOPE = ['User.Read']  # Define your scopes here





async def get_current_user(request: Request) -> dict | None:

    session_id = request.cookies.get("session_id")
    print("REQUEST app id:", id(request.app))

    if not session_id:
        print("No session_id cookie")
        return None

    redis = getattr(request.app.state, "redis_client", None)
    if not redis:
        print("Redis not available")
        return None

    user_data = await redis.hgetall(f"session:{session_id}")
    if not user_data:
        print(f"No session data for session:{session_id}")
        return None


    # Convert IDs back to integers
    try:
        user_id = int(user_data.get("user_id")) if user_data.get("user_id") else None
        org_id = int(user_data.get("user_org")) if user_data.get("user_org") else None
    except ValueError:
        print("Invalid ID stored in Redis")
        return None
    
    return {
        "id": user_id,
        "org_id": org_id,
        "role": user_data.get("user_role"),
        "name": user_data.get("name"),
        "email": user_data.get("email"),
        "language": user_data.get("language") or "hu",
        "first_character": user_data.get("first_character"),
    }

async def login_required(user: dict = Depends(get_current_user)) -> dict:
    if not user:
        #Exception handling is in fastapi_app.py,
        raise HTTPException(status_code=401, detail="Not logged in")
    return user





#This is a factory function that returns a dependency function.
#Depends(get_current_user) automatically calls your get_current_user function and injects its return value into the user argument.
def role_required(*allowed_roles: str):
    async def dependency(user: dict = Depends(get_current_user)):
        if not user:
            raise HTTPException(status_code=401, detail="Not logged in")
        if user.get("role") not in allowed_roles:
            raise HTTPException(status_code=403, detail="Forbidden")
        return user
    return dependency


#----------------------
#       INDEX
#----------------------



@router.get("/routes")
async def list_routes(request: Request):
    routes = [route.name for route in request.app.routes]
    return {"routes": routes}

@router.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    user: dict | None = Depends(get_current_user)  # auto-fetch current user from session/Redis
):
    redis = request.app.state.redis_client
    session_id = request.cookies.get("session_id")



    # --- Flash message handling ---
    flash_message = None
    flash_id = request.query_params.get("flash_id")
    if flash_id and redis:
        raw = await redis.get(f"flash:{flash_id}")
        if raw:
            try:
                flash_message = raw.decode() if isinstance(raw, bytes) else str(raw)
                print("FLASH!!!!!!!!!!!!!!")
                print(flash_message)
                flash_message = json.loads(raw)
            except Exception:
                flash_message = None
            await redis.delete(f"flash:{flash_id}")


    First_character = None
    subscription = None
    service_message = None
    chat_control_access = metrics_access = advanced_ai_access = False
    user_role = None

   
    
    
    if session_id and not await redis.exists(f"session:{session_id}"):  #boolian false or true
            # Session expired → redirect to logout
            return RedirectResponse(
            url="/logout?reason=expired",
            status_code=302
            )
    
    if user:    
        First_character = user.get("first_character")
        user_role = user.get("role")
        language = user.get("language", "hu")

        async with async_session_scope() as db_session:
            # Fetch client + subscription
            client = await db_session.scalar(
                select(Client).options(joinedload(Client.subscription)).where(Client.id == user["org_id"])
            )
            if client:
                subscription = client.subscription
                service_message = None
            else:
                service_message = "Please contact Red Rain to select a service."

            # Permissions
            if subscription:
                chat_control_access = has_permission(user_role, subscription, "chat_control")
                metrics_access = has_permission(user_role, subscription, "chatbot_metrics")
                advanced_ai_access = has_permission(user_role, subscription, "advanced_ai")

        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "First_character": First_character,
                "logged_in": True,
                "subscription": subscription,
                "service_message": service_message,
                "chat_control_access": chat_control_access,
                "metrics_access": metrics_access,
                "advanced_ai_access": advanced_ai_access,
                "user_role": user_role,
                "flash_message": flash_message,
                "language": language
            },
        )

    # Non-logged-in branch
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "flash_message": flash_message,
            "logged_in": False,
        },
    )
#----------------------
#       MAPS
#----------------------

# @router.get("/mapdetailed", response_class=HTMLResponse)
# async def map_page_detailed(request: Request):
#     params = request.query_params
#     ip_data = await longitude_latitude_detailed(
#         params.get("year"),
#         params.get("month"),
#         params.get("day"),
#         params.get("hour"),
#         params.get("minutes"),
#         params.get("seconds"),
#         params.get("year_end"),
#         params.get("month_end"),
#         params.get("day_end"),
#         params.get("hour_end"),
#         params.get("minutes_end"),
#         params.get("seconds_end"),
#     )

#     cleaned_ip_data = [
#         entry for entry in ip_data if entry["location"]["lat"] is not None and entry["location"]["lng"] is not None
#     ]

#     return templates.TemplateResponse("map.html", {"request": request, "ip_data": cleaned_ip_data})
#same just safer and handling None:



# ----------------------------
# Query parameters validation You define a schema: these are the expected fields and their types. FastAPI automatically reads the query parameters from the request and creates a MapDetailedQuery instance.
# ----------------------------
class MapDetailedQuery(BaseModel):
    year: int
    month: int
    day: int
    hour: int
    minutes: int
    seconds: int
    year_end: int
    month_end: int
    day_end: int
    hour_end: int
    minutes_end: int
    seconds_end: int

# ----------------------------
# MAP
# ----------------------------
@router.get("/mapdetailed", response_class=HTMLResponse)
async def map_page_detailed(
    request: Request,
    params: MapDetailedQuery = Depends(),
    user: dict = Depends(login_required),
):
    from_ = datetime(
        params.year, params.month, params.day,
        params.hour, params.minutes, params.seconds,
        tzinfo=pytz.UTC
    )
    to_ = datetime(
        params.year_end, params.month_end, params.day_end,
        params.hour_end, params.minutes_end, params.seconds_end,
        tzinfo=pytz.UTC
    )

    if to_ < from_:
        raise HTTPException(status_code=400, detail="Invalid time range: end is before start")

    redis = request.app.state.redis_client
    cpu_pool=request.app.state.cpu_pool
    cpu_sem=request.app.state.cpu_sem
    # Fetch coordinates in memory-efficient way
    ip_data = await build_coords_from_sources_async(
        client_id=user["org_id"],
        start_dt=from_,
        end_dt=to_,
        table_name="chat_messages",
        redis=redis,
        cpu_pool=cpu_pool,
        cpu_sem=cpu_sem,
        chunk_size=10_000,
        ttl_seconds=3600
    )

    if not ip_data:
        # Optional: handle no coordinates found
        ip_data = []

    return templates.TemplateResponse("map.html", {
        "request": request,
        "ip_data": ip_data
    })



@router.get("/map", response_class=HTMLResponse)
async def map_page(request: Request):
    redis = request.app.state.redis_client
    cpu_pool=request.app.state.cpu_pool
    cpu_sem=request.app.state.cpu_sem
    if not cpu_pool or not cpu_sem:
        return HTMLResponse("Server not ready (CPU pool not available)", status_code=503)
    if not redis:
        return HTMLResponse("Server not ready (Redis not available)", status_code=503)

    session_id = request.cookies.get("session_id")
 

    user = await get_current_user(request)
    if not user:
        return templates.TemplateResponse("map.html", {"request": request, "ip_data": []})

    ip_data = await longitude_latitude(user["org_id"], redis, cpu_pool=cpu_pool, cpu_sem=cpu_sem,)
    cleaned_ip_data = [
        entry for entry in ip_data if entry["location"]["lat"] is not None and entry["location"]["lng"] is not None
    ]

    return templates.TemplateResponse("map.html", {"request": request, "ip_data": cleaned_ip_data})


#----------------------
#   SERVICE SELECTOR
#----------------------

def has_permission(user_role, subscription, feature):
    if not subscription:
        return False

    permissions = {
        "chat_control": (subscription.can_access_chat_control and user_role in ["Manager", "Team Leader", "Administrator"]),
        "basic_metrics": (subscription.can_access_basic_metrics and user_role in ["Manager", "Team Leader"]),
        "enhanced_metrics": (subscription.can_access_enhanced_metrics and user_role in ["Manager", "Team Leader"]),
        "advanced_ai": (subscription.can_access_advanced_ai and user_role in ["Manager", "Team Leader"]),
    }
    return permissions.get(feature, False)



@router.get("/serviceselector", response_class=HTMLResponse)
async def serviceselector_vbanai(
    request: Request,
    csrf_protect: CsrfProtect=Depends(),
    current_user: dict = Depends(login_required),  # ensures user is logged in
):
    # redis = request.app.state.redis_client
    # session_id = request.cookies.get("session_id")
    # session_key = f"session:{session_id}"

    # # If session expired in Redis → logout
    # if not await redis.exists(session_key):
    #     return RedirectResponse(
    #     url="/logout?reason=expired",
    #     status_code=302
    # )
    

    email = current_user["email"]
    email_prefix = email.split("@")[0] if email else ""
    user_id = current_user["id"]
    user_org = current_user["org_id"]
    name = current_user["name"]
    user_role = current_user["role"]
    language = current_user.get("language", "hu")
    first_character = current_user.get("first_character") 
 
  

    # Get the company name associated with the user from the Client table
    #client = Client.query.filter_by(client_name=current_user.client_id).first()
    async with async_session_scope() as db_session:
        client = await db_session.scalar(
            select(Client)
            .options(joinedload(Client.subscription))
            .where(Client.id == user_org)
        )

        if client:
            client_name = client.client_name
            subscription = client.subscription
            service_message = None
        else:
            client_name = "Your Company"
            subscription = None
            service_message = "Please contact Red Rain to select a service."

        
        # Check access permissions based on the subscription
        if subscription:
            chat_control_access = has_permission(user_role, subscription, "chat_control")
            basic_metrics_access = has_permission(user_role, subscription, "basic_metrics")
            enhanced_metrics_access = has_permission(user_role, subscription, "enhanced_metrics")
            advanced_ai_access = has_permission(user_role, subscription, "advanced_ai")
        else:
            chat_control_access = False
            basic_metrics_access = False
            enhanced_metrics_access = False
            advanced_ai_access = False
        
        #csrf_token = csrf_protect.csrf_token()
        #csrf_token = csrf_protect.create_csrf()
        csrf_token, signed_token = csrf_protect.generate_csrf_tokens()
  

        # Additional session-related data
        response = templates.TemplateResponse(
        "serviceselector_vbanai.html",
        {
            "request": request,
            "user_role": user_role,
            "First_character": first_character,
            "email_prefix": email_prefix,
            "name": name,
            "client_name": client_name,
            "subscription": subscription,
            "service_message": service_message,
            "chat_control_access": chat_control_access,
            "basic_metrics_access": basic_metrics_access,
            "enhanced_metrics_access": enhanced_metrics_access,
            "advanced_ai_access": advanced_ai_access,
            "language": language,
            "user_id": user_id,
            "csrf_token": csrf_token,  # <-- pass CSRF to template
        },
    )
    csrf_protect.set_csrf_cookie(signed_token, response)  # <-- set cookie
    return response




def is_valid_email(email):
    # Define a regex pattern for validating an Email
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_pattern, email) is not None


@router.get("/get_users")
async def get_users(
        request: Request,
        user: dict = Depends(login_required),  # ensures user is logged in
    ):
    client_id = user.get("org_id")
    redis = request.app.state.redis_client

    session_id = request.cookies.get("session_id")

    if not await redis.exists(f"session:{session_id}"):  #boolian false or true
            # Session expired → redirect to logout
        return RedirectResponse(
        url="/logout?reason=expired",
        status_code=302
    )



    users_data = []
    async with async_session_scope() as db_session:
        result = await db_session.execute(
            select(User).options(joinedload(User.role)).where(User.client_id == client_id, User.is_deleted == False)
        )
        users = result.scalars().all()

        for u in users:
            redis_key = f"online:{client_id}:{u.id}"
            is_online = await redis.exists(redis_key) == 1
            users_data.append({
                "id": u.id,
                "email": u.email,
                "name": u.name or "",
                "role": u.role.role_name if u.role else "No Role",
                "is_online": is_online
            })
    print(users)
    print("  * * *  ")
    print(users_data)
    return JSONResponse(users_data)



# ---- helper for manager-dashboard route

def msg(text_en: str, text_hu: str, category: str, lang: str):
    return {
        "text": text_hu if lang == "hu" else text_en,
        "category": category
    }


ROLE_TRANSLATION_MAP = {
    "Menedzser": "Manager",
    "Csopotvezető": "Team Leader",
    "Adminisztrátor": "Administrator"
}

EMAIL_TEMPLATES = {
    "invite": {
        "subject": {
            "en": "Account Registration",
            "hu": "Fiók regisztráció"
        },
        "body": {
            "en": (
                "Hello {name},\n\n"
                "Please complete your registration by clicking the link below:\n"
                "{link}\n\n"
                "This link is valid for 3 days."
            ),
            "hu": (
                "Kedves {name}!\n\n"
                "Kérjük, fejezd be a regisztrációt az alábbi linkre kattintva:\n"
                "{link}\n\n"
                "A link 3 napig érvényes."
            )
        }
    },
    "role_change":{
        "subject": {
        "en": "Access Change Notification",
        "hu": "Jogosultság változás"
    },
    "body": {
        "en": (
            "Hello,\n\n"
            "Your access has been updated from '{old_role}' to '{new_role}'."
        ),
        "hu": (
            "Üdvözlünk!\n\n"
            "A jogosultságod megváltozott:\n"
            "Régi jogosultság: '{old_role}'\n"
            "Új jogosultság: '{new_role}'."
        )
    }
        
  },
  "remove_user":{
      "subject": {
        "en": "Your account has been deleted",
        "hu": "Töröltük a fiókod"
    },
    "body": {
        "en": "Hello,\n\nYour account has been removed by the manager.",
        "hu": "Üdvözlünk!\n\nA fiókodat töröltük."
    }
  }
      
  

    
}

# ----------------------------------


@router.post("/manager-dashboard")
async def manager_dashboard(
    request: Request,
    csrf_protect: CsrfProtect = Depends(), # Depends() is FastAPI’s way of saying: Before running this route, call some other function/class and inject its return value here.
    #csrf_token: str = Form(...), # Form(...) REGUIRED, with no default means required. If the CSRF token is missing, FastAPI rejects the request before your route runs.
    #form_type: str | None = Form(None),   # OPTIONAL, form_type: str | None = Form(None),   # It looks inside the submitted form (request.form() under the hood). It tries to find a field called "form_type". If it exists, it converts it to a str and gives it to you in the parameter form_type. If it doesn’t exist, it uses the default (None in your case).
    current_user: dict = Depends(login_required),  # ensures user is logged in
):
  # Ensure that only users with the "Manager" role can access this page
  # if current_user.role != 'Team Leader' or current_user.role != 'Manager':
  #   flash("You do not have permission to access this page.", 'danger')
  #   return redirect(url_for('index'))
  await csrf_protect.validate_csrf(request)

  # csrf token validation here we block the attacker - user legitimatly can submits the form if token matches
  
  form = await request.form()
  form_type = form.get("form_type")
  lang=current_user.get("language", "hu")
  if lang not in ["en", "hu"]:
      print("[DEBUG] Invalid lang detected, defaulting to 'hu'")
      lang = "hu"
  
  messages = []

  if form_type == "invite_user":
      try:
          # Collect all fields
          
          emails = form.get("emails")
          names = form.get("names", "")
          raw_role_name = form.get("role")
          role_name = ROLE_TRANSLATION_MAP.get(raw_role_name, raw_role_name)
          print("Raw role from form:", raw_role_name)
          print("Normalized role for DB query:", role_name)
          email_list = [e.strip() for e in emails.replace(',', '\n').splitlines() if e.strip()]
          name_list = [n.strip() for n in names.split(',') if n.strip()]
          print("[DEBUG] Form raw_role_name:", raw_role_name)
          print(email_list, name_list)
          if len(email_list) != len(name_list):
              messages.append(
                  msg(
                      text_en="The number of names must match the number of emails.",
                      text_hu="Ugyanannyi email címet kell beírnod, ahány nevet választottál!",
                      category="danger",
                      lang=lang
                  )
              )
              print("[DEBUG] Email/name count mismatch")
              return JSONResponse({"messages": messages, "success": False})
          
          async with async_session_scope() as db_session:
              # Find role
              result = await db_session.execute(
                  select(Role).where(func.lower(Role.role_name) == role_name.lower())
              )
              role = result.scalar_one_or_none()
              print("[DEBUG] Role found:", role)
              if not role:
                  messages.append(
                      msg(
                          text_en="Invalid role selected.",
                          text_hu="Érvénytelen munkakört választottál.",
                          category="danger",
                          lang=lang
                      )
                  )

              new_users = []
              for email, name in zip(email_list, name_list):
                  print(f"[DEBUG] Processing user: {email} / {name}")
                  if not is_valid_email(email):
                      messages.append(
                          msg(
                              text_en=f"The email {email} is not valid.",
                              text_hu=f"Az email cím {email} nem érvényes.",
                              category="danger",
                              lang=lang
                          )
                      )
                      continue

                  result = await db_session.execute(
                        select(User).where(User.email == email)
                    )
                  existing_user = result.scalar_one_or_none()

                  # ─────────────────────────────────────────────
                  # CASE 2: user exists and already activated
                  # ─────────────────────────────────────────────

                  if existing_user and existing_user.is_active:
                      messages.append(
                          msg(
                              text_en=f"The email {email} is already registered.",
                              text_hu=f"Az email címet {email} már regisztráltad.",
                              category="info",
                              lang=lang
                          )
                      )
                      print(f"[DEBUG] Email already exists: {email}")
                      continue
                
                  # ─────────────────────────────────────────────
                  # CASE 3: user exists but NOT activated → resend
                  # ─────────────────────────────────────────────
                  if existing_user and not existing_user.is_active:
                      token = s.dumps(
                          {"email": email, "lang": lang},
                          salt="email-confirm",
                      )
                      registration_link = (
                          f"https://redrain1230.loophole.site/register/confirm?token={token}"
                      )
                      # here await send_email is fine: That means awaiting send_email() does not interfere with SQLAlchemy, because the session state is essentially read-only.
                      try:
                          await send_email(
                              subject=EMAIL_TEMPLATES["invite"]["subject"][lang],
                              recipients=[email],
                              body=EMAIL_TEMPLATES["invite"]["body"][lang].format(
                                  name=existing_user.name,
                                  link=registration_link,
                              ),
                          )

                          messages.append(
                              msg(
                                  f"Invitation resent to {email}.",
                                  f"A meghívó újraküldve: {email}.",
                                  "success",
                                  lang,
                              )
                          )
                          print(f"[DEBUG] Invitation resent: {email}")

                      except Exception as e:
                          messages.append(
                              msg(
                                  f"Failed to resend invitation to {email}: {str(e)}",
                                  f"Nem sikerült újraküldeni a meghívót: {email}.",
                                  "danger",
                                  lang,
                              )
                          )
                      continue
                
                  # ─────────────────────────────────────────────
                  # CASE 1: brand new user
                  # ─────────────────────────────────────────────
                  new_user = User(
                      email=email,
                      name=name,
                      client_id=current_user["org_id"],
                      role_id=role.id,
                      is_active=False
                  )
                  new_users.append(new_user)
                  print(f"[DEBUG] New user object created: {new_user}")

                  # Token + registration link
                  token = s.dumps({'email': email, 'lang': lang}, salt="email-confirm")
                  registration_link = f"https://redrain1230.loophole.site/register/confirm?token={token}"
                  print(f"[DEBUG] Registration link for {email}: {registration_link}")


                  try:
                      await send_email(
                          subject=EMAIL_TEMPLATES["invite"]["subject"][lang],
                          recipients=[email],
                          body=EMAIL_TEMPLATES["invite"]["body"][lang].format(
                              name=name,
                              link=registration_link
                          )
                      )

                      messages.append(
                          msg(
                              text_en=f"User invited successfully: {email} ({name}).",
                              text_hu=f"Sikeresen meghívtad: {email} ({name}).",
                              category="success",
                              lang=lang
                          )
                      )
                      print(f"[DEBUG] Email sent successfully to: {email}")

                  except Exception as e:
                      messages.append(
                          msg(
                              text_en=f"An error occurred while sending the email to {email}: {str(e)}.",
                              text_hu=f"Hiba történt az email küldése közben: {email}.",
                              category="danger",
                              lang=lang
                          )
                      )

              if new_users:
                  try:
                      db_session.add_all(new_users)
                     
                  except SQLAlchemyError as e:
                      await db_session.rollback()
                      messages.append(
                          msg(
                              text_en=f"An error occurred while saving users: {str(e)}",
                              text_hu="Hiba történt a mentés közben.",
                              category="danger",
                              lang=lang
                          )
                      )

      except Exception as e:
          messages.append(
              msg(
                  text_en=f"An error occurred: {str(e)}.",
                  text_hu=f"Hiba történt!: {str(e)}.",
                  category="danger",
                  lang=lang
              )
          )
      print("ide eljön????")
      success = not any(m['category'] == 'danger' for m in messages)
      return JSONResponse({'messages': messages, 'success': success})

   
  elif form_type == "manage_user":
      try:
          # emails = form.get('emails')
          # role_name = form.get('role')
          selected_user_email = form.get("selected_user")
          selected_role_name = form.get("selected_role")

          if not selected_user_email or not selected_role_name:
              messages.append(
                  msg(
                      text_en="Please select both a user and a role.",
                      text_hu="Felhasználót és pozíciót is választanod kell!",
                      category="danger",
                      lang=lang
                  )
              )
              return JSONResponse({"messages": messages, "success": False})

          # --- DB operations ---
          async with async_session_scope() as db_session:
              result = await db_session.execute(
                  select(User)
                  .options(selectinload(User.role))
                  .where(User.email == selected_user_email)
              )
              user = result.scalar_one_or_none()
              # pl.: user = User(
              #     id=1,
              #     email="alice@example.com",
              #     name="Alice",
              #     role_id=2,
              #     is_active=True,
              #     role=Role(id=2, role_name="Team Leader")  # loaded because of selectinload
              # )

              if not user:
                  messages.append(
                      msg(
                          text_en="User not found.",
                          text_hu="A felhasználó nincs benne az adatbázisba.",
                          category="danger",
                          lang=lang
                      )
                  )
                  return JSONResponse({"messages": messages, "success": False})
   
              
              old_role = user.role.role_name if user.role else "No Role"
              result_role = await db_session.execute(
                  select(Role).where(Role.role_name == selected_role_name)
              )
              role = result_role.scalar_one_or_none()
              if not role:
                  messages.append(
                      msg(
                          text_en="Invalid role selected.",
                          text_hu="Nem megfelelő pozíciót választottál.",
                          category="danger",
                          lang=lang
                      )
                  )
                  return JSONResponse({"messages": messages, "success": False})
              user.role_id = role.id
              user_email = user.email

          # --- Outside the session: safe to do async IO ---
          # as the session closes and safle can start another async IO, in invitation we have first the email send and after the db modfication
          try:
              await send_email(
                  subject=EMAIL_TEMPLATES["role_change"]["subject"][lang],
                  recipients=[user_email],
                  body=EMAIL_TEMPLATES["role_change"]["body"][lang].format(
                      old_role=old_role,
                      new_role=selected_role_name
                  )
              )
          except Exception as e:
              # Email failure should not block role change
              print(f"Error sending role-change email: {e}")

          messages.append(
              msg(
                  text_en=f"User {user_email} role updated to {selected_role_name}.",
                  text_hu=f"A felhasználó {user_email} pozíciója frissítve lett: {selected_role_name}.",
                  category="success",
                  lang=lang
              )
          )

      except Exception as e:
          messages.append(
              msg(
                  text_en=f"Error: {str(e)}",
                  text_hu=f"Hiba történt: {str(e)}",
                  category="danger",
                  lang=lang
              )
          )

      # Determine success based on messages
      success = not any(m['category'] == 'danger' for m in messages)
      return JSONResponse({'messages': messages, 'success': success})

      
  elif form_type == "remove_user":  
      try:
         
          emails = form.get('emails')
          role_name = form.get('role')
          selected_user_email = form.get("selected_user")


          if not selected_user_email:
              messages.append(
                  msg(
                      text_en="Please select a user to remove.",
                      text_hu="Válaszd ki a munkatársat, akit törölni akarsz!",
                      category="danger",
                      lang=lang
                  )
              )
              return JSONResponse({'messages': messages, 'success': False})

          async with async_session_scope() as db_session:
              result = await db_session.execute(
                  select(User).where(User.email == selected_user_email)
              )
              user = result.scalar_one_or_none()

              if user:
                  user.is_deleted = True
                  user.deleted_at = datetime.utcnow()
                  user.is_active = False
              else:
                  messages.append(
                    msg(
                        text_en="User not found.",
                        text_hu="A felhasználó nem található.",
                        category="danger",
                        lang=lang
                    )
                )

                  
          if user:
              try:
                  await send_email(
                      subject=EMAIL_TEMPLATES["remove_user"]["subject"][lang],
                      recipients=[user.email],
                      body=EMAIL_TEMPLATES["remove_user"]["body"][lang]
                  )
              except Exception as e:
                  print(f"Error sending removal email: {e}")

              messages.append(
                  msg(
                      text_en=f"User {user.email} removed successfully.",
                      text_hu=f"A felhasználó {user.email} törölve lett.",
                      category="success",
                      lang=lang
                  )
              )

             
      except Exception as e:
        messages.append(
            msg(
                text_en=f"Error: {str(e)}",
                text_hu=f"Hiba történt: {str(e)}",
                category="danger",
                lang=lang
            )
        )
      success = not any(m['category'] == 'danger' for m in messages)
      return JSONResponse({'messages': messages, 'success': success})
  


@router.get("/design", response_class=HTMLResponse)
async def design_page(
    request: Request,
    first_character: str | None = Query(None),
    user: dict = Depends(login_required),  # ensures user is logged in
):
    

    language = user.get("language", "hu")

    user_id = user["id"]
    user_org = user["org_id"]
    user_role = user["role"]
    
    if not first_character:
        first_character = user.get("first_character") 
 
  

    # Get the company name associated with the user from the Client table
    #client = Client.query.filter_by(client_name=current_user.client_id).first()
    async with async_session_scope() as db_session:
        client = await db_session.scalar(
            select(Client)
            .options(joinedload(Client.subscription))
            .where(Client.id == user_org)
        )

        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        

       
        client_name = client.client_name
        subscription = client.subscription
    
        
        # Check access permissions based on the subscription
        if subscription:
            chat_control_access = has_permission(user_role, subscription, "chat_control")
            metrics_access = has_permission(user_role, subscription, "chatbot_metrics")
            advanced_ai_access = has_permission(user_role, subscription, "advanced_ai")
        else:
            # Default to no access if no subscription is found
            chat_control_access = False
            metrics_access = False
            advanced_ai_access = False
        
      
       


    
  
    # Determine icon path
    icon_path = client.icon_path 
    use_google_icon = False
    if not icon_path:
        use_google_icon = True
     

    # Build template context
    context = {
        "request": request,
  
        "client_id_initial": client.client_code,
        "client_mode": client.mode,
        "primary_color": client.primary_color,
        "reply_bg_color": client.reply_bg_color,
        "operator_icon": client.operator_icon,
        "font_color": client.font_color,
        "header_font_weight": client.header_font_weight,
        "header_font_size": client.header_font_size,
        "general_body_font_size": client.general_body_font_size,
        "general_body_font_size2": client.general_body_font_size2,
        "language": language,
        "language_selector":client.language_selector,
        "language_hu_logo_text": client.language_hu_logo_text,
        "language_en_logo_text": client.language_en_logo_text,
        "greeting_message_hu": client.greeting_message_hu,
        "greeting_message_en": client.greeting_message_en,
        "agent_request_confirmation_hu": client.agent_request_confirmation_hu,
        "agent_request_confirmation_en": client.agent_request_confirmation_en,

        "languages":client.languages,
        "everything_which_is_white": client.everything_which_is_white,
        "user_input_message_color": client.user_input_message_color,
        "popup_bg_color": client.popup_bg_color,
        "footer_bg_color": client.footer_bg_color,
        "footer_controls_bg": client.footer_controls_bg,
        "footer_input_bg_color": client.footer_input_bg_color,
        "footer_focus_outline_color": client.footer_focus_outline_color,
        "scrollbar_color": client.scrollbar_color,
        "border_radius": client.border_radius,
        "border_width": client.border_width,
        "border_color": client.border_color,
        "confirmation_button_bgcolor":client.confirmation_button_bgcolor,
        "icon_path": icon_path, 
        "use_google_icon": use_google_icon,
        "agent_icon": client.agent_icon,
        "emoji_icon": client.emoji_icon,
        "attachment_icon": client.attachment_icon,
        "font_general": client.font_general,
        "font_header_text": client.font_header_text,
        "first_character": first_character,

        "user_role": user_role,
        "client_name": client_name,
        "subscription": subscription,
        "chat_control_access": chat_control_access,
        "metrics_access": metrics_access,
        "advanced_ai_access": advanced_ai_access,
        "user_id": user_id,
    }

    return templates.TemplateResponse("graphic_design.html", context)


@router.get("/popup_settings", response_class=HTMLResponse)
async def popup_settings(
    request: Request,
    first_character: str | None = Query(None),
    user: dict = Depends(login_required),  # ensures user is logged in
):
    redis = request.app.state.redis_client
    session_id = request.cookies.get("session_id")
   
    language = user.get("language", "hu")

    user_id = user["id"]
    user_org = user["org_id"]
    user_role = user["role"]
    
    if not first_character:
        first_character = user.get("first_character")
    

    async with async_session_scope() as db_session:
        client = await db_session.scalar(
            select(Client)
            .options(joinedload(Client.subscription))
            .where(Client.id == user_org)
        )

        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        

       
        client_name = client.client_name
        subscription = client.subscription
    
        
        # Check access permissions based on the subscription
        if subscription:
            chat_control_access = has_permission(user_role, subscription, "chat_control")
            metrics_access = has_permission(user_role, subscription, "chatbot_metrics")
            advanced_ai_access = has_permission(user_role, subscription, "advanced_ai")
        else:
            # Default to no access if no subscription is found
            chat_control_access = False
            metrics_access = False
            advanced_ai_access = False
        
      
  
    # Determine icon path
    icon_path = client.icon_path 
    use_google_icon = False
    if not icon_path:
        use_google_icon = True


    print("chat_control_access", chat_control_access, "metrics_access", metrics_access, "advanced_ai_access", advanced_ai_access)
     
   
    # Build template context
    context = {
        "request": request,
        "user_id": str(client.id),
        "client_id_initial": client.client_code,
        "client_mode": client.mode,
        "primary_color": client.primary_color,
        "reply_bg_color": client.reply_bg_color,
        "operator_icon": client.operator_icon,
        "font_color": client.font_color,
        "header_font_weight": client.header_font_weight,
        "header_font_size": client.header_font_size,
        "general_body_font_size": client.general_body_font_size,
        "general_body_font_size2": client.general_body_font_size2,
        "language": language,
        "language_selector":client.language_selector,
        "language_hu_logo_text": client.language_hu_logo_text,
        "language_en_logo_text": client.language_en_logo_text,
        "greeting_message_hu": client.greeting_message_hu,
        "greeting_message_en": client.greeting_message_en,
        "agent_request_confirmation_hu": client.agent_request_confirmation_hu,
        "agent_request_confirmation_en": client.agent_request_confirmation_en,

        "languages":client.languages,
        "everything_which_is_white": client.everything_which_is_white,
        "user_input_message_color": client.user_input_message_color,
        "popup_bg_color": client.popup_bg_color,
        "footer_bg_color": client.footer_bg_color,
        "footer_controls_bg": client.footer_controls_bg,
        "footer_input_bg_color": client.footer_input_bg_color,
        "footer_focus_outline_color": client.footer_focus_outline_color,
        "scrollbar_color": client.scrollbar_color,
        "border_radius": client.border_radius,
        "border_width": client.border_width,
        "border_color": client.border_color,
        "confirmation_button_bgcolor":client.confirmation_button_bgcolor,
        "icon_path": icon_path, 
        "use_google_icon": use_google_icon,
        "agent_icon": client.agent_icon,
        "emoji_icon": client.emoji_icon,
        "attachement_icon": client.attachment_icon,
        "font_general": client.font_general,
        "font_header_text": client.font_header_text,
        "first_character": first_character,

        "user_role": user_role,
        "First_character": first_character,
        "client_name": client_name,
        "subscription": subscription,
        "chat_control_access": chat_control_access,
        "metrics_access": metrics_access,
        "advanced_ai_access": advanced_ai_access,
        "user_id": user_id,
    }

    return templates.TemplateResponse("popup_settings.html", context)





#########################################################################
#    MANAGET SAVING RESTORING EDITED POPUP DESING, TEXT FONT ELEMENTS   #
#########################################################################





class ClientConfigPayload(BaseModel):
    primary_color: Optional[str]
    border_radius: Optional[str]
    border_width: Optional[str]
    border_color: Optional[str]

    reply_bg_color: Optional[str]
    operator_icon: Optional[str]
    font_color: Optional[str]
    everything_which_is_white: Optional[str]
    user_input_message_color: Optional[str]

    popup_bg_color: Optional[str]
    footer_bg_color: Optional[str]
    footer_controls_bg: Optional[str]
    footer_input_bg_color: Optional[str]
    footer_focus_outline_color: Optional[str]
    confirmation_button_bgcolor: Optional[str]

    scrollbar_color: Optional[str]





@router.post("/api/client/config/manage_previous_layout")
async def manage_previous_layout(
    request: Request,
    current_user: dict = Depends(login_required),
):
    redis = request.app.state.redis_client
    session_id = request.cookies.get("session_id")
    payload = await request.json()
    messages = []

    # -----------------------------
    # Session validation
    # -----------------------------
    if not session_id or not await redis.exists(f"session:{session_id}"):
        return RedirectResponse("/logout?reason=expired", status_code=302)

    lang = current_user.get("language", "hu")
    user_org = current_user["org_id"]
    email = current_user.get("email")
    email_prefix = email.split("@")[0] if email else None

    try:
        async with async_session_scope(org_id=user_org) as session:

            # -----------------------------
            # Load client
            # -----------------------------
            client = await session.scalar(
                select(Client).where(Client.id == user_org)
            )

            if not client:
                return JSONResponse({
                    "success": False,
                    "messages": [msg(
                        text_en="Client not found",
                        text_hu="Az ügyfél nem található",
                        category="danger",
                        lang=lang
                    )]
                }, status_code=404)

            # =========================================================
            # STEP 1️⃣ SAVE CURRENT CLIENT STATE → HISTORY (BACKUP)
            # =========================================================
            previous_state = {
                "primary_color": client.primary_color,
                "border_radius": client.border_radius,
                "border_width": client.border_width,
                "border_color": client.border_color,
                "reply_bg_color": client.reply_bg_color,
                "operator_icon": client.operator_icon,
                "font_color": client.font_color,
                "everything_which_is_white": client.everything_which_is_white,
                "user_input_message_color": client.user_input_message_color,
                "popup_bg_color": client.popup_bg_color,
                "footer_bg_color": client.footer_bg_color,
                "footer_controls_bg": client.footer_controls_bg,
                "footer_input_bg_color": client.footer_input_bg_color,
                "footer_focus_outline_color": client.footer_focus_outline_color,
                "confirmation_button_bgcolor":client.confirmation_button_bgcolor,
                "scrollbar_color": client.scrollbar_color,
            }

            history = await session.scalar(
                select(ClientConfigHistory)
                .where(ClientConfigHistory.client_id == client.id)
            )

            if history:
                history.parameters = previous_state
            else:
                session.add(ClientConfigHistory(
                    client_id=client.id,
                    parameters=previous_state
                ))

            # =========================================================
            # STEP 2️⃣ APPLY NEW VALUES → CLIENTS TABLE
            # =========================================================
            new_values = {k: v for k, v in payload.items() if v is not None}

            for key, value in new_values.items():
                if hasattr(client, key):
                    setattr(client, key, value)

            # Audit info
            client.config_updated_by = email_prefix
            client.config_updated_at = datetime.utcnow()

            session.add(client)

        # -----------------------------
        # Success message
        # -----------------------------
        messages.append(msg(
            text_en="Configuration saved successfully",
            text_hu="Beállítások sikeresen elmentve",
            category="success",
            lang=lang
        ))

        return JSONResponse({"success": True, "messages": messages})

    except Exception as e:
        print("SAVE CONFIG ERROR:", e)

        messages.append(msg(
            text_en="Failed to save configuration",
            text_hu="A beállítások mentése sikertelen",
            category="danger",
            lang=lang
        ))

        return JSONResponse({"success": False, "messages": messages}, status_code=500)


@router.post("/api/client/config/restore_previous_layout")
async def restore_previous_layout(
    request: Request,
    current_user: dict = Depends(login_required),
):
    redis = request.app.state.redis_client
    session_id = request.cookies.get("session_id")
    messages = []

    # -----------------------------
    # Session validation
    # -----------------------------
    if not session_id or not await redis.exists(f"session:{session_id}"):
        return RedirectResponse("/logout?reason=expired", status_code=302)

    lang = current_user.get("language", "hu")
    user_org = current_user["org_id"]
    email = current_user.get("email")
    email_prefix = email.split("@")[0] if email else None

    try:
        async with async_session_scope(org_id=user_org) as session:

            # -----------------------------
            # Load client
            # -----------------------------
            client = await session.scalar(
                select(Client).where(Client.id == user_org)
            )

            if not client:
                return JSONResponse({
                    "success": False,
                    "messages": [msg(
                        text_en="Client not found",
                        text_hu="Az ügyfél nem található",
                        category="danger",
                        lang=lang
                    )]
                }, status_code=404)

            # -----------------------------
            # Load history (previous state)
            # -----------------------------
            history = await session.scalar(
                select(ClientConfigHistory)
                .where(ClientConfigHistory.client_id == client.id)
            )

            if not history or not history.parameters:
                return JSONResponse({
                    "success": False,
                    "messages": [msg(
                        text_en="No previous configuration found",
                        text_hu="Nem található korábbi konfiguráció",
                        category="warning",
                        lang=lang
                    )]
                })

            # =========================================================
            # RESTORE: overwrite client with history values
            # =========================================================
            for key, value in history.parameters.items():
                if hasattr(client, key):
                    setattr(client, key, value)

            # Audit info
            client.config_updated_by = email_prefix
            client.config_updated_at = datetime.utcnow()

            session.add(client)

        messages.append(msg(
            text_en="Previous configuration restored successfully",
            text_hu="Az előző konfiguráció sikeresen visszaállítva",
            category="success",
            lang=lang
        ))

        return JSONResponse({
            "success": True,
            "messages": messages
        })

    except Exception as e:
        print("RESTORE CONFIG ERROR:", e)

        messages.append(msg(
            text_en="Failed to restore configuration",
            text_hu="A konfiguráció visszaállítása sikertelen",
            category="danger",
            lang=lang
        ))

        return JSONResponse({
            "success": False,
            "messages": messages
        }, status_code=500)
    



class TextFontPayload(BaseModel):
    # fonts
    font_header_text: Optional[str]
    header_font_weight: Optional[str]
    header_font_size: Optional[str]
    font_general: Optional[str]
    general_body_font_size: Optional[str]
    general_body_font_size2: Optional[str]

    # texts
    language_hu_logo_text: Optional[str]
    language_en_logo_text: Optional[str]
    language_selector: Optional[str]
    greeting_message_hu: Optional[str]
    greeting_message_en: Optional[str]
    agent_request_confirmation_hu: Optional[str]
    agent_request_confirmation_en: Optional[str]

    # toggles
    agent_icon: Optional[bool]
    emoji_icon: Optional[bool]
    attachment_icon: Optional[bool]

    # languages
    languages: Optional[List[str]]

    class Config:
        extra = "allow"



@router.post("/api/client/config/manage_layout_textfont")
async def manage_previous_layout(
    request: Request,
    payload: TextFontPayload,   # Validate the incoming data
    current_user: dict = Depends(login_required),
):
    print("---- ---- --- ")
    redis = request.app.state.redis_client
    session_id = request.cookies.get("session_id")
    messages = []

    print("PAYLOAD RECEIVED:", payload.dict())
   

    # -----------------------------
    # Session validation
    # -----------------------------
    if not session_id or not await redis.exists(f"session:{session_id}"):
        return RedirectResponse("/logout?reason=expired", status_code=302)

    lang = current_user.get("language", "hu")
    user_org = current_user["org_id"]
    email = current_user.get("email")
    email_prefix = email.split("@")[0] if email else None

    try:
        async with async_session_scope(org_id=user_org) as session:

            # -----------------------------
            # Load client
            # -----------------------------
            client = await session.scalar(
                select(Client).where(Client.id == user_org)
            )

            if not client:
                return JSONResponse({
                    "success": False,
                    "messages": [msg(
                        text_en="Client not found",
                        text_hu="Az ügyfél nem található",
                        category="danger",
                        lang=lang
                    )]
                }, status_code=404)

            # =========================================================
            # STEP 1️⃣ SAVE CURRENT CLIENT STATE → HISTORY (BACKUP)
            # =========================================================
            previous_state = {
                # fonts
                "font_header_text": client.font_header_text,
                "header_font_weight": client.header_font_weight,
                "header_font_size": client.header_font_size,
                "font_general": client.font_general,
                "general_body_font_size": client.general_body_font_size,
                "general_body_font_size2": client.general_body_font_size2,

                # texts
                "language_hu_logo_text": client.language_hu_logo_text,
                "language_en_logo_text": client.language_en_logo_text,
                "language_selector": client.language_selector,
                "greeting_message_hu": client.greeting_message_hu,
                "greeting_message_en": client.greeting_message_en,
                "agent_request_confirmation_hu": client.agent_request_confirmation_hu,
                "agent_request_confirmation_en": client.agent_request_confirmation_en,

                # behavior toggles
                "agent_icon": client.agent_icon,
                "emoji_icon": client.emoji_icon,
                "attachment_icon": client.attachment_icon,

                # languages
                "languages": client.languages,
            }

            history = await session.scalar(
                select(ClientBehaviorHistory)
                .where(ClientBehaviorHistory.client_id == client.id)
            )

            if history:
                history.parameters = previous_state
            else:
                session.add(ClientBehaviorHistory(
                    client_id=client.id,
                    parameters=previous_state
                ))

            # =========================================================
            # STEP 2️⃣ APPLY NEW VALUES → CLIENTS TABLE
            # =========================================================
            new_values = payload.dict(exclude_none=True)

            for key, value in new_values.items():
                if hasattr(client, key):
                    setattr(client, key, value)

            # Audit info
            client.textfont_updated_by = email_prefix
            client.textfont_updated_at = datetime.utcnow()

            session.add(client)

        # -----------------------------
        # Success message
        # -----------------------------
        messages.append(msg(
            text_en="Configuration saved successfully",
            text_hu="Beállítások sikeresen elmentve",
            category="success",
            lang=lang
        ))

        return JSONResponse({"success": True, "messages": messages})

    except Exception as e:
        print("SAVE CONFIG ERROR:", e)

        messages.append(msg(
            text_en="Failed to save configuration",
            text_hu="A beállítások mentése sikertelen",
            category="danger",
            lang=lang
        ))

        return JSONResponse({"success": False, "messages": messages}, status_code=500)


@router.post("/api/client/config/restore_previous_layout_textfont")
async def restore_previous_layout(
    request: Request,
    current_user: dict = Depends(login_required),
):
    redis = request.app.state.redis_client
    session_id = request.cookies.get("session_id")
    messages = []

    # -----------------------------
    # Session validation
    # -----------------------------
    if not session_id or not await redis.exists(f"session:{session_id}"):
        return RedirectResponse("/logout?reason=expired", status_code=302)

    lang = current_user.get("language", "hu")
    user_org = current_user["org_id"]
    email = current_user.get("email")
    email_prefix = email.split("@")[0] if email else None

    try:
        async with async_session_scope(org_id=user_org) as session:

            # -----------------------------
            # Load client
            # -----------------------------
            client = await session.scalar(
                select(Client).where(Client.id == user_org)
            )

            if not client:
                return JSONResponse({
                    "success": False,
                    "messages": [msg(
                        text_en="Client not found",
                        text_hu="Az ügyfél nem található",
                        category="danger",
                        lang=lang
                    )]
                }, status_code=404)

            # -----------------------------
            # Load history (previous state)
            # -----------------------------
            history = await session.scalar(
                select(ClientBehaviorHistory)
                .where(ClientBehaviorHistory.client_id == client.id)
            )

            if not history or not history.parameters:
                return JSONResponse({
                    "success": False,
                    "messages": [msg(
                        text_en="No previous configuration found",
                        text_hu="Nem található korábbi konfiguráció",
                        category="warning",
                        lang=lang
                    )]
                })

            # =========================================================
            # RESTORE: overwrite client with history values
            # =========================================================
            for key, value in history.parameters.items():
                if hasattr(client, key):
                    setattr(client, key, value)

            # Audit info
            client.textfont_updated_by = email_prefix
            client.textfont_updated_at = datetime.utcnow()

            session.add(client)

        messages.append(msg(
            text_en="Previous configuration restored successfully",
            text_hu="Az előző konfiguráció sikeresen visszaállítva",
            category="success",
            lang=lang
        ))

        return JSONResponse({
            "success": True,
            "messages": messages
        })

    except Exception as e:
        print("RESTORE CONFIG ERROR:", e)

        messages.append(msg(
            text_en="Failed to restore configuration",
            text_hu="A konfiguráció visszaállítása sikertelen",
            category="danger",
            lang=lang
        ))

        return JSONResponse({
            "success": False,
            "messages": messages
        }, status_code=500)



@router.get("/register/confirm")
async def register_confirm(request: Request, token: str):
    redis = request.app.state.redis_client  # aioredis client
    flash_id = str(uuid.uuid4())  # unique ID for this flash message
    flash_message = {"text": "", "category": ""}
    
    lang = "hu"
    
    try:
        data = s.loads(token, salt="email-confirm", max_age=60 * 60 * 48)
        email = data["email"]
        lang = data.get("lang", "hu") 
    except SignatureExpired:
        flash_message = {
            "text": "The registration link has expired. Please request a new one."
                    if lang == "en" else
                    "A regisztrációs link lejárt. Kérj újat.",
            "category": "danger"
        }
        await redis.set(f"flash:{flash_id}", json.dumps(flash_message), ex=60)  # expires in 60 seconds
        return RedirectResponse(url=f"/?flash_id={flash_id}")
    except BadSignature:
        flash_message = {
            "text": "The registration link is invalid." if lang == "en" else "A regisztrációs link érvénytelen.",
            "category": "danger"
        }
        await redis.set(f"flash:{flash_id}", json.dumps(flash_message), ex=60)
        return RedirectResponse(url=f"/?flash_id={flash_id}")

    # Activate user
    async with async_session_scope() as db_session:
        result = await db_session.execute(select(User).where(User.email == email))
        user = result.scalar_one_or_none()

        if not user:
            flash_message = {
                "text": "The user does not exist." if lang == "en" else "A felhasználó nem létezik.",
                "category": "danger"
            }
        elif user.is_active:
            flash_message = {
                "text": "Account is already activated. You can log in now."
                        if lang == "en" else
                        "A fiókot már korábban aktiváltad. Jelentkezz be.",
                "category": "info"
            }
        else:
            user.is_active = True
            db_session.add(user)
        
            flash_message = {
                "text": "Your account has been activated. You can now log in."
                        if lang == "en" else
                        "A fiókodat aktiváltuk. Most már be tudsz lépni.",
                "category": "success"
            }


    # Store flash message in Redis, flash_id is appended to the URL
    await redis.set(f"flash:{flash_id}", json.dumps(flash_message), ex=60)
    return RedirectResponse(url=f"/?flash_id={flash_id}")



#----------------------
#   PREDICTION MODULE
#----------------------

@router.get("/predictive_dashboard", response_class=HTMLResponse)
async def predictive_dashboard(request: Request, user: dict = Depends(role_required("Manager"))):
    # Determine message based on user's language
    message_text = (
        "Nem áll rendelkezésre elegendő adat ehhez a szolgáltatáshoz"
        if user.get("language") == "hu"
        else "No data available for this service"
    )

    # Render inline HTML via TemplateResponse
    return templates.TemplateResponse(
        "base.html",  # your main base template with header/navbar
        {
            "request": request,  # REQUIRED for url_for inside templates
            "user": user,
            "content_html": f"""
                <div class="message" style="
                    font-size: 24px;
                    text-align: center;
                    margin-top: 20%;
                ">
                    {message_text}
                </div>
            """
        }
    )


#----------------------
#   WEEKLY SHORT
#----------------------

@router.get("/unauthorized", response_class=PlainTextResponse)
async def unauthorized():
    return "You are not authorized to view this page", 403

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_vbanai(request: Request, csrf_protect: CsrfProtect=Depends(), user: dict = Depends(role_required("Manager", "Team Leader"))):
    
    session_id = request.cookies.get("session_id")
    redis = request.app.state.redis_client
    if not await redis.exists(f"session:{session_id}"):  #boolian false or true
            # Session expired → redirect to logout
        return RedirectResponse(
        url="/logout?reason=expired",
        status_code=302
    )

    
    cpu_pool = request.app.state.cpu_pool
    cpu_sem = request.app.state.cpu_sem

    First_character = user.get("first_character")
    user_role = user.get("role") or "User"
    language = user.get("language", "hu")

    #usernumber_previousweek, usernumber, average_userquerry, today, previous_monday=await asyncio.to_thread(user_querry_forquickreview, user["org_id"], "chat_messages")
    #top3locations = await asyncio.to_thread(locationranking, user["org_id"])
    
    
    # Run CPU-bound helpers concurrently
    user_results, top3locations, mainChartData0 = await asyncio.gather(
        user_querry_forquickreview(user["org_id"], redis, cpu_pool=cpu_pool, cpu_sem=cpu_sem),
        locationranking(user["org_id"], redis, cpu_pool=cpu_pool, cpu_sem=cpu_sem),
        datatransformation_for_chartjs_weekly(user["org_id"], cpu_pool=cpu_pool, cpu_sem=cpu_sem), return_exceptions=True  #Then handle exceptions individually. If one CPU task fails, asyncio.gather doesn't cancel all.
    )
    usernumber_previousweek, usernumber, average_userquerry, today, previous_monday = user_results


   
    if len(top3locations)==0:
        number1location=""
        number2location=""
        number3location=""
    if len(top3locations)==1:
        number1location=top3locations[0]
        number2location=""
        number3location=""
    if len(top3locations)==2:
        number1location=top3locations[0]
        number2location=top3locations[1]
        number3location=""
    if len(top3locations)==3:
        number1location=top3locations[0]
        number2location=top3locations[1]
        number3location=top3locations[2]

    #mainChartData0=datatransformation_for_chartjs(previous_monday.year, previous_monday.month, previous_monday.day, "00", "00", "00", today.year, today.month, today.day, today.hour, today.minute, today.second, "weekly", 'chat_messages')
    #mainChartData0 = await asyncio.to_thread(datatransformation_for_chartjs_weekly, user["org_id"], "weekly")
    

    def calculate_manufacturer(mainChartData0):
        manufacturer = {}
        for i in range(len(mainChartData0)):
            for j in range(len(mainChartData0[i]['x_secondary_b'])):
                key = mainChartData0[i]['x_secondary_b'][j]
                value = mainChartData0[i]['secondaryChartData_b'][0][j]
                manufacturer[key] = manufacturer.get(key, 0) + value
        return [{'x': k, 'y': v} for k, v in manufacturer.items()]
    manufacturer = await run_cpu_task(calculate_manufacturer, mainChartData0, cpu_pool=cpu_pool, cpu_sem=cpu_sem)
    print("gyártó")
    print(manufacturer)

    today_year = today.year
    today_month = f'{today.month:02}'
    today_day = f'{today.day:02}'
    today_hour = f'{today.hour:02}'
    today_minute = f'{today.minute:02}'
    today_second = f'{today.second:02}'

    previous_monday_year = previous_monday.year
    previous_monday_month = f'{previous_monday.month:02}'
    previous_monday_day = f'{previous_monday.day:02}'

    client_name = 'Your Company'
    subscription = None
    service_message = "Please contact Red Rain to select a service."
    chat_control_access = False
    metrics_access = False
    advanced_ai_access = False
    

    try:
        async with async_session_scope() as db_session:
          # Fetch the client and preload subscription data
          result = await db_session.execute(
              select(Client)
              .options(selectinload(Client.subscription))  # Preload subscription data
              .where(Client.id == user["org_id"])  # user from Depends
          )
          client = result.scalar_one_or_none()

          
          if client:
              client_name = client.client_name
              subscription = client.subscription  # This will contain the Subscription object now
              service_message = None  # No message needed since the client exists
          else:
              client_name = 'Your Company'
              subscription = None
              service_message = "Please contact Red Rain to select a service."
          
      
          # Check access permissions based on the subscription
          if subscription:
              chat_control_access = has_permission(user_role, subscription, "chat_control")
              metrics_access = has_permission(user_role, subscription, "chatbot_metrics")
              advanced_ai_access = has_permission(user_role, subscription, "advanced_ai")
          else:
              # Default to no access if no subscription is found
              chat_control_access = False
              metrics_access = False
              advanced_ai_access = False

    except Exception as e:
        print(f"Database error: {e}")  # Log the error for debugging

    
    csrf_token = csrf_protect.generate_csrf()


    return templates.TemplateResponse(
        "dashboard_vbanai.html",
        {
            "request": request,
            "First_character": First_character,
            "logged_in": True,
            "subscription": subscription,
            "service_message": service_message,
            "chat_control_access": chat_control_access,
            "metrics_access": metrics_access,
            "advanced_ai_access": advanced_ai_access,
            "user_role": user_role,
            "manufacturer": manufacturer,
            "mainChartData0": mainChartData0,
            "usernumber": usernumber,
            "average_userquerry": average_userquerry,
            "usernumber_previousweek": usernumber_previousweek,
            "number1location": number1location,
            "number2location": number2location,
            "number3location": number3location,
            "today_year": today_year,
            "today_month": today_month,
            "today_day": today_day,
            "today_hour": today_hour,
            "today_minute": today_minute,
            "today_second": today_second,
            "previous_monday_year": previous_monday_year,
            "previous_monday_month": previous_monday_month,
            "previous_monday_day": previous_monday_day,
            "previous_monday_hour": "00",
            "previous_monday_minute": "00",
            "previous_monday_second": "00",
            "language": language,
            "csrf_token": csrf_token
        }
    )


   
#----------------------
#   CHATS
#----------------------


@router.get("/chats_in_requested_period_weekly", response_class=HTMLResponse)
async def chats_in_requested_period_weekly(
    request: Request,
    user: dict = Depends(role_required("Manager", "Team Leader")),
    year: str = Query(...),
    month: str = Query(...),
    day: str = Query(...),
    hour: str = Query(...),
    minutes: str = Query(...),
    seconds: str = Query(...),
    year_end: str = Query(...),
    month_end: str = Query(...),
    day_end: str = Query(...),
    hour_end: str = Query(...),
    minutes_end: str = Query(...),
    seconds_end: str = Query(...),
):
    session_id = request.cookies.get("session_id")
    redis = request.app.state.redis_client
    cpu_pool=request.app.state.cpu_pool
    cpu_sem=request.app.state.cpu_sem
    if not await redis.exists(f"session:{session_id}"):  #boolian false or true
            # Session expired → redirect to logout
        return RedirectResponse(
        url="/logout?reason=expired",
        status_code=302
    )


    language = user.get("language", "hu")
    # Construct datetime range
    start_str = f"{year}-{month}-{day} {hour}:{minutes}:{seconds}"
    end_str = f"{year_end}-{month_end}-{day_end} {hour_end}:{minutes_end}:{seconds_end}"

    start_date = pytz.UTC.localize(datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S"))
    end_date = pytz.UTC.localize(datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S"))

    async with async_session_scope() as db_session:
        result = await db_session.execute(
            select(Client).where(Client.id == user["org_id"])
        )
        client = result.scalar_one_or_none()
        client_timezone = client.timezone if client and client.timezone else "UTC"

    # Run blocking function in thread
    rows, columns = await fetch_chat_messages_weekly(
        start_date,
        end_date,
        user["org_id"],
        client_timezone,
        redis,
        cpu_pool=cpu_pool, 
        cpu_sem=cpu_sem
    )

    tz = pytz.timezone(client_timezone)
    #start_date_local = start_date.astimezone(tz).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    end_date_local = end_date.astimezone(tz).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
   
    return templates.TemplateResponse(
        "chats_in_requested_period.html",
        {
            "request": request,
            "columns": columns,
            "rows": rows,
            "start_date": start_date,
            "end_date": end_date_local,
            "language": language,
        },
    )


@router.get("/chats_in_requested_period", response_class=HTMLResponse)
async def chats_in_requested_period(
    request: Request,
    frequency: str = Query(...),
    year: int = Query(...),
    month: int = Query(...),
    day: int = Query(...),
    hour: int = Query(...),
    minutes: int = Query(...),
    seconds: int = Query(...),
    year_end: int = Query(...),
    month_end: int = Query(...),
    day_end: int = Query(...),
    hour_end: int = Query(...),
    minutes_end: int = Query(...),
    seconds_end: int = Query(...),
    user: dict = Depends(role_required("Manager", "Team Leader")),
):
    
    session_id = request.cookies.get("session_id")
    redis = request.app.state.redis_client
    if not session_id or not await redis.exists(f"session:{session_id}"):  #int 1 or 0
        # Session expired → redirect to logout
        return RedirectResponse(
        url="/logout?reason=expired",
        status_code=302
    )

    cpu_pool = request.app.state.cpu_pool
    cpu_sem = request.app.state.cpu_sem

    if not redis or not cpu_pool or not cpu_sem:
        return HTMLResponse(
            "Server not ready (initialization in progress)",
            status_code=503,
        )
    language = user.get("language", "hu")
    # Build datetime strings
    # start_str = f"{year}-{month}-{day} {hour}:{minutes}:{seconds}"
    # end_str = f"{year_end}-{month_end}-{day_end} {hour_end}:{minutes_end}:{seconds_end}"

    start_str = f"{year}-{month:02d}-{day:02d} {hour:02d}:{minutes:02d}:{seconds:02d}"
    end_str = f"{year_end}-{month_end:02d}-{day_end:02d} {hour_end:02d}:{minutes_end:02d}:{seconds_end:02d}"

    # Parse into naive datetimes
    try:
        start_date = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        return HTMLResponse(
            content=f"<h3>Invalid date/time parameters: {e}</h3>",
            status_code=400
        )

    # Fetch client timezone asynchronously
    async with async_session_scope() as db_session:
        result = await db_session.execute(
            select(Client).where(Client.id == user["org_id"])
        )
        client = result.scalar_one_or_none()
        client_timezone = client.timezone if client and client.timezone else "UTC"

    # Localize dates and convert to UTC
    local_tz = pytz.timezone(client_timezone)
    start_localized = local_tz.localize(start_date)
    end_localized = local_tz.localize(end_date)
    start_utc = start_localized.astimezone(pytz.UTC)
    end_utc = end_localized.astimezone(pytz.UTC)

    # Run blocking fetch in a thread
    rows, columns = await fetch_chat_messages(start_utc, end_utc, user["org_id"], client_timezone, frequency, redis, cpu_pool=cpu_pool, cpu_sem=cpu_sem)
    
    
    # Cached topic counts
    topic_key = f"topic_counts:{user['org_id']}:{start_utc.isoformat()}:{end_utc.isoformat()}"

    cached = await redis.get(topic_key)
    if cached:
        topic_counts = json.loads(cached)
    else:
        topic_counts = await fetch_topic_classification_counts(start_utc, end_utc, user["org_id"])
        await redis.set(topic_key, json.dumps(topic_counts), ex=3600)


    return templates.TemplateResponse(
        "chats_in_requested_period.html",
        {
            "request": request,
            "columns": columns,
            "rows": rows,
            "start_date": start_date,
            "end_date": end_date,
            "language": language,
            "topic_counts": topic_counts,
            "topic":"Összes",
            "frequency": frequency
        },
    )


@router.get("/chats_in_requested_period/topic", response_class=HTMLResponse)
async def chats_in_requested_period_topic(
    request: Request,
    topic: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    language: str = Query(...),
    frequency: str = Query(...),
    user: dict = Depends(role_required("Manager", "Team Leader"))
):
    
    session_id = request.cookies.get("session_id")
    redis = request.app.state.redis_client
    if not await redis.exists(f"session:{session_id}"):  #int 1 or 0
        # Session expired → redirect to logout
        return RedirectResponse(
        url="/logout?reason=expired",
        status_code=302
    )


    # Fetch client timezone asynchronously
    async with async_session_scope() as db_session:
        result = await db_session.execute(
            select(Client).where(Client.id == user["org_id"])
        )
        client = result.scalar_one_or_none()
        client_timezone = client.timezone if client and client.timezone else "UTC"


    try:
        if not start_date or not end_date:
            raise ValueError("Missing start_date or end_date in query parameters.")
        
        start_date = datetime.fromisoformat(start_date)
        end_date = datetime.fromisoformat(end_date)

    except Exception as e:
        return HTMLResponse(
            content=f"<h3>Invalid or missing date parameters: {e}</h3>", status_code=400
        )
    
    # Localize dates and convert to UTC
    local_tz = pytz.timezone(client_timezone)
    start_localized = local_tz.localize(start_date)
    end_localized = local_tz.localize(end_date)
    start_utc = start_localized.astimezone(pytz.UTC)
    end_utc = end_localized.astimezone(pytz.UTC)

    # Run blocking fetch in a thread
    rows, columns = await fetch_chat_messages(start_utc, end_utc, user["org_id"], client_timezone, frequency, redis, topic)
    
    # Cached topic counts
    topic_key = f"topic_counts:{user['org_id']}:{start_utc.isoformat()}:{end_utc.isoformat()}"

    cached = await redis.get(topic_key)
    if cached:
        topic_counts = json.loads(cached)
    else:
        topic_counts = await fetch_topic_classification_counts(start_utc, end_utc, user["org_id"])
        await redis.set(topic_key, json.dumps(topic_counts), ex=3600)

    return templates.TemplateResponse(
        "chats_in_requested_period.html",
        {
            "request": request,
            "columns": columns,
            "rows": rows,
            "start_date": start_date,
            "end_date": end_date,
            "language": language,
            "topic_counts": topic_counts,
            "topic": topic
        },
    )






#------------------------
#   CUSTOM DATA ANALYSIS
#-------------------------
#----------------------
#   FIRST PAGE
#----------------------





#------------------------
#   CUSTOM DATA ANALYSIS
#-------------------------
#----------------------
#   FIRST PAGE
#----------------------



@router.post("/chats_deepinsight_landingpage", response_class=HTMLResponse)
async def chats_deepinsight_landingpage(
    request: Request,
    csrf_protect: CsrfProtect = Depends(),
    csrf_token: str = Form(...),
    user: dict = Depends(role_required("Manager", "Team Leader"))
):
    
    csrf_protect.validate_csrf(csrf_token, request)
    session_id = request.cookies.get("session_id")
    redis = request.app.state.redis_client
    if not await redis.exists(f"session:{session_id}"):  #boolian false or true
            # Session expired → redirect to logout
        return RedirectResponse(
        url="/logout?reason=expired",
        status_code=302
    )

    try:
        # Extract form data from request
        form = await request.form()
        First_character = user.get("first_character")

        year = parse_int(form, "year_start")
        month = parse_int(form, "month_start")
        day = parse_int(form, "day_start")
        hour = parse_int(form, "hour")
        minutes = parse_int(form, "minutes")
        seconds = parse_int(form, "seconds")

        year_end = parse_int(form, "year_end")
        month_end = parse_int(form, "month_end")
        day_end = parse_int(form, "day_end")
        hour_end = parse_int(form, "hour_end")
        minutes_end = parse_int(form, "minutes_end")
        seconds_end = parse_int(form, "seconds_end")

        frequency = form.get("frequency")
        client = None
        try:
            async with async_session_scope() as db_session:
                result = await db_session.execute(
                    select(Client)
                    .options(selectinload(Client.subscription))
                    .where(Client.id == user["org_id"])
                )
                client = result.scalar_one_or_none()

        except Exception as e:
            print(f"Database error: {e}")  # Log the error for debugging


        client_timezone_str = client.timezone if client and client.timezone else "UTC"
        client_tz = pytz.timezone(client_timezone_str)

        # Create naive local datetime objects
        start_naive = datetime(year, month, day, hour, minutes, seconds)
        end_naive = datetime(year_end, month_end, day_end, hour_end, minutes_end, seconds_end)

        # Localize to client's timezone
        start_local = client_tz.localize(start_naive)
        end_local = client_tz.localize(end_naive)

        # Convert to UTC for DB query
        start_utc = start_local.astimezone(pytz.UTC)
        end_utc = end_local.astimezone(pytz.UTC)

        # Call your transformation function with UTC-aware datetimes
        data = await datatransformation_for_chartjs(
            user["org_id"],
            start_utc.year, start_utc.month, start_utc.day, start_utc.hour, start_utc.minute, start_utc.second,
            end_utc.year, end_utc.month, end_utc.day, end_utc.hour, end_utc.minute, end_utc.second,
            frequency,
            "chat_messages", redis
        )
        
        topic = "topic_all"
        redis_key = (
            f"deepinsight:{user['org_id']}:" 
            f"{topic}:"
            f"{int(start_utc.timestamp())}:"
            f"{int(end_utc.timestamp())}"
        )

        await redis.set(redis_key, json.dumps(data), ex=3600)

        if not data:
            
            # Language-sensitive message
            if user.get("language") == "hu":
                message_text = "Nem áll rendelkezésre elegendő adat ehhez a szolgáltatáshoz"
            else:
                message_text = "No data available for this service"

            html_content = f"""
            <html>
                <head>
                    <style>
                        .message {{
                            font-size: 24px;
                            text-align: center;
                            margin-top: 20%;
                        }}
                    </style>
                </head>
                <body>
                    <div class="message">{message_text}</div>
                </body>
            </html>
            """
            return HTMLResponse(content=html_content) 
          
        client_name = 'Your Company'
        subscription = None
        service_message = "Please contact Red Rain to select a service."
        chat_control_access = False
        metrics_access = False
        advanced_ai_access = False
        user_role = user.get("role", "User")

        
            
        if client:
            client_name = client.client_name
            subscription = client.subscription  # This will contain the Subscription object now
            service_message = None  # No message needed since the client exists
        else:
            client_name = 'Your Company'
            subscription = None
            service_message = "Please contact Red Rain to select a service."
        
    
        # Check access permissions based on the subscription
        if subscription:
            chat_control_access = has_permission(user_role, subscription, "chat_control")
            metrics_access = has_permission(user_role, subscription, "chatbot_metrics")
            advanced_ai_access = has_permission(user_role, subscription, "advanced_ai")
        else:
            # Default to no access if no subscription is found
            chat_control_access = False
            metrics_access = False
            advanced_ai_access = False

        csrf_token = csrf_protect.generate_csrf()
        return templates.TemplateResponse(
            "chats_deepinsight_landingpage.html",
            {
                "csrf_token": csrf_token,
                "request": request,
                # "data": data,
                "year": year, "month": month, "day": day,
                "hour": hour, "minutes": minutes, "seconds": seconds,
                "year_end": year_end, "month_end": month_end, "day_end": day_end,
                "hour_end": hour_end, "minutes_end": minutes_end, "seconds_end": seconds_end,
                "frequency": frequency,
                "First_character": First_character,
                "subscription": subscription,
                "service_message": service_message,
                "chat_control_access": chat_control_access,
                "metrics_access": metrics_access,
                "advanced_ai_access": advanced_ai_access,
                "user_role": user_role,
                "language": user.get("language")
            }
        )
    except Exception as e:
        # Log the exception to the console
        print(f"An error occurred in the topicMonitoring route: {str(e)}")
        # Optionally, you can return an error response to the client
        return JSONResponse({'error': 'An internal server error occurred'}, status_code=500)








def parse_int(form, key, default=0):
    try:
        return int(form.get(key, default))
    except (TypeError, ValueError):
        return default




@router.post("/topicMonitoring", response_class=HTMLResponse)
async def topic_monitoring(
    request: Request,
    csrf_protect: CsrfProtect = Depends(),
    csrf_token: str = Form(...),
    user: dict = Depends(role_required("Manager", "Team Leader"))
):
    
    csrf_protect.validate_csrf(csrf_token, request)
    session_id = request.cookies.get("session_id")
    redis = request.app.state.redis_client
    if not await redis.exists(f"session:{session_id}"):  #boolian false or true
            # Session expired → redirect to logout
        return RedirectResponse(
        url="/logout?reason=expired",
        status_code=302
    )

    try:
        # Extract form data from request
        form = await request.form()
        First_character = user.get("first_character")

        year = parse_int(form, "year_start")
        month = parse_int(form, "month_start")
        day = parse_int(form, "day_start")
        hour = parse_int(form, "hour")
        minutes = parse_int(form, "minutes")
        seconds = parse_int(form, "seconds")

        year_end = parse_int(form, "year_end")
        month_end = parse_int(form, "month_end")
        day_end = parse_int(form, "day_end")
        hour_end = parse_int(form, "hour_end")
        minutes_end = parse_int(form, "minutes_end")
        seconds_end = parse_int(form, "seconds_end")

        frequency = form.get("frequency")
        client = None
        try:
            async with async_session_scope() as db_session:
                result = await db_session.execute(
                    select(Client)
                    .options(selectinload(Client.subscription))
                    .where(Client.id == user["org_id"])
                )
                client = result.scalar_one_or_none()

        except Exception as e:
            print(f"Database error: {e}")  # Log the error for debugging


        client_timezone_str = client.timezone if client and client.timezone else "UTC"
        client_tz = pytz.timezone(client_timezone_str)

        # Create naive local datetime objects
        start_naive = datetime(year, month, day, hour, minutes, seconds)
        end_naive = datetime(year_end, month_end, day_end, hour_end, minutes_end, seconds_end)

        # Localize to client's timezone
        start_local = client_tz.localize(start_naive)
        end_local = client_tz.localize(end_naive)

        # Convert to UTC for DB query
        start_utc = start_local.astimezone(pytz.UTC)
        end_utc = end_local.astimezone(pytz.UTC)

        # Call your transformation function with UTC-aware datetimes
        #  REDIS  !!!!
        
        start_ts = int(start_utc.timestamp())
        end_ts = int(end_utc.timestamp())
        topic = "topic_all"

        redis_key = f"deepinsight:{user['org_id']}:{topic}:{start_ts}:{end_ts}"

        cached = await redis.get(redis_key)
        if cached:
            data = json.loads(cached)
        else:
            # only compute if cache missing (should not happen)
            data = await datatransformation_for_chartjs(
                user["org_id"],
                start_utc.year, start_utc.month, start_utc.day, start_utc.hour, start_utc.minute, start_utc.second,
                end_utc.year, end_utc.month, end_utc.day, end_utc.hour, end_utc.minute, end_utc.second,
                frequency,
                "chat_messages", redis
            )
            await redis.set(redis_key, json.dumps(data), ex=3600)

        if not data:
            
            # Language-sensitive message
            if user.get("language") == "hu":
                message_text = "Nem áll rendelkezésre elegendő adat ehhez a szolgáltatáshoz"
            else:
                message_text = "No data available for this service"

            html_content = f"""
            <html>
                <head>
                    <style>
                        .message {{
                            font-size: 24px;
                            text-align: center;
                            margin-top: 20%;
                        }}
                    </style>
                </head>
                <body>
                    <div class="message">{message_text}</div>
                </body>
            </html>
            """
            return HTMLResponse(content=html_content) 
          
        client_name = 'Your Company'
        subscription = None
        service_message = "Please contact Red Rain to select a service."
        chat_control_access = False
        metrics_access = False
        advanced_ai_access = False
        user_role = user.get("role", "User")

        
            
        if client:
            client_name = client.client_name
            subscription = client.subscription  # This will contain the Subscription object now
            service_message = None  # No message needed since the client exists
        else:
            client_name = 'Your Company'
            subscription = None
            service_message = "Please contact Red Rain to select a service."
        
    
        # Check access permissions based on the subscription
        if subscription:
            chat_control_access = has_permission(user_role, subscription, "chat_control")
            metrics_access = has_permission(user_role, subscription, "chatbot_metrics")
            advanced_ai_access = has_permission(user_role, subscription, "advanced_ai")
        else:
            # Default to no access if no subscription is found
            chat_control_access = False
            metrics_access = False
            advanced_ai_access = False

        
        # Cached topic counts
        topic_key = f"topic_counts:{user['org_id']}:{start_utc.isoformat()}:{end_utc.isoformat()}"

        cached = await redis.get(topic_key)
        if cached:
            topic_counts = json.loads(cached)
        else:
            topic_counts = await fetch_topic_classification_counts(start_utc, end_utc, user["org_id"])
            await redis.set(topic_key, json.dumps(topic_counts), ex=3600)

            
        return templates.TemplateResponse(
            "charts.html",
            {
                "request": request,
                "data": data,
                "year": year, "month": month, "day": day,
                "hour": hour, "minutes": minutes, "seconds": seconds,
                "year_end": year_end, "month_end": month_end, "day_end": day_end,
                "hour_end": hour_end, "minutes_end": minutes_end, "seconds_end": seconds_end,
                "frequency": frequency,
                "First_character": First_character,
                "subscription": subscription,
                "service_message": service_message,
                "chat_control_access": chat_control_access,
                "metrics_access": metrics_access,
                "advanced_ai_access": advanced_ai_access,
                "user_role": user_role,
                "topic_counts": topic_counts,
                "topic":"Összes",
                "language": user.get("language")
            }
        )
    except Exception as e:
        # Log the exception to the console
        print(f"An error occurred in the topicMonitoring route: {str(e)}")
        # Optionally, you can return an error response to the client
        return JSONResponse({'error': 'An internal server error occurred'}, status_code=500)



# @router.post("/topicMonitoring", response_class=HTMLResponse)
# async def topic_monitoring(
#     request: Request,
#     csrf_protect: CsrfProtect = Depends(),
#     csrf_token: str = Form(...),
#     user: dict = Depends(role_required("Manager", "Team Leader"))
# ):
    
#     csrf_protect.validate_csrf(csrf_token, request)
#     session_id = request.cookies.get("session_id")
#     redis = request.app.state.redis_client
#     if not await redis.exists(f"session:{session_id}"):  #boolian false or true
#             # Session expired → redirect to logout
#             return RedirectResponse(url="/logout", status_code=302)

#     try:
#         # Extract form data from request
#         form = await request.form()
#         First_character = user.get("first_character")

#         year = parse_int(form, "year_start")
#         month = parse_int(form, "month_start")
#         day = parse_int(form, "day_start")
#         hour = parse_int(form, "hour")
#         minutes = parse_int(form, "minutes")
#         seconds = parse_int(form, "seconds")

#         year_end = parse_int(form, "year_end")
#         month_end = parse_int(form, "month_end")
#         day_end = parse_int(form, "day_end")
#         hour_end = parse_int(form, "hour_end")
#         minutes_end = parse_int(form, "minutes_end")
#         seconds_end = parse_int(form, "seconds_end")

#         frequency = form.get("frequency")
#         client = None
#         try:
#             async with async_session_scope() as db_session:
#                 result = await db_session.execute(
#                     select(Client)
#                     .options(selectinload(Client.subscription))
#                     .where(Client.id == user["org_id"])
#                 )
#                 client = result.scalar_one_or_none()

#         except Exception as e:
#             print(f"Database error: {e}")  # Log the error for debugging


#         client_timezone_str = client.timezone if client and client.timezone else "UTC"
#         client_tz = pytz.timezone(client_timezone_str)

#         # Create naive local datetime objects
#         start_naive = datetime(year, month, day, hour, minutes, seconds)
#         end_naive = datetime(year_end, month_end, day_end, hour_end, minutes_end, seconds_end)

#         # Localize to client's timezone
#         start_local = client_tz.localize(start_naive)
#         end_local = client_tz.localize(end_naive)

#         # Convert to UTC for DB query
#         start_utc = start_local.astimezone(pytz.UTC)
#         end_utc = end_local.astimezone(pytz.UTC)

#         # Call your transformation function with UTC-aware datetimes
#         data = await datatransformation_for_chartjs(
#             user["org_id"],
#             start_utc.year, start_utc.month, start_utc.day, start_utc.hour, start_utc.minute, start_utc.second,
#             end_utc.year, end_utc.month, end_utc.day, end_utc.hour, end_utc.minute, end_utc.second,
#             frequency,
#             "chat_messages", redis
#         )

#         if not data:
            
#             # Language-sensitive message
#             if user.get("language") == "hu":
#                 message_text = "Nem áll rendelkezésre elegendő adat ehhez a szolgáltatáshoz"
#             else:
#                 message_text = "No data available for this service"

#             html_content = f"""
#             <html>
#                 <head>
#                     <style>
#                         .message {{
#                             font-size: 24px;
#                             text-align: center;
#                             margin-top: 20%;
#                         }}
#                     </style>
#                 </head>
#                 <body>
#                     <div class="message">{message_text}</div>
#                 </body>
#             </html>
#             """
#             return HTMLResponse(content=html_content) 
          
#         client_name = 'Your Company'
#         subscription = None
#         service_message = "Please contact Red Rain to select a service."
#         chat_control_access = False
#         metrics_access = False
#         advanced_ai_access = False
#         user_role = user.get("role", "User")

        
            
#         if client:
#             client_name = client.client_name
#             subscription = client.subscription  # This will contain the Subscription object now
#             service_message = None  # No message needed since the client exists
#         else:
#             client_name = 'Your Company'
#             subscription = None
#             service_message = "Please contact Red Rain to select a service."
        
    
#         # Check access permissions based on the subscription
#         if subscription:
#             chat_control_access = has_permission(user_role, subscription, "chat_control")
#             metrics_access = has_permission(user_role, subscription, "chatbot_metrics")
#             advanced_ai_access = has_permission(user_role, subscription, "advanced_ai")
#         else:
#             # Default to no access if no subscription is found
#             chat_control_access = False
#             metrics_access = False
#             advanced_ai_access = False

        
#         return templates.TemplateResponse(
#             "charts.html",
#             {
#                 "request": request,
#                 "data": data,
#                 "year": year, "month": month, "day": day,
#                 "hour": hour, "minutes": minutes, "seconds": seconds,
#                 "year_end": year_end, "month_end": month_end, "day_end": day_end,
#                 "hour_end": hour_end, "minutes_end": minutes_end, "seconds_end": seconds_end,
#                 "frequency": frequency,
#                 "First_character": First_character,
#                 "subscription": subscription,
#                 "service_message": service_message,
#                 "chat_control_access": chat_control_access,
#                 "metrics_access": metrics_access,
#                 "advanced_ai_access": advanced_ai_access,
#                 "user_role": user_role,
#                 "language": user.get("language")
#             }
#         )
#     except Exception as e:
#         # Log the exception to the console
#         print(f"An error occurred in the topicMonitoring route: {str(e)}")
#         # Optionally, you can return an error response to the client
#         return JSONResponse({'error': 'An internal server error occurred'}, status_code=500)


@router.get("/productbreakdown_topics", response_class=HTMLResponse)
async def topic_monitoring(
    request: Request,
    user: dict = Depends(role_required("Manager", "Team Leader"))
):
    
    session_id = request.cookies.get("session_id")
    redis = request.app.state.redis_client
    if not await redis.exists(f"session:{session_id}"):  #boolian false or true
            # Session expired → redirect to logout
        return RedirectResponse(
        url="/logout?reason=expired",
        status_code=302
    )

    try:
        # Extract form data from request
        query_params = request.query_params
        year = int(query_params.get("year"))
        month = int(query_params.get("month"))
        day = int(query_params.get("day"))
        hour = int(query_params.get("hour"))
        minutes = int(query_params.get("minutes"))
        seconds = int(query_params.get("seconds"))

        year_end = int(query_params.get("year_end"))
        month_end = int(query_params.get("month_end"))
        day_end = int(query_params.get("day_end"))
        hour_end = int(query_params.get("hour_end"))
        minutes_end = int(query_params.get("minutes_end"))
        seconds_end = int(query_params.get("seconds_end"))

        frequency = query_params.get("frequency")
        topic = query_params.get("topic")
   


        client = None
        try:
            async with async_session_scope() as db_session:
                result = await db_session.execute(
                    select(Client)
                    .options(selectinload(Client.subscription))
                    .where(Client.id == user["org_id"])
                )
                client = result.scalar_one_or_none()

        except Exception as e:
            print(f"Database error: {e}")  # Log the error for debugging


        client_timezone_str = client.timezone if client and client.timezone else "UTC"
        client_tz = pytz.timezone(client_timezone_str)

        # Create naive local datetime objects
        start_naive = datetime(year, month, day, hour, minutes, seconds)
        end_naive = datetime(year_end, month_end, day_end, hour_end, minutes_end, seconds_end)

        start_str = f"{year}-{month:02d}-{day:02d} {hour:02d}:{minutes:02d}:{seconds:02d}"
        end_str = f"{year_end}-{month_end:02d}-{day_end:02d} {hour_end:02d}:{minutes_end:02d}:{seconds_end:02d}"

        # Parse into naive datetimes
        try:
            start_date = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
            end_date = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            return HTMLResponse(
                content=f"<h3>Invalid date/time parameters: {e}</h3>",
                status_code=400
            )

        # Localize to client's timezone
        start_local = client_tz.localize(start_naive)
        end_local = client_tz.localize(end_naive)

        # Convert to UTC for DB query
        start_utc = start_local.astimezone(pytz.UTC)
        end_utc = end_local.astimezone(pytz.UTC)

        # Call your transformation function with UTC-aware datetimes
        data = await datatransformation_for_chartjs(
            user["org_id"],
            start_utc.year, start_utc.month, start_utc.day, start_utc.hour, start_utc.minute, start_utc.second,
            end_utc.year, end_utc.month, end_utc.day, end_utc.hour, end_utc.minute, end_utc.second,
            frequency,
            "chat_messages", redis, topic
        )

        if not data:
            
            # Language-sensitive message
            if user.get("language") == "hu":
                message_text = "Nem áll rendelkezésre elegendő adat ehhez a szolgáltatáshoz"
            else:
                message_text = "No data available for this service"

            html_content = f"""
            <html>
                <head>
                    <style>
                        .message {{
                            font-size: 24px;
                            text-align: center;
                            margin-top: 20%;
                        }}
                    </style>
                </head>
                <body>
                    <div class="message">{message_text}</div>
                </body>
            </html>
            """
            return HTMLResponse(content=html_content) 
          
        client_name = 'Your Company'
        subscription = None
        service_message = "Please contact Red Rain to select a service."
        chat_control_access = False
        metrics_access = False
        advanced_ai_access = False
        user_role = user.get("role", "User")

        
            
        if client:
            client_name = client.client_name
            subscription = client.subscription  # This will contain the Subscription object now
            service_message = None  # No message needed since the client exists
        else:
            client_name = 'Your Company'
            subscription = None
            service_message = "Please contact Red Rain to select a service."
        
    
        # Check access permissions based on the subscription
        if subscription:
            chat_control_access = has_permission(user_role, subscription, "chat_control")
            metrics_access = has_permission(user_role, subscription, "chatbot_metrics")
            advanced_ai_access = has_permission(user_role, subscription, "advanced_ai")
        else:
            # Default to no access if no subscription is found
            chat_control_access = False
            metrics_access = False
            advanced_ai_access = False

      
        return templates.TemplateResponse(
            "charts_for_topics.html",
            {
                "request": request,
                "data": data,
                "year": year, "month": month, "day": day,
                "hour": hour, "minutes": minutes, "seconds": seconds,
                "year_end": year_end, "month_end": month_end, "day_end": day_end,
                "hour_end": hour_end, "minutes_end": minutes_end, "seconds_end": seconds_end,
                "frequency": frequency,
                "subscription": subscription,
                "service_message": service_message,
                "chat_control_access": chat_control_access,
                "metrics_access": metrics_access,
                "advanced_ai_access": advanced_ai_access,
                "user_role": user_role,
                "language": user.get("language"),
                "topic":topic,
                "start_date": start_date,
                "end_date": end_date,
            }
        )
    except Exception as e:
        # Log the exception to the console
        print(f"An error occurred in the topicMonitoring route: {str(e)}")
        # Optionally, you can return an error response to the client
        return JSONResponse({'error': 'An internal server error occurred'}, status_code=500)






#------------------------
#   CUSTOM DATA ANALYSIS
#-------------------------
#----------------------
#   BREAKDOWN PAGE
#----------------------
utc = pytz.UTC
def convert_utc_str_to_local(utc_str, local_tz):
    # parse naive datetime string
    naive_dt = datetime.strptime(utc_str, '%Y-%m-%d %H:%M:%S')
    # localize it to UTC
    utc_dt = naive_dt.replace(tzinfo=utc)
    # convert to requested timezone
    local_dt = utc_dt.astimezone(local_tz)
    return local_dt.strftime('%Y-%m-%d %H:%M:%S')

@router.get("/detaileduserdata", response_class=HTMLResponse)
async def detailed_user_data(
    request: Request,
    user: dict = Depends(role_required("Manager", "Team Leader"))
):
    session_id = request.cookies.get("session_id")
    redis = request.app.state.redis_client # request.app is the reference to the FastAPI instance (fastapi_app) that is serving
    if not await redis.exists(f"session:{session_id}"):  #boolian false or true
            # Session expired → redirect to logout
        return RedirectResponse(
        url="/logout?reason=expired",
        status_code=302
    )


    try:
        # Parse query parameters safely
        query_params = request.query_params
        year = parse_int(query_params, "year")
        month = parse_int(query_params, "month")
        day = parse_int(query_params, "day")
        hour = parse_int(query_params, "hour")
        minutes = parse_int(query_params, "minutes")
        seconds = parse_int(query_params, "seconds")
        year_end = parse_int(query_params, "year_end")
        month_end = parse_int(query_params, "month_end")
        day_end = parse_int(query_params, "day_end")
        hour_end = parse_int(query_params, "hour_end")
        minutes_end = parse_int(query_params, "minutes_end")
        seconds_end = parse_int(query_params, "seconds_end")
        frequency = query_params.get("frequency", "daily")
        topic = query_params.get("topic", "Összes")

        # Fetch client asynchronously
        async with async_session_scope() as db_session:
            result = await db_session.execute(
                select(Client)
                .where(Client.id == user["org_id"])
            )
            client = result.scalar_one_or_none()

        client_timezone_str = client.timezone if client and client.timezone else "UTC"
        client_tz = pytz.timezone(client_timezone_str)

        # Create naive local datetime objects
        start_naive = datetime(int(year), int(month), int(day), int(hour), int(minutes), int(seconds))
        end_naive = datetime(int(year_end), int(month_end), int(day_end), int(hour_end), int(minutes_end), int(seconds_end))

        # Localize to client's timezone
        start_local = client_tz.localize(start_naive)
        end_local = client_tz.localize(end_naive)

        # Convert to UTC for DB query
        start_utc = start_local.astimezone(pytz.UTC)
        end_utc = end_local.astimezone(pytz.UTC)
         # Run transformation in thread (blocking)

        cpu_pool = request.app.state.cpu_pool
        cpu_sem = request.app.state.cpu_sem
        final_transformed_data, data_for_final_transformation_copy, timestamp, start_end_date_byfrequency, usernumber, querry_on_average, changesinusernumber, locations = await datatransformation_for_chartjs_detailed(
            user["org_id"],
            start_utc.year, start_utc.month, start_utc.day, start_utc.hour, start_utc.minute, start_utc.second,
            end_utc.year, end_utc.month, end_utc.day, end_utc.hour, end_utc.minute, end_utc.second,
            frequency,
            'chat_messages', redis, topic, cpu_pool=cpu_pool,
            cpu_sem=cpu_sem)
        

        if final_transformed_data==[] and data_for_final_transformation_copy==[] and timestamp==[] and start_end_date_byfrequency==[] and usernumber==[] and querry_on_average==[] and changesinusernumber==[] and locations==[]:
            if user.get("language") == "hu":
                message_text = "Nem áll rendelkezésre elegendő adat ehhez a szolgáltatáshoz"
            else:
                message_text = "No data available for this service"

            html_content = f"""
            <html>
                <head>
                    <style>
                        .message {{
                            font-size: 24px;
                            text-align: center;
                            margin-top: 20%;
                        }}
                    </style>
                </head>
                <body>
                    <div class="message">{message_text}</div>
                </body>
            </html>
            """
            return HTMLResponse(content=html_content)

        
        frequency=frequency.capitalize()
      
        # Process manufacturer_final asynchronously
        def process_manufacturer(data_copy):
            result_list = []
            for p in range(len(data_copy)):
                manufacturer = {}
                for i in range(len(data_copy[p])):
                    for x in range(len(data_copy[p][i]['x_secondary_b'])):
                        key = data_copy[p][i]['x_secondary_b'][x]
                        manufacturer[key] = manufacturer.get(key, 0) + data_copy[p][i]['secondaryChartData_b'][0][x]
                result_list.append([{'x': k, 'y': v} for k, v in manufacturer.items()])
            return result_list

        manufacturer_final = await run_cpu_task(process_manufacturer, data_for_final_transformation_copy, cpu_pool=cpu_pool, cpu_sem=cpu_sem)

      
        # Convert start/end dates asynchronously
        async def convert_dates(dates, tz):
            converted = []
            for start_str, end_str in dates:
                start_local = convert_utc_str_to_local(start_str, tz)
                end_local = convert_utc_str_to_local(end_str, tz)
                converted.append([start_local, end_local])
            return converted

        converted_start_end_date = await run_cpu_task(convert_dates, start_end_date_byfrequency, client_tz, cpu_pool=cpu_pool, cpu_sem=cpu_sem)

        return templates.TemplateResponse(
            "detaileduserdata.html",
            {
                "request": request,
                "manufacturer_final": manufacturer_final,
                "mainChartData0": data_for_final_transformation_copy,
                "frequency": frequency,
                "start_end_date_byfrequency": converted_start_end_date,
                "changesinusernumber": changesinusernumber,
                "usernumber": usernumber,
                "querry_on_average": querry_on_average,
                "locations": locations,
                "language": user.get("language")
            }
        )

    except Exception as e:
        print(f"Error in detailed_user_data: {e}")
        return HTMLResponse("Internal server error", status_code=500)



# @app.errorhandler(400)  # Handles Bad Request errors
# def handle_bad_request(error):
#     # Optionally, you can check if the error is specifically a CSRF error
#     # CSRF errors usually return 400 Bad Request
#     flash('Your session has expired or is invalid. Please log in.', 'error')
#     return redirect(url_for('index'))













                                    #--------------------------
                                    #----    ENTRA B2C     ----
                                    #--------------------------






#associate a user with a company based on their email address.
async def find_client_by_email(email: str) -> int | None:
    """
    Associate a user with a company based on their email domain.
    """
    domain = email.split('@')[-1].split('.')[0].replace(' ', '').lower().strip()
    print("DOMAIN", domain)

    async with async_session_scope() as session:
        stmt = select(Client).where(
            func.replace(func.lower(Client.client_name), ' ', '') == domain
        )
        result = await session.execute(stmt)
        client = result.scalar_one_or_none()  # returns Client instance or None
        if client:
            return client.id
    return None

# ---------------------  EXCHANGE FOR TOKEN -------------



async def exchange_code_for_token(auth_code: str) -> dict:
    # Fetch configuration values from environment variables
    tenant = os.environ.get("B2C_TENANT")
    client_id = os.environ.get("B2C_CLIENT_ID")
    client_secret = os.environ.get("B2C_SECRET_VALUE")
    redirect_uri = "https://redrain1230.loophole.site/auth"
    policy = os.environ.get("B2C_POLICY")

    # Ensure all necessary values are present
    if not all([tenant, client_id, client_secret, redirect_uri, policy]):
        raise HTTPException(status_code=500, detail="Missing required environment variables")

    # Token endpoint URL for B2C
    token_url = f"https://{tenant}.b2clogin.com/{tenant}.onmicrosoft.com/{policy}/oauth2/v2.0/token"

    # Prepare the token request payload
    payload = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "code": auth_code,
        "redirect_uri": redirect_uri,
        "scope": "openid profile email",
    }

    # Send the request (non-blocking)
    async with httpx.AsyncClient() as client:
        response = await client.post(token_url, data=payload)

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.json())

    return response.json()  # token response (dict)


async def get_b2c_jwks():
    # Fetch configuration values from environment
    tenant = os.environ.get("B2C_TENANT", "your_tenant_name")
    policy = os.environ.get("B2C_POLICY", "your_policy_name")
    
    # OpenID configuration URL
    openid_config_url = f"https://{tenant}.b2clogin.com/{tenant}.onmicrosoft.com/{policy}/v2.0/.well-known/openid-configuration"
    async with httpx.AsyncClient() as client:
        resp = await client.get(openid_config_url)
        resp.raise_for_status()
        jwks_uri = resp.json()["jwks_uri"]
        resp = await client.get(jwks_uri)
        resp.raise_for_status()
        return resp.json()["keys"]

def get_signing_key(kid: str, jwks: list):
    for key in jwks:
        if key["kid"] == kid:
            return RSAAlgorithm.from_jwk(json.dumps(key))
    raise ValueError("Signing key not found")

#------------------------------------

# REQUEST and RESPONSE in FastAPI  in my LOGIN environment:

# REQUEST coming form browser, It contains headers, cookies, query parameters, body data, and also a reference to the app.
# print(request.url)  # full URL the client requested
# print(request.cookies)  # dictionary of cookies sent by the browser
# print(request.headers)  # all HTTP headers
# first browser send: GET /login/external, Cookie: (none — first visit)

# as I add: fastapi_app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)
# SessionMiddleware intercepts every request before your route handler.
# It checks if the browser sent a cookie called "session".
# If yes, it loads it (JSON decode) → request.session becomes that dict.
# If no (new user) → request.session is an empty dict {}.
# This creates a dictionary for each user: request.session.

# At this point: request.session  # => {}
# Middleware is responsible only for "session" cookie.  no login session_id

# on the backend login/external:
# New user → request.session empty → create random session_id.
# Store it in request.session['session_id'] → now middleware will serialize this back into "session" cookie in the response.
# in login route we add an outh state to  request.session['oauth_state'] = state
# this is crf token when getting back from azure
# so the COOKIE created by MIDDLEWARE IS NEEDED ONLY FOR OAUTH flow

# in Auth receive we receive GET /auth?state=abc123&code=xyz987 Cookie: session={session_id: "<random>", oauth_state: "<state>"}
# compare state and we loggedin then we create REAL session cookie for LOGGED IN USERS



# At this point, the response that goes back to browser with created response automaticall will contain
# Set-Cookie: session={session_id: "<random>", oauth_state: "..."}; Path=/; HttpOnly
#the middleware automatically writes a Set-Cookie header for "session"
#So, yes, the middleware will serialize your request.session into a cookie in the HTTP response automatically, even if your response is a redirect.
# Browser receives this along with the 302 redirect, and stores the "session" cookie.
# Browser then follows the redirect to Azure.

# RESPONSE
# Represents what FastAPI will send back to the client (browser).
# You can manually modify headers or cookies before sending
# response = JSONResponse({"ok": True})
# response.set_cookie("mycookie", "value")



# When the browser returns, SessionMiddleware reads the session cookie and fills request.session.
# request.session['foo'] = 'bar'
# This is separate from session_id.
# It is just a place to store server-side state between requests.
# here I am doing:
# session_id = request.session.get("session_id")
# if not session_id:
#     session_id = secrets.token_urlsafe(16)
#     request.session["session_id"] = session_id




@router.api_route("/login/external", methods=["GET", "POST"])
async def login_external(request: Request):
    
    # Fetch configuration values from environment variables
    tenant = os.environ.get("B2C_TENANT", "your_tenant_name")  # E.g., "redrainaib2ctenant"
    client_id = os.environ.get("B2C_CLIENT_ID", "your_client_id")
    policy = os.environ.get("B2C_POLICY", "your_policy_name")
    redirect_uri="https://redrain1230.loophole.site/auth"
    
    
   
    # Generate a unique state value and store it in the session
    state = secrets.token_urlsafe(16)  # Generate a secure random state
    request.session['oauth_state'] = state  # Later, when the user comes back from Microsoft, you compare the state returned with this saved state. Match → safe, Mismatch → possible attack.
    

    ##################################################
    # 1.) session dictionary managed by SessionMiddleware)
    ##################################################
    # SessionMiddleware automatically serializes this dictionary into a cookie and sends it to the browser.
 


    session_id = request.session.get("session_id")
    if not session_id:
        session_id = secrets.token_urlsafe(16)
        request.session['session_id'] = session_id  # storing session_id in the session dictionary created/managed by SessionMiddleware
    redis = request.app.state.redis_client
    if redis:  # redisre is mentjük multiworker setup miatt, mert A user starts login on Worker 1, state is generated and stored in request.session. Microsoft redirects back after login — the request might go to Worker 2.

      ##############################################
       # 2.) Redis key/value for login state (setex)
       #############################################

        await redis.setex(f"{STATE_KEY_PREFIX}{session_id}", 300, state)
    print("elmentett!!!", await redis.get(f"{STATE_KEY_PREFIX}{session_id}"))
    # például:
    # await redis.setex(f"{STATE_KEY_PREFIX}{session_id}", 300, state)
    # Key: state:xG-g5zKwpwhq11b7Um05oQ
    # Value: { some state data }  # serialized as string, maybe JSON
    # TTL: 300 seconds


   


    # Construct the authorization URL
    base_url = f"https://{tenant}.b2clogin.com/{tenant}.onmicrosoft.com/oauth2/v2.0/authorize"
    params = {
        'p': policy,
        'client_id': client_id,
        'nonce': 'defaultNonce',  # You can generate a unique nonce if needed
        'redirect_uri': redirect_uri,
        'scope': 'openid',
        'response_type': 'code',
        'prompt': 'login',
        'state': state
    }
   
    
    # Encode the parameters
    query_string = urlencode(params)
    full_url = f"{base_url}?{query_string}"

    return RedirectResponse(full_url)




###########################################################
###########################################################
###########################################################
###########################################################



#redirection happens here
@router.get("/auth")
async def auth(
    request: Request,
    state: str = Query(...),  #means this query parameter is required.
    code: str = Query(None)   # means this query parameter is optional (default None).
):
    #if I have redirect url: https://your-app.com/auth?state=abc123&code=xyz987 state = "abc123" and code = "xyz987"  by this Query  , None means default value is None but if we have code it will be the value
    
    redis = request.app.state.redis_client
    session_id = request.session.get("session_id")

    # Load expected state from Redis Verify state (CSRF protection)
    stored_state = await load_oauth_state(redis, session_id)

    print("SESSION COOKIE:", request.session.get("session_id"))
    print("STATE FROM QUERY:", state)
    print("STATE FROM REDIS:", stored_state)

    if not stored_state or stored_state != state:
        return JSONResponse({"error": "Invalid state"}, status_code=400)

    if not code:
        return JSONResponse({"error": "Authorization code not found"}, status_code=400)
    
    await redis.delete(f"{STATE_KEY_PREFIX}{session_id}")

    # Exchange auth code for token
    try:
        token_response = await exchange_code_for_token(code)
    except HTTPException as e:
        return JSONResponse({"error": f"Token exchange failed: {e.detail}"}, status_code=e.status_code)

    # Extract and verfy the ID token and access token
    id_token = token_response.get('id_token')
    if not id_token:
        return JSONResponse({"error": "ID token not found"}, status_code=400)

    jwks = await get_b2c_jwks()
    unverified_header = jwt.get_unverified_header(id_token)
    key = get_signing_key(unverified_header["kid"], jwks)

    tenant = os.environ.get("B2C_TENANT", "your_tenant_name")
    policy = os.environ.get("B2C_POLICY", "your_policy_name")
    client_id = os.environ.get("B2C_CLIENT_ID", "your_client_id")
    
    unverified_claims = jwt.decode(
        id_token,
        options={"verify_signature": False}
    )

    token_issuer = unverified_claims["iss"]

    try:
        claims = jwt.decode(
            id_token,
            key=key,
            algorithms=["RS256"],
            audience=client_id,
            issuer=token_issuer,
            leeway=120 
           # issuer=f"https://{tenant}.b2clogin.com/{tenant}.onmicrosoft.com/{policy}/v2.0/"
        )
    except jwt.PyJWTError as e:
        return JSONResponse({"error": f"Invalid ID token: {str(e)}"}, status_code=400)
    

    email = claims.get('emails')[0] if 'emails' in claims and claims['emails'] else None
    if not email:
        return JSONResponse({"error": "Email not found in ID token"}, status_code=400)


  

    # --- Lookup client ---
    client_id = await find_client_by_email(email)  # should be async if using async DB
    print("CLIENT ID: ", client_id)
    if not client_id:
        # Store flash message in Redis with short TTL
        flash_id = str(uuid.uuid4())
        await redis.setex(f"flash:{flash_id}", 30, "Client not found for the given email domain.")
        return RedirectResponse(url=f"/?flash_id={flash_id}", status_code=302)


  

    # Find or create the user in the database
    
    async with async_session_scope() as db_session:
        result = await db_session.execute(
            select(User).options(joinedload(User.role)).where(User.email == email)
        )
        user = result.scalar_one_or_none()

        lang = user.language or "hu"

        if not user:
            flash_id = str(uuid.uuid4())
            await redis.setex(f"flash:{flash_id}", 30, "Your email is not registered. Please contact your company.")
            return RedirectResponse(url=f"/?flash_id={flash_id}", status_code=302)

        if user.is_deleted or not user.is_active:
            flash_id = str(uuid.uuid4())
            await redis.setex(f"flash:{flash_id}", 30, "Your account is not active or deleted.")
            return RedirectResponse(url=f"/?flash_id={flash_id}", status_code=302)

        # Save user info in Redis
        # Login successful → create "real" session cookie
        # we save save the user info in Redis




        ########################################
        # 3.) Redis hash for full session (hset)
        #######################################

        # hset a keyhez tartozó field - value (uganaz mint key:value) ket hash formátumban tárolja, nem stringben így egyesével is lehet a fieldeket kérdezni nem kell először dumpolni stb.
        # Command	Stored as	Retrieve
        # setex(key, ttl, value)	string (or JSON if you serialize)	redis.get(key) → whole value at once
        # hset(key, mapping={...})	hash / dict	redis.hgetall(key) → access individual fields easily



        now = int(time.time())
        await redis.hset(f"session:{session_id}", mapping={
            "user_id": str(user.id),
            "user_org": str(client_id),
            "language": user.language or "hu",
            "user_role": user.role.role_name if user.role else "Unknown",
            "first_character": email[0].upper(),
            "email": email,
            "name": user.name,
            "last_active": now
        })
        await redis.expire(f"session:{session_id}", SESSION_TTL)
        
        # This helps to quickly know which users are active in a given organization (org_id) without scanning all sessions.
        online_key = f"online:{client_id}:{user.id}"
        exists = await redis.exists(online_key)
        if not exists:
    # Set the key to some value (e.g., 1) and give it a TTL
            await redis.setex(online_key, SESSION_TTL, 1)
        else:
            # Optionally: refresh TTL if user is still active
            await redis.expire(online_key, SESSION_TTL)

        
                        
   
    try:
        
        # --- Notify others via async Socket.IO ---
        org_key = f"org:{client_id}:connections"
        sids_bytes = await redis.smembers(org_key)  # returns set of bytes
        sids = [s.decode() for s in sids_bytes]

        for sid in sids:
            try:
                await sio.emit(
                    'user_online_status_changed',
                    {'user_id': user.id, 'is_online': True},
                    to=sid
                )
            except Exception as emit_err:
                print(f"Emit error to SID {sid}: {emit_err}")

        print(f"[Redis] Marked user {user.id} of org {client_id} as online.")
    except Exception as e:
        print(f"[Redis Error] Failed to set online status: {e}")

    # --- Return redirect (simulate login) ---
    response = RedirectResponse(url="/serviceselector", status_code=302)
    
    secure = request.url.scheme == "https"

    ###########################################
    # 4.) Manual cookies (response.set_cookie)
    ##########################################

    # set a second cookie for browser to use for subsequent authenticated requests:
    # This cookie is independent of "session" created by middleware.
    # session_id cookie is the actual login session → read by get_current_user on future requests.
    # Ezt használjuk a bejelentezés után nem a middleware cookie-t
    response.set_cookie(   # without this modern browser will treat my cookie as unsecure
        "session_id",
        session_id,
        httponly=True,     #JavaScript cannot access this cookie. Only sent with HTTP requests.
        secure=secure,      # mind http and https esetén küldi a cookie-t pl session-t
        samesite="Lax",     # Good default for login flows
        max_age=SESSION_TTL_COOKIE
    )

    response.set_cookie(
        "lang",
        lang,
        httponly=False,  # cookie can be sent over HTTP or HTTPS.
        secure=secure,
        samesite="Lax",
        max_age=SESSION_TTL_COOKIE
    )

    # Cookies:
    # session_id = xG-g5zKwpwhq11b7Um05oQ
    # lang = hu
    # session_id = request.cookies.get("session_id")
    # lang = request.cookies.get("lang")
  
    # We sent both middleware cookies and manual cookies to the browser

    return response



@router.post("/heartbeat")
async def heartbeat(request: Request):
    # Cookie sent: session_id=xG-g5zKwpwhq11b7Um05oQ, session_id = "xG-g5zKwpwhq11b7Um05oQ" 
    session_id = request.cookies.get("session_id")



    
    redis = request.app.state.redis_client
    print(f"[Heartbeat] Received at {datetime.utcnow().isoformat()} for session: {session_id}")

    if not session_id:
        return JSONResponse({"error": "No session"}, status_code=401)

    session_key = f"session:{session_id}"
    # "session:xG-g5zKwpwhq11b7Um05oQ"
    exists = await redis.exists(session_key)
    if not exists:
        return JSONResponse({"error": "Session expired"}, status_code=401)

    now = int(time.time())
    await redis.hset(session_key, "last_active", now)
    # UpDATE will be:
    # Key: session:xG-g5zKwpwhq11b7Um05oQ
    # Hash Fields:
    # {
    #     "user_id": "123",
    #     "user_org": "7",
    #     "language": "hu",
    #     "user_role": "Admin",
    #     "first_character": "V",
    #     "email": "viktor@example.com",
    #     "name": "Viktor",
    #     "last_active": 1768935640   # <- updated by heartbeat
    # }

    return JSONResponse({"status": "ok"})







###########################################################
###########################################################
###########################################################
###########################################################

####################  LOGOUT  #############################
####################  LOGOUT  #############################
####################  LOGOUT  #############################

###########################################################
###########################################################
###########################################################
###########################################################








@router.api_route("/logout", methods=["GET", "POST"])
async def logout(request: Request, 
                 current_user: dict | None = Depends(get_current_user),
                 reason: str = Query("manual")):
    
    
    print("scheme:", request.url.scheme)
    print("index:", request.url_for("index"))


    redis = request.app.state.redis_client
    session_id = request.cookies.get("session_id")

    tenant = os.environ.get("B2C_TENANT")
    policy = os.environ.get("B2C_POLICY")




    redirect_uri = quote("https://redrain1230.loophole.site/", safe="")
    



    lang = request.cookies.get("lang", "hu")  # fallback to cookie if user not present
    if lang not in ["en", "hu"]:
        lang = "hu"

  

    # --- Load session details from Redis ---
    # user_id = await redis.get(f"session:{session_id}:user_id")
    # org_id = await redis.get(f"session:{session_id}:user_org")

    user_id = await redis.hget(f"session:{session_id}", "user_id")
    org_id = await redis.hget(f"session:{session_id}", "user_org")

    if user_id:
        user_id = user_id.decode() if isinstance(user_id, bytes) else str(user_id)
    if org_id:
        org_id = org_id.decode() if isinstance(org_id, bytes) else str(org_id)

    print(f"[Logout] Before clearing session: user_id={user_id}, org_id={org_id}")
    org_id_int = int(org_id) if org_id else None
    # --- Clear Redis session keys ---
    await redis.delete(f"session:{session_id}")

    if user_id and org_id:

        online_key = f"online:{org_id}:{user_id}"  # match your naming convention
        await redis.delete(online_key) 
        
        remaining_sids = []
        sids_bytes = await redis.smembers(f"org:{org_id}:connections")
        sids = [s.decode() for s in sids_bytes if s.decode() != session_id] 
        for sid_str in sids:
            conn_data = await redis.hgetall(f"connection:{sid_str}")
            if conn_data.get(b"user_id", b"").decode() == user_id:
                remaining_sids.append(sid_str)
        
        if not remaining_sids:
            await asyncio.gather(
                *(
                    sio.emit(
                        "user_online_status_changed",
                        {"user_id": user_id, "is_online": False},
                        to=s
                    )
                    for s in sids
                ),
                return_exceptions=True
            )
       

        # Only remove the current SID
        conn_key = f"connection:{session_id}"
        await redis.delete(conn_key)
        await redis.srem(f"org:{org_id}:connections", session_id)

        print(f"Admin {user_id} from org {org_id} disconnected from session {session_id}")

        org_connections_count = await redis.scard(f"org:{org_id}:connections")
        if org_connections_count == 0:
           
            # DB cleanup: OrgEventLog / Client.mode / last_manualmode_triggered_by
            try:
                async with async_session_scope() as db_session:
                    await db_session.execute(delete(OrgEventLog).where(OrgEventLog.org_id == org_id_int))
                    await db_session.execute(
                        update(Client).where(Client.id == org_id_int).values(mode="automatic")
                    )

                    client_to_update = await db_session.scalar(
                        select(Client).where(
                            Client.id == org_id_int,
                            Client.is_active == True,
                            Client.last_manualmode_triggered_by.isnot(None),
                        )
                    )

                    if client_to_update:
                        client_to_update.last_manualmode_triggered_by = None
                        print(f"Updated client {client_to_update.id}: set last_manualmode_triggered_by to None.")

                print(f"Cleared entries and reset mode to 'automatic' for org {org_id_int}")
            except Exception as e:
                print(f"[DB] Cleanup failed for org {org_id_int}: {e}")
            finally:
                await redis.delete(f"session:{session_id}")
        else:
            print(f"[Logout] {org_connections_count} connections remain active for org {org_id_int}.")
    else:
        print(f"[Logout] No org_id available for user {user_id}; skipped org cleanup.")


    # Add a flash message in Redis
    lang = request.cookies.get("lang", "hu") 

    flash_id = str(uuid.uuid4())
    if reason == "expired":
        flash_message = {
            "text": "Your session expired due to inactivity. You have been logged out automatically." if lang == "en" else "Az időkorlát lejárt, ezért a kijelentkezés automatikusan megtörtént.",
            "category": "warning"
        }
    else:
        flash_message = {
            "text": "You have been logged out successfully." if lang == "en" else "Sikeresen kijelentkeztél.",
            "category": "success"
        }
    await redis.setex(f"flash:{flash_id}", FLASH_EXPIRE_SECONDS, json.dumps(flash_message))

    # Embed flash_id directly in post_logout_redirect_uri
    redirect_uri = quote(f"https://redrain1230.loophole.site/?flash_id={flash_id}", safe="")
    logout_url = (
        f"https://{tenant}.b2clogin.com/{tenant}.onmicrosoft.com/"
        f"{policy}/oauth2/v2.0/logout?post_logout_redirect_uri={redirect_uri}"
    )

    response = RedirectResponse(url=logout_url)
    response.delete_cookie("session_id")
    
        # if "?" in response.headers["Location"]:
        #     response.headers["Location"] += f"&flash_id={flash_id}"
        # else:
        #     response.headers["Location"] += f"?flash_id={flash_id}"

    # Redirect to the B2C logout URL
    return response




@router.post("/update_language")
async def update_language(request: Request, user: dict = Depends(get_current_user)):
    """
    Update the logged-in user's preferred language.
    Expects JSON: { "language": "en" } or { "language": "hu" }
    """
    data = await request.json()
    language = data.get("language")
    print(language)

    if language not in ["en", "hu"]:
        raise HTTPException(status_code=400, detail="Invalid language")

    async with async_session_scope() as db_session:
        result = await db_session.execute(select(User).where(User.id == user["id"]))
        db_user = result.scalar_one_or_none()
        if not db_user:
            raise HTTPException(status_code=404, detail="User not found")

        db_user.language = language
        # SQLAlchemy async session commits automatically at the end of async_session_scope

    
    session_id = request.cookies.get("session_id")
    if session_id:
        redis = request.app.state.redis_client
        await redis.hset(f"session:{session_id}", "language", language)

    return JSONResponse({"success": True})






    ################
    # Payment part #
    ################




@router.get("/subscription", response_class=HTMLResponse)
async def subscription(request: Request,
    csrf_protect: CsrfProtect=Depends(),
    current_user: dict = Depends(login_required),
    ):

    
    email = current_user["email"]
    email_prefix = email.split("@")[0] if email else ""
    user_id = current_user["id"]
    user_org = current_user["org_id"]
    name = current_user["name"]
    user_role = current_user["role"]
    language = current_user.get("language", "hu")
    first_character = current_user.get("first_character")

    if language not in ["en", "hu"]:
      print("[DEBUG] Invalid lang detected, defaulting to 'hu'")
      lang = "hu"

    async with async_session_scope() as db_session:
        client = await db_session.scalar(
            select(Client)
            .options(joinedload(Client.subscription))
            .where(Client.id == user_org)
        )

        if client:
            client_name = client.client_name
            subscription = client.subscription
            if subscription:
                # fetch active price from SubscriptionPrice table
                active_price = await db_session.scalar(
                    select(SubscriptionPrice)
                    .where(
                        SubscriptionPrice.subscription_id == subscription.id,
                        SubscriptionPrice.active == True
                    )
                )
                if active_price:
                    price = active_price.amount  # in minor units, e.g., 10000 HUF
        else:
            client_name = "Your Company"
            service_message = "Please contact Red Rain to select a service."

    # Check access permissions based on the subscription
    if subscription:
        chat_control_access = has_permission(user_role, subscription, "chat_control")
        basic_metrics_access = has_permission(user_role, subscription, "basic_metrics")
        enhanced_metrics_access = has_permission(user_role, subscription, "enhanced_metrics")
        advanced_ai_access = has_permission(user_role, subscription, "advanced_ai")
    else:
        chat_control_access = False
        basic_metrics_access = False
        enhanced_metrics_access = False
        advanced_ai_access = False


    csrf_token, signed_token = csrf_protect.generate_csrf_tokens()
    response = templates.TemplateResponse(
        "subscription.html",
        {
            "request": request,
            "user_role": user_role,
            "First_character": first_character,
            "email_prefix": email_prefix,
            "name": name,
            "client": client,
            "client_name": client_name,
            "subscription": subscription,
            "price": price,
            "chat_control_access": chat_control_access,
            "basic_metrics_access": basic_metrics_access,
            "enhanced_metrics_access": enhanced_metrics_access,
            "advanced_ai_access": advanced_ai_access,
            
            "language": language,
            "user_id": user_id,
            "csrf_token": csrf_token,  # <-- pass CSRF to template
        },
    )
    csrf_protect.set_csrf_cookie(signed_token, response)  # <-- set cookie
    return response



@router.post("/subscription/change")
async def change_subscription(
    client_id: int = Form(...),
    plan_id: int = Form(...),
    auto_upgrade: bool | None = Form(None),
    seats: int = Form(...),
    current_user: dict = Depends(login_required),
):
    lang = current_user.get("language", "hu")
    if lang not in ["hu", "en"]:
        lang = "hu"

    messages = []
 
    try:
        async with async_session_scope() as db_session:
            # Get client
            client = await db_session.scalar(
                select(Client).where(Client.id == client_id)
            )
            
            if not client:
                messages.append(msg(
                    text_en="Client not found.",
                    text_hu="Az ügyfél nem található.",
                    category="danger",
                    lang=lang
                ))
                return JSONResponse({"messages": messages, "success": False})
            
            if client.id != current_user["org_id"]:
                return JSONResponse({"success": False})
            
            client.auto_upgrade = bool(auto_upgrade)
            # Get subscription plan
            subscription_plan = await db_session.scalar(
                select(Subscription).where(Subscription.id == plan_id)
            )
            if not subscription_plan:
                messages.append(msg(
                    text_en="Subscription plan not found.",
                    text_hu="Az előfizetési csomag nem található.",
                    category="danger",
                    lang=lang
                ))
                return JSONResponse({"messages": messages, "success": False})
            


            # Retrieve Stripe subscription This grabs the existing Stripe subscription.
            stripe_sub = stripe.Subscription.retrieve(client.stripe_subscription_id)
            stripe_item_id = stripe_sub["items"]["data"][0]["id"]

            # Compute total quantity: base seat + additional seats
            quantity = subscription_plan.base_seats + seats

            # Update Stripe subscription
            stripe.Subscription.modify(
                client.stripe_subscription_id,
                items=[{
                    "id": stripe_item_id,
                    "price": subscription_plan.stripe_price_id,
                    "quantity": quantity
                }]
            )

            # Detect changes for flash messages
            plan_changed = client.subscription_id != subscription_plan.id
            seats_changed = client.seats != seats

            # Update database
            client.subscription_id = subscription_plan.id
            client.seats = seats
            await db_session.commit()

            # Prepare language-aware flash message
            if plan_changed and seats_changed:
                messages.append(msg(
                    text_en="Subscription and additional seats updated successfully!",
                    text_hu="Előfizetés és további ülések frissítve!",
                    category="success",
                    lang=lang
                ))
            elif plan_changed:
                messages.append(msg(
                    text_en="Subscription plan updated successfully!",
                    text_hu="Előfizetés frissítve!",
                    category="success",
                    lang=lang
                ))
            elif seats_changed:
                messages.append(msg(
                    text_en="Additional seats updated successfully!",
                    text_hu="További ülések frissítve!",
                    category="success",
                    lang=lang
                ))
            else:
                messages.append(msg(
                    text_en="No changes made.",
                    text_hu="Nincs változtatás.",
                    category="info",
                    lang=lang
                ))

            return JSONResponse({"messages": messages, "success": True})

    except stripe.error.StripeError as e:
        # Stripe-specific error
        messages.append(msg(
            text_en=f"Stripe error: {str(e)}",
            text_hu=f"Stripe hiba: {str(e)}",
            category="danger",
            lang=lang
        ))
        return JSONResponse({"messages": messages, "success": False})
    except Exception as e:
        # General error
        messages.append(msg(
            text_en=f"An error occurred: {str(e)}",
            text_hu=f"Hiba történt: {str(e)}",
            category="danger",
            lang=lang
        ))
        return JSONResponse({"messages": messages, "success": False})

  



                                    #---------------------------
                                    #----    ADMIN PAGE     ----
                                    #---------------------------   
                                    
                                    
                                    #---------------------------
                                    #----    ADMIN PAGE     ----
                                    #---------------------------










                                    #---------------------------
                                    #----    ADMIN PAGE     ----
                                    #---------------------------   
                                    
                                    
                                    #---------------------------
                                    #----    ADMIN PAGE     ----
                                    #---------------------------

import json
import time


from datetime import datetime, timedelta





import threading
import time
import json
from datetime import datetime, timedelta


BLOB_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")

blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)



def normalize_timestamp(ts: str) -> str:
    """
    Convert any timestamp string into ISO8601 UTC format without microseconds:
    '2025-08-17 17:42:59' -> '2025-08-17T17:42:59Z'
    '2025-08-17T17:42:59.104581+00:00' -> '2025-08-17T17:42:59Z'
    """
    if not ts:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
    
    # Try to parse ISO or space-separated datetime
    try:
        # Replace space with T if needed
        ts_fixed = ts.replace(' ', 'T') if ' ' in ts and 'T' not in ts else ts
        dt = datetime.fromisoformat(ts_fixed.replace('Z', '+00:00'))
        # Force UTC and remove microseconds
        dt = dt.astimezone(timezone.utc).replace(microsecond=0)
        return dt.isoformat().replace('+00:00', 'Z')
    except Exception as e:
        # Fallback to current UTC if parsing fails
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')





# async def classify_topic(input_text: str) -> str:

#      # WAIT until model is loaded
#     await fastapi_app.state.models_loaded_event.wait()


#     minilm_model = fastapi_app.state.minilm_model_encoder_for_clf_classifier
#     lr_classifier = fastapi_app.state.lr_classifier

#     if minilm_model is None or lr_classifier is None:
#         raise RuntimeError("Models not available")

#     label_reverse = {
#     0: "Termékérdeklődés",
#     1: "Vásárlási szándék",
#     2: "Ár és promóció",
#     3: "Panaszok és problémák",
#     4: "Szolgáltatás",
#     5: "Egyéb"
#     }
#     emb = await asyncio.to_thread(minilm_model.encode, [input_text])
#     pred = await asyncio.to_thread(lr_classifier.predict, [emb[0]])
#     return label_reverse[pred]





# async def classify_and_save(payload: dict, redis_client):
#     """
#     Wait for chatbot DB insert, then classify topic and update topic_classification field.
#     """
#     try:
#         message = payload["message"]
#         user_id = message["user_id"]
#         org_id = message["org_id"]
#         standalone_prediction = message["standalone_prediction"]
#         user_message = message["user_message"]
#         non_standalone_input = message["input_for_not_standalone_topic_classification"]
#         bot_message = message["bot_message"]
#         saved_flag_key = message.get("saved_flag_key")
#         created_at_str = message["timestamp"]

#         #we are converting the time str got from redis it back into a datetime
#         created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))

#         # --- Wait until chatbot side finishes DB insert ---
#         if saved_flag_key:
#             print(f"Waiting for Redis save flag: {saved_flag_key}")
#             for _ in range(60):  # wait up to 30 seconds
#                 exists = await redis_client.exists(saved_flag_key)
#                 if exists:
#                     print(f"Redis save flag detected: {saved_flag_key}")
#                     break
#                 await asyncio.sleep(0.5)
#             else:
#                 print(f"Timeout waiting for DB save flag for {saved_flag_key}")
#                 return
#         else:
#             print("No saved_flag_key provided in payload; proceeding anyway.")

#         # --- Choose input for topic classification ---
#         classification_input = (
#             user_message if standalone_prediction == 0 else non_standalone_input
#         )

#         # --- Run model inference (non-blocking) ---
#         try:
#             topic = await classify_topic(classification_input)
#             print(f"Predicted topic: {topic}")
#         except Exception as e:
#             print("Topic classification failed")
#             topic = "unknown"

#         # --- Update the database record ---
#         async with async_session_scope() as session:
#             try:
#                 stmt = (
#                     update(ChatHistory)
#                     .where(
#                         ChatHistory.user_id == user_id,
#                         ChatHistory.client_id == org_id,
#                         ChatHistory.message == user_message,
#                         ChatHistory.response == bot_message,
#                         ChatHistory.created_at.between(created_at - timedelta(seconds=1), created_at + timedelta(seconds=1))
#                     )
#                     .values(topic_classification=topic)
#                     .execution_options(synchronize_session="fetch")
#                 )
#                 result = await session.execute(stmt)
              
              

#                 if result.rowcount == 0:
#                     print(
#                         f"No chat record found for "
#                         f"user_id={user_id}, org_id={org_id}, message={user_message[:50]}"
#                     )
#                 else:
#                     print(f"Topic classification saved for {result.rowcount} record(s).")

#             except SQLAlchemyError as e:
#                 await session.rollback()
#                 print(f"Database update failed: {e}")
#                 raise

#     except Exception as e:
#         print(f"classify_and_save() failed: {e}")




# async def fetch_last_msgs_pipeline(org_id: int, redis_client: aioredis.Redis, batch_size: int = 10000):
#     """
#     Fetch last 4 messages per user for a tenant (org_id) from DB + blobs.
#     Update Redis incrementally in batches, stop early for users who already have 4 messages.
#     """

#     user_msgs = {}  # in-memory per-user message collection

#     # --- Step 1: Fetch from DB ---
#     async with async_session_scope(org_id=org_id) as session:
#         stmt = (
#             select(ChatHistory)
#             .where(ChatHistory.client_id == org_id)
#             .order_by(desc(ChatHistory.created_at))
#         )
#         stream = await session.stream(stmt)
#         count = 0

#         async for row in stream:
#             msg: ChatHistory = row[0]
#             uid = msg.user_id

#             if uid not in user_msgs:
#                 user_msgs[uid] = []

#             if len(user_msgs[uid]) < 4:
#                 user_msgs[uid].append({
#                     "timestamp": msg.created_at.isoformat(),
#                     "user_message": msg.message,
#                     "bot_message": msg.response,
#                     "agent": msg.agent or "bot",       # 'bot' for automatic, admin name for manual
#                     "mode": msg.mode or "automatic"
#                 })

#             count += 1
#             if count % batch_size == 0:
#                 await _update_redis_batch(org_id, user_msgs, redis_client)
#                 # Keep only users who still need more messages
#                 user_msgs = {uid: msgs for uid, msgs in user_msgs.items() if len(msgs) < 4}

#             # Stop early if all users have 4 messages
#             if user_msgs and all(len(v) >= 4 for v in user_msgs.values()):
#                 break

#         if user_msgs:
#             await _update_redis_batch(org_id, user_msgs, redis_client)

#     # --- Step 2: Fetch from Blobs ---

#     async with BlobServiceClient.from_connection_string(BLOB_CONN_STR) as blob_service:
#         container_client = blob_service.get_container_client(CONTAINER_NAME)

#         async for blob in container_client.list_blobs(name_starts_with="chat_messages_"):
#             blob_client = container_client.get_blob_client(blob)
#             stream = await blob_client.download_blob()
#             data = await stream.readall()
#             lines = [json.loads(line) for line in data.decode("utf-8").splitlines()]

#             await _process_blob_lines_incremental_early(lines, org_id, redis_client, batch_size)


# async def _update_redis_batch(org_id: int, user_msgs: dict, redis_client: aioredis.Redis):
#     """
#     Write batch of user messages to Redis, merge with existing data, keep latest 4.
#     """
#     for uid, msgs in user_msgs.items():
#         if not msgs:
#             continue

#         key = f"tenant:{org_id}:user:{uid}:recent_msgs"
#         existing = await redis_client.get(key)
#         existing_msgs = json.loads(existing) if existing else []

#         # Merge DB messages with existing Redis messages
#         merged = msgs + existing_msgs
#         merged.sort(key=lambda m: m["timestamp"], reverse=True)
#         merged = merged[:4]

#         await redis_client.set(key, json.dumps(merged))


# async def _process_blob_lines_incremental_early(lines, org_id: int, redis_client: aioredis.Redis, batch_size: int):
#     """
#     Process blob batch with early stopping for users who already have 4 messages in Redis.
#     """
#     lines_buffer = []

#     for line in reversed(lines):  # newest first
#         lines_buffer.append(line)
#         if len(lines_buffer) >= batch_size:
#             await _process_blob_lines_buffer_early(lines_buffer, org_id, redis_client)
#             lines_buffer = []

#     if lines_buffer:
#         await _process_blob_lines_buffer_early(lines_buffer, org_id, redis_client)


# async def _process_blob_lines_buffer_early(lines, org_id: int, redis_client: aioredis.Redis):
#     """
#     Merge blob messages with Redis, skip users who already have 4 messages.
#     """
#     batch_user_msgs = {}

#     # Collect messages per user
#     for msg in lines:
#         if msg["client_id"] != org_id:
#             continue
#         uid = msg["user_id"]

#         # Check existing Redis messages first
#         key = f"tenant:{org_id}:user:{uid}:recent_msgs"
#         existing = await redis_client.get(key)
#         existing_msgs = json.loads(existing) if existing else []

#         # Skip users who already have 4 messages
#         if len(existing_msgs) >= 4:
#             continue

#         if uid not in batch_user_msgs:
#             batch_user_msgs[uid] = []

#         batch_user_msgs[uid].append({
#             "timestamp": msg["created_at"],
#             "user_message": msg["message"],
#             "bot_message": msg["response"],
#             "agent": msg.get("agent", "bot"),
#             "mode": msg.get("mode", "automatic")
#         })

#     # Merge and save back
#     for uid, new_msgs in batch_user_msgs.items():
#         key = f"tenant:{org_id}:user:{uid}:recent_msgs"
#         existing = await redis_client.get(key)
#         existing_msgs = json.loads(existing) if existing else []

#         merged = new_msgs + existing_msgs
#         merged.sort(key=lambda m: m["timestamp"], reverse=True)
#         merged = merged[:4]

#         await redis_client.set(key, json.dumps(merged))




###########################################################################
#                            REDIS
###########################################################################






# INACTIVITY_TIMEOUT_SECONDS = 30





# async def redis_listener():
#     redis_client = fastapi_app.state.redis_client

#     while True:  # Outer loop -> reconnects after inactivity or errors
#         try:
#             pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
#             await pubsub.subscribe("chatbot:messages")
#             print("Chatbot subscribed to Redis channel 'chatbot:messages'")

#             last_activity = datetime.utcnow()

#             while True:  # Inner loop -> process messages
#                 now = datetime.utcnow()

#                 # inactivity check
#                 if (now - last_activity) > timedelta(seconds=INACTIVITY_TIMEOUT_SECONDS):
#                     print(f"[WARNING] No message or heartbeat in {INACTIVITY_TIMEOUT_SECONDS} seconds.")
#                     break  # break inner loop → reconnect in outer loop

#                 # Poll for message (non-blocking with timeout)
#                 message = await pubsub.get_message(timeout=1.0)

#                 if message is None:
#                     await asyncio.sleep(0.1)  # small pause when idle
#                     continue

#                 if message["type"] != "message":
#                     continue

#                 try:
#                     data = json.loads(message["data"])
#                     last_activity = datetime.utcnow()  # reset inactivity timer

#                     # Handle heartbeat
#                     if data.get("type") in ("heartbeat", "admin_heartbeat"):
#                         print(f"[Heartbeat] Received at {data.get('timestamp')}")
#                         continue

#                     print("MESSAGE FROM REDIS:", data)

#                     org_id = str(data.get("message", {}).get("org_id"))
#                     msg = data.get("message", {})
#                     is_recurrent = msg.get("is_recurrent", {})

#                     if is_recurrent:
#                         # First-time setup for this tenant/user
#                         await fetch_last_msgs_pipeline(org_id, redis_client)



#                     user_id=msg.get("user_id")

#                     if "timestamp" in msg:
#                         msg["timestamp"] = normalize_timestamp(msg["timestamp"])

#                     event = await log_event(org_id, "new_message", msg)
#                     mode = get_client_mode(org_id)

#                     print("event!!!", event)

#                     #  HANDLING RECURRENT USER LOGIC

#                     user_cache_key = f"tenant:{org_id}:user:{user_id}:recent_msgs"

#                     # --- Step 1: Get current history ---
#                     # Fetch last 4 messages from Redis BEFORE adding the new one
#                     recent_msgs_raw = await redis_client.lrange(user_cache_key, 0, 3)
#                     recent_history = [json.loads(m) for m in recent_msgs_raw]
    

#                     if event and "data" in event:
#                         event["data"]["recent_history"] = recent_history

#                     # Step 2: Push the new message for next round
#                     new_msg = {
#                         "timestamp": msg.get("timestamp"),
#                         "user_message": msg.get("user_message"),
#                         "bot_message": msg.get("bot_message"),
#                     }
#                     await redis_client.lpush(user_cache_key, json.dumps(new_msg))
#                     await redis_client.ltrim(user_cache_key, 0, 3)

             

#                     # === AUTOMATIC MODE ===
#                     if mode == "automatic":

#                         if msg.strip().lower() in ["ügyintézőt kérek.", "please connect me to a colleague."]:
#                             pass

                        
#                         # -------- Emit message to all connected admins in the org --------

#                         try:
#                             org_key = f"org:{org_id}:connections"
#                             sids_bytes = await redis_client.smembers(org_key)  # returns set of bytes
#                             sids = [s.decode() for s in sids_bytes]
#                             sids = [row[0] for row in result.fetchall()]
#                             for sid in sids:
#                                 try:
#                                     await sio.emit(
#                                         "new_message_FirstUser",
#                                         {"messages": [event["data"]]
#                                             },
#                                         to=sid,
#                                     )
#                                 except Exception as emit_err:
#                                     print(f"Emit error to SID {sid}: {emit_err}")
#                         except OperationalError as db_err:
#                             print(f"[DB ERROR - Automatic] OperationalError: {db_err}")
#                         except Exception as db_err:
#                             print(f"[DB ERROR - Automatic] General DB error: {db_err}")

#                         # --- Run classification & saving in background ---
#                         asyncio.create_task(
#                         classify_and_save(data, redis_client)
# )
                    
#                     # === MANUAL MODE ===
#                     elif mode == "manual":
#                         try:
#                             async with async_session_scope() as session:
#                                 result = await session.execute(
#                                     select(Client.last_manualmode_triggered_by).where(
#                                         Client.id == org_id, Client.is_active.is_(True)
#                                     )
#                                 )
#                                 admin_user_id = result.scalar()

#                                 if admin_user_id:
                        

#                                     # Get all active sids for this org
#                                     org_connections_key = f"org:{org_id}:connections"
#                                     sids_bytes = await redis_client.smembers(org_connections_key)
#                                     sids = [s.decode() for s in sids_bytes]

#                                     # Find a connection that belongs to the admin_user_id
#                                     admin_sid = None
#                                     for sid in sids:
#                                         conn_data = await redis_client.hgetall(f"connection:{sid}")
#                                         if conn_data.get(b"user_id") and int(conn_data[b"user_id"]) == int(admin_user_id):
#                                             admin_sid = sid
#                                             break

#                                     if admin_sid:
#                                         await sio.emit(
#                                             "new_message_FirstUser",
#                                             {"messages": [event["data"]]
#                                              },
#                                             to=admin_sid,
#                                         )
#                                         print(f"Emitted to SID {admin_sid} for user {admin_user_id}")
#                                     else:
#                                         print(f"No active SID found for user_id {admin_user_id} in org {org_id}")
#                                 else:
#                                     print(f"No admin user_id found for org_id {org_id}")
#                         except OperationalError as e:
#                             print(f"[DB ERROR - Manual] OperationalError: {e}")
#                         except Exception as e:
#                             print(f"[DB ERROR - Manual] General DB error: {e}")

#                         # --- Run classification & saving in background ---
#                         asyncio.create_task(
#                         classify_and_save(data, redis_client)
#                         )
#                 except Exception as e:
#                     print(f"Error processing message from Redis: {e}")

#         except Exception as outer_e:
#             print(f"[Redis Listener] Connection lost, retrying in 5s... Error: {outer_e}")
#             await asyncio.sleep(5)
#         finally:
#             try:
#                 if pubsub:
#                     await pubsub.close()
#             except:
#                 pass


# async def send_admin_heartbeat():
#     redis_client = fastapi_app.state.redis_client
#     while True:
#         try:
#             heartbeat_data = {
#                 "type": "admin_heartbeat",
#                 "timestamp": datetime.utcnow().isoformat()
#             }
#             await redis_client.publish("chatbot:admin_heartbeat", json.dumps(heartbeat_data))
#             print(f"[Admin Heartbeat] Sent at {heartbeat_data['timestamp']}")
#             await asyncio.sleep(10)  # normal interval
#         except (aioredis.exceptions.ConnectionError, asyncio.TimeoutError) as e:
#             # transient failure → retry sooner
#             print(f"[Admin Heartbeat Transient Error] {e}, retrying in 2s")
#             await asyncio.sleep(2)
#         except Exception as e:
#             # permanent / unknown failure → log and wait longer
#             print(f"[Admin Heartbeat Fatal Error] {e}, retrying in 30s")
#             await asyncio.sleep(30)


# redis_password=os.environ.get("redis_password")
# redis_host = "aichatbotredis111.redis.cache.windows.net"
# redis_port = 6380
# redis_url = f"rediss://default:{redis_password}@{redis_host}:{redis_port}"




###########################################################################
#                 End of REDIS CONFIG
###########################################################################



@router.post("/api/upload_image")
async def upload_image(
    file: UploadFile = File(...),
    org_id: str = Form(...),
    user_id: str = Form(...)
):
    """
    Uploads a file to Azure Blob Storage under:
    imageuploads/org_id/user_id/unique_filename
    """
    try:
        # Generate file extension and path
        extension = os.path.splitext(file.filename)[1]
        blob_name = f"imageuploads/{org_id}/{user_id}/{uuid.uuid4()}{extension}"

        # Upload directly to Azure Blob Storage
        blob_client = container_client.get_blob_client(blob_name)
        await blob_client.upload_blob(file.file, overwrite=True, content_type=file.content_type)

        # Construct public URL
        blob_url = blob_client.url

        return {
            "image_url": blob_url,
            "org_id": org_id,
            "user_id": user_id,
            "file_name": file.filename
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")



@router.get("/chatAdminPage", response_class=HTMLResponse)
async def chat_admin_page(
    request: Request,
    first_character: str | None = Query(None),
    user: dict = Depends(login_required),  # ensures user is logged in
):
 
    redis = request.app.state.redis_client
    session_id = request.cookies.get("session_id")
    session_key = f"session:{session_id}"

    # If session expired in Redis → logout
    if not await redis.exists(session_key) or not await redis.hget(session_key, "user_id"):
        return RedirectResponse(
        url="/logout?reason=expired",
        status_code=302
    )
    

    user_id = user["id"]
    user_org = user["org_id"]
    language = user.get("language", "hu")
    first_character = user.get("first_character")

    
    return templates.TemplateResponse(
        "admin_dashboard.html",
        {
            "request": request,
            "user_org": user_org,
            "first_character": first_character,
            "language": language,
            "user_id": user_id,
        },
    )





async def get_client_mode(org_id: str) -> str:
    """
    Retrieve the mode for a specific organization from the clients table.
    If the organization is not found, return 'automatic' as the default mode.
    """
    async with async_session_scope() as db_session:
        try:
            result = await db_session.execute(
                select(Client.mode).where(Client.id == org_id)
            )
            mode = result.scalar_one_or_none()

            if mode:
                print(f"Retrieved mode for org_id={org_id}: {mode}")
                return mode
            else:
                print(f"No mode found for org_id={org_id}. Defaulting to 'automatic'.")
                return "automatic"

        except Exception as e:
            print(f"Error retrieving client mode: {e}")
            raise


async def get_sorted_event_logs(org_id: str, since_timestamp=None) -> list[dict]:
    """
    Retrieve and sort the event logs for the given organization by timestamp using async streaming.
    """
    events = []

    try:
        org_int = int(org_id)

        async with async_session_scope() as db_session:
            stmt = select(OrgEventLog).where(OrgEventLog.org_id == org_int)
            if since_timestamp:
                stmt = stmt.where(OrgEventLog.timestamp > since_timestamp)
            stmt = stmt.order_by(OrgEventLog.timestamp)

            # Use async streaming
            stream_result = await db_session.stream(stmt)
            async for event in stream_result.scalars():
                events.append({
                    "org_id": event.org_id,
                    "event_type": event.event_type,
                    "data": event.data,
                    "timestamp": event.timestamp,  # optionally .isoformat() if needed
                })

        return events

    except Exception as e:
        print(f"Error retrieving event logs for org_id={org_id}: {e}")
        raise

    


async def log_event(org_id, event_type, data, frontend_time=None):
    try:
        # Determine timestamp (frontend or fallback)
        timestamp = frontend_time or data.get("message", {}).get("timestamp")
        if timestamp:
            # Handle ISO 8601 with 'Z' (UTC) properly
            timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        else:
            timestamp = datetime.now(timezone.utc)

        async with async_session_scope() as session:
            event = OrgEventLog(
                org_id=org_id,
                event_type=event_type,
                data=data,
                timestamp=timestamp,
            )
            session.add(event)

            # Flush writes pending INSERT, commit persists permanently
          

            return {
                "org_id": event.org_id,
                "event_type": event.event_type,
                "data": event.data,
                "timestamp": event.timestamp,
            }

    except Exception as e:
        # Log the exception with traceback if needed
        print(f"Error logging event for org {org_id}: {e}")
        return None

        



async def get_recent_messages(org_id: str, minutes: int = 15, mode: str = "automatic"):
    """
    Retrieve recent 'new_message' events for the given organization within the last `minutes`.
    Uses async streaming to avoid loading all rows into memory at once.
    """
    current_time = datetime.now(timezone.utc)
    cutoff_time = current_time - timedelta(minutes=minutes)
    day_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)

    print("Current time:", current_time)
    print("Cutoff time:", cutoff_time)

    try:
        async with async_session_scope() as session:
            # Step 1: Stream all recent events in last N minutes
            recent_events = []
            user_ids = set()

            stream_result = await session.stream(
                select(OrgEventLog)
                .where(
                    OrgEventLog.org_id == org_id,
                    OrgEventLog.event_type == "new_message",
                    OrgEventLog.timestamp >= cutoff_time,
                )
                .order_by(OrgEventLog.timestamp)
            )

            async for event in stream_result.scalars():
                recent_events.append(event)
                if event.data and event.data.get("user_id"):
                    user_ids.add(event.data["user_id"])

            print(f"Found {len(recent_events)} 'new_message' events for org_id={org_id}")
            print(f"Active user_ids in last {minutes} minutes:", user_ids)

            if not user_ids:
                return []

            # Step 2: Stream all today's events for those active users
            todays_events = []

            stream_result = await session.stream(
                select(OrgEventLog)
                .where(
                    OrgEventLog.org_id == org_id,
                    OrgEventLog.event_type == "new_message",
                    OrgEventLog.timestamp >= day_start,
                    OrgEventLog.data["user_id"].astext.in_(list(user_ids)),
                )
                .order_by(OrgEventLog.timestamp)
            )

            async for event in stream_result.scalars():
                todays_events.append(event)

            print(f"Found {len(todays_events)} 'new_message' events today for active users")

            # Step 3: Convert events to dictionaries before returning
            return [
                {
                    "org_id": event.org_id,
                    "event_type": event.event_type,
                    "data": event.data,
                    "timestamp": (
                        event.timestamp.timestamp()
                        if isinstance(event.timestamp, datetime)
                        else float(event.timestamp)
                    ),
                }
                for event in todays_events
            ]

    except Exception as e:
        print("Error querying recent messages:", e)
        raise



async def should_show_internal_alert(sid: str, redis, org_id: str) -> bool:
    
    try:
        # Get connection data for this sid
        conn_data = await redis.hgetall(f"connection:{sid}")
        if not conn_data:
            print(f"No connection found for sid={sid}")
            return False

        # Decode byte values (Redis returns bytes)
        conn_data = {k.decode(): v.decode() for k, v in conn_data.items()}

        open_ts = conn_data.get("admin_internal_message_open")
        close_ts = conn_data.get("admin_internal_message_close")

        # Convert timestamps to datetime
        open_time = (
            datetime.fromtimestamp(int(open_ts) / 1000, tz=timezone.utc)
            if open_ts and open_ts != "null"
            else None
        )
        close_time = (
            datetime.fromtimestamp(int(close_ts) / 1000, tz=timezone.utc)
            if close_ts and close_ts != "null"
            else None
        )

        # Query latest internal message for org
        async with async_session_scope() as session:
            result = await session.execute(
                select(OrgEventLog)
                .where(
                    OrgEventLog.org_id == org_id,
                    OrgEventLog.event_type == "admin_internal_message",
                )
                .order_by(OrgEventLog.timestamp.desc())
            )
            latest_msg = result.scalars().first()

            # No messages, no alert
            if not latest_msg:
                return False

            latest_msg_time = latest_msg.timestamp

            # 1) Dropdown never opened, but messages exist => show alert
            if open_time is None:
                return True

            # 2) Dropdown opened but never closed => no alert
            if open_time and close_time is None:
                return False

            # 3) Dropdown closed => show alert if messages arrived after closing
            if close_time and latest_msg_time > close_time:
                return True

            return False

    except Exception as e:
        print(f"Error checking internal alert for user {sid}: {e}")
        return False



from sqlalchemy import select
import json

@sio.on("admin_response_to_the_chatbot")
async def handle_admin_response(sid, data):
    """Handle admin messages sent to chatbot and forward them to Redis with admin name and org_id."""

    print(f"Admin response received from sid={sid}: {data}")

    # --- Retrieve org_id and user_id from Connections ---
    org_id = None
    user_id = None
    admin_name = None

    try:
        # Fetch connection data from Redis
        connection_key = f"connection:{sid}"
        conn_data = await redis.hgetall(connection_key)

        if not conn_data:
            print(f"No connection found for socket ID: {sid}")
            return

        # Extract org_id and user_id
        org_id_bytes = conn_data.get(b"org_id")
        try:
            org_id = int(org_id_bytes.decode()) if org_id_bytes else None
        except (ValueError, AttributeError):
            org_id = None
            print(f"[Warning] Invalid org_id in Redis for SID {sid}: {org_id_bytes}")

        # Extract user_id safely
        user_id_bytes = conn_data.get(b"user_id")
        try:
            user_id = int(user_id_bytes.decode()) if user_id_bytes else None
        except (ValueError, AttributeError):
            user_id = None
            print(f"[Warning] Invalid user_id in Redis for SID {sid}: {user_id_bytes}")
        
        if user_id:
            
            # Fetch from PostgreSQL User table
            async with async_session_scope() as session:
                user = await session.scalar(select(User).where(User.id == user_id))
                if user:
                    admin_name = user.name or "Unknown Admin"

    except Exception as e:
        print(f"Database error while retrieving org_id or admin name: {e}")
        return

    if not org_id:
        print("org_id not found, aborting message forwarding.")
        return

    # --- Enrich message data ---
    data["admin_name"] = admin_name

    # --- Publish to Redis ---
    try:
        environ = sio.get_environ(sid)
        app = environ.get("asgi.scope", {}).get("app")

        if not app:
            print("FastAPI app not available")
            return

        if not hasattr(app.state, "redis_client"):
            print("Redis not available")
            return

        redis = app.state.redis_client

        await redis.publish("chatbot:messages", json.dumps(data))
        print(f"Published admin message to Redis: org_id={org_id}, admin={admin_name}")
    except Exception as e:
        print(f"Redis publish error: {e}")

















    ##########################             ##########################

    ##########################             ##########################
    #   CONNECT      #########             #   CONNECT      #########
    ##########################             ##########################

    ##########################             ##########################


LOCK_TIMEOUT = 30  # seconds

async def acquire_redis_lock(redis, key: str, lock_id: str, expire: int = LOCK_TIMEOUT) -> bool:
    """
    Try to acquire a Redis lock for a given key.
    Returns True if acquired, False if already locked.
    """
    return await redis.set(key, lock_id, nx=True, ex=expire)


async def release_redis_lock(redis, key: str, lock_id: str):
    """
    Release the lock only if it is still held by us.
    """
    script = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
    """
    await redis.eval(script, keys=[key], args=[lock_id])

@sio.event
async def connect(sid, environ, auth): # connect() is special → Socket.IO passes environ directly
    #other events (@sio.on(...)) are not → you must fetch it yourself with sio.get_environ(sid)
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    app = environ.get("asgi.scope", {}).get("app")
    redis = app.state.redis_client
    session_id = None
    for key, value in cookies:
        if key == b'cookie':
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]
    

  
    if not session_id or not await redis.exists(f"session:{session_id}"):
        # Session expired → tell client to logout and disconnect
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further connection handling
    
    
    

    print("NÉZZÜK MI VAN ITT!!!")
    print(f"New connection: {sid}")  # Log each WebSocket connection 
    query = environ.get("asgi.scope", {}).get("query_string", b"").decode()
    params = parse_qs(query)
    user_id = params.get("user_id", [None])[0]
    org = params.get("user_org", [None])[0]
    socket_id = sid
    client_timezone = "UTC"

    print("USErID: ", user_id, "ORG:", org)



    try:
        user_id_int = int(user_id)
        org_int = int(org)
    except (TypeError, ValueError):
        # Either user_id or org is missing or not numeric
        print(f"Invalid user_id or org: user_id={user_id}, org={org}")
        await sio.emit("force_logout", {"reason": "Invalid session"}, to=sid)
        await sio.disconnect(sid)
        return
    

    lock_key = f"org_connect_cleanup_lock:{org_int}"
    lock_id = str(uuid.uuid4())
    acquired = await acquire_redis_lock(redis, lock_key, lock_id, expire=30)
    if acquired:
        try:
            async with async_session_scope() as session:
                # Check if cleanup is needed (no active connections, but stale entries exist)
                active_sockets = await redis.scard(f"org:{org_int}:connections")

                if active_sockets == 0:
                    # Perform missing cleanup if necessary
                    print(f"[Connect] Performing missing cleanup for org {org_int}")
                    await session.execute(delete(OrgEventLog).where(OrgEventLog.org_id == org_int))
                    await session.execute(update(Client).where(Client.id == org_int).values(mode="automatic"))
                    await redis.delete(f"user_mode_override:{org_int}")
                    await session.execute(
                        update(User)
                        .where(User.client_id == org_int, User.is_deleted.is_(False))
                        .values(admin_internal_message_open=None)
                    )
                    client_to_update = await session.scalar(
                        select(Client)
                        .where(Client.id == org_int, Client.is_active.is_(True), Client.last_manualmode_triggered_by.isnot(None))
                    )
                    if client_to_update:
                        client_to_update.last_manualmode_triggered_by = None
                        print(f"[Connect] Updated client {client_to_update.id}: last_manualmode_triggered_by cleared")
        finally:
            await release_redis_lock(redis, lock_key, lock_id)
    else:
        print(f"[Connect] Another worker is handling cleanup for org {org_int}")


    
    if org_int:
      try:
          async with async_session_scope() as session:
             #This prevents phantom connections for deleted or non-existent users.  
            user = await session.get(User, user_id_int)
            if not user:
                await sio.emit("force_logout", {"reason": "User not found"}, to=sid)
                await sio.disconnect(sid)
                return
            
            
            # Regarding Redis Data Model, I use two complementary structures:
            # 1.) Stores metadata about a single socket, Created the hash object when the user connects, deleted when they disconnect.
            
            # Key:   connection:{socket_id}
            # Value: {
            # "user_id": <int>,
            # "org_id": <int>
            # "manualmode_triggered": "false" | "true",
            # "disconnected_at": <ISO timestamp or "null">
            # }

            # 2.) SET per organization, Keeps track of which sockets belong to which org:
            # Key:   org:{org_id}:connections
            # Value: { socket_id_1, socket_id_2, ... }



            # This allows you to:
            # Quickly look up all active sockets for an org (SMEMBERS)
            # Quickly check which org a socket belongs to (HGET connection:{sid} org_id)
            
            connection_key = f"connection:{socket_id}"
            org_set_key = f"org:{org_int}:connections"
            # ------------------------
            # I SHOULD REMOVE THE THE CLEANING IF I WANT TO ALLOW THAT USER CAN LOGIN ON MULTIPLE DEVICES AT THE SAME TIME AND SEE THE RESULTS IN REAL TIME

            # Check for existing connection
            # existing_sockets = await redis.smembers(org_set_key)
            # for s in existing_sockets:
            #     s = s.decode() if isinstance(s, bytes) else s
            #     u_id = await redis.hget(f"connection:{s}", "user_id")
            #     if u_id and int(u_id) == user_id_int:
            #         # Remove old duplicate socket
            #         await redis.srem(org_set_key, s)
            #         await redis.delete(f"connection:{s}")
            #         print(f"Removed old connection for user {user_id_int} in org {org_int}")
            
            # --------------------------------

            # Save new connection info
            await redis.hset(connection_key, mapping={
                "user_id": user_id_int,
                "org_id": org_int,
                "manualmode_triggered": "false",
                "disconnected_at": "null",
                "admin_internal_message_open": "null",  # initially closed
                "admin_internal_message_close": "null",
            })
            await redis.sadd(org_set_key, socket_id)
            await redis.expire(connection_key, 3600 * 6)  # optional TTL for safety
            print(f"Connection saved: {connection_key}")

            client = await session.get(Client, org_int)
            if client and client.timezone:
                client_timezone = client.timezone

      except Exception as e:
          print(f"Error saving connection: {e}")

      # Emit initial message (history or heartbeat)
      await sio.emit(
          "history_start",
          {"socket_id": socket_id, "org": org_int, "timezone": client_timezone},
          to=socket_id
      )

@sio.event
async def heartbeat(sid):
    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client

    connection_key = f"connection:{sid}"

    # Refresh TTL for the connection itself
    await redis.expire(connection_key, 3600 * 6)  # 6 hours

    # Get org_id from the connection hash
    connection_data = await redis.hgetall(connection_key)
    org_id_bytes = connection_data.get(b"org_id")
    if not org_id_bytes:
        print(f"[Heartbeat] No org_id found for socket {sid}")
        return

    org_id = org_id_bytes.decode() if isinstance(org_id_bytes, bytes) else str(org_id_bytes)

    # Refresh TTL for all tab_mode keys for this org
    tab_mode_keys = await redis.keys(f"org:{org_id}:tab:*:mode")
    # Decode keys if needed
    tab_mode_keys = [k.decode() if isinstance(k, bytes) else k for k in tab_mode_keys]

    # Refresh TTL concurrently
    if tab_mode_keys:
        await asyncio.gather(*(redis.expire(k, 3600 * 6) for k in tab_mode_keys))
        print(f"[Heartbeat] Refreshed TTL for {len(tab_mode_keys)} tab_mode keys in org {org_id}")


    # Refresh TTL for user_mode_override key if it exists
    user_mode_key = f"user_mode_override:{org_id}"
    exists = await redis.exists(user_mode_key)
    if exists:
        await redis.expire(user_mode_key, 3600 * 6)  # Extend TTL
        print(f"[Heartbeat] Refreshed TTL for user_mode_override key for org {org_id}")
    

@sio.on("history_ready")
async def handle_history_ready(sid, data):
    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")
    if not app or not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return
    redis = app.state.redis_client
    socket_id = data.get("socket_id")
    try:
        org = data.get("org")
        user_id = data.get("user_id")
        org=int(org)
        user_id=int(user_id)
    except (KeyError, TypeError, ValueError):
        print("Invalid org or user_id in history_ready")
        return

    try:
        missed_event_dicts = await get_sorted_event_logs(org)
        enriched_events = []

        async with async_session_scope() as session:
            client = await session.scalar(select(Client).where(Client.id == org))
            tz_name = client.timezone if client and client.timezone else "UTC"

            for event in missed_event_dicts:
                ts = event.get("timestamp")
                if isinstance(ts, datetime):
                    event["timestamp"] = ts.isoformat()
                    enrich_event_with_local_timestamp(event["data"], tz_name)
                else:
                    print(f"[SKIP] No timestamp found in event for org {org}")

                if event["event_type"] == "mode_changed":
                    mode = event["data"].get("mode")

                    connection_key = f"connection:{socket_id}"
                    # If socket exists, update manualmode_triggered in Redis
                    #exists = await redis.exists(connection_key)
                    if await redis.exists(connection_key) > 0:
                        await redis.hset(connection_key, "manualmode_triggered", str(mode == "manual").lower())
                        print(f"[Redis] Updated mode={mode} for org {org} socket {socket_id}")
                    else:
                        print(f"[Redis] No connection found for socket {socket_id}, skipping update")

                enriched_events.append(event)

        show_alert = await should_show_internal_alert(sid, redis, int(org))

        enriched_events.sort(key=lambda e: e["timestamp"])  # or better: store datetime for sorting

        await sio.emit(
            "event_log_batch",
            {"events": enriched_events, "show_chat_alert": show_alert},
            to=socket_id,
        )
        print(f"Emitted {len(enriched_events)} events in batch to socket {socket_id}")

    except Exception as e:
        print(f"Error during full history replay: {e}")

      



    #############################
    #   DISCONNECT      #########
    #############################





@sio.on("disconnect")
async def handle_disconnect(sid):
    print("SOCKET DISCONNECTED:", sid)
    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")
    if not app or not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return
    redis = app.state.redis_client
    try:
        # Get connection info from Redis
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)

        if not connection_data:
            print(f"[Redis] No connection record found for {sid}, skipping.")
            return
       

        if connection_data:
            # Decode Redis fields
            org_id_bytes = connection_data.get(b"org_id")
            org_id = int(org_id_bytes.decode()) if org_id_bytes else None

            user_id_bytes = connection_data.get(b"user_id")
            user_id = int(user_id_bytes.decode()) if user_id_bytes else None
            manual_mode_bytes = connection_data.get(b"manualmode_triggered", b"false")
            manual_mode = (manual_mode_bytes.decode() if isinstance(manual_mode_bytes, bytes) else str(manual_mode_bytes)) == "true"

            # Mark disconnected_at in Redis
            disconnected_at = datetime.utcnow().isoformat()
            await redis.hset(connection_key, "disconnected_at", disconnected_at)
            print(f"Admin {user_id} from org {org_id} disconnected with socket ID {sid}")

            async def cleanup_after_grace_period():
                await asyncio.sleep(15)  # Grace period

                lock_key = f"org_cleanup_lock:{org_id}"
                lock_id = str(uuid.uuid4())

                acquired = await acquire_redis_lock(redis, lock_key, lock_id)
                if not acquired:
                    print(f"[Grace Period] Another worker is already cleaning org {org_id}")
                    return  # Another worker is handling cleanup
                try:
                    try_count = 0
                    while try_count < 2:
                        try:   
                            conn = await redis.hget(connection_key, "disconnected_at")
                            if not conn or conn == b"null":
                                print(f"[Grace Period] User {user_id} reconnected, skip cleanup.")
                                return

                            # Check for other active admins (still connected)
                            org_set_key = f"org:{org_id}:connections"
                            active_sockets = await redis.smembers(org_set_key)
                            active_sockets = [s.decode() for s in active_sockets if s.decode() != sid]

                            print(f"[Grace Period] Active sockets for org {org_id}: {len(active_sockets)}")


                            if not active_sockets:
                                # No active connections → reset mode and clear entries
                                async with async_session_scope() as delayed_session:
                                    await delayed_session.execute(
                                        delete(OrgEventLog).where(OrgEventLog.org_id == org_id)
                                    )
                                    await delayed_session.execute(
                                        update(Client).where(Client.id == org_id).values(mode="automatic")
                                    )
                                    await redis.delete(f"user_mode_override:{org_id}")

                                    await delayed_session.execute(
                                        update(User)
                                        .where(User.client_id == org_id, User.is_deleted.is_(False))
                                        .values(admin_internal_message_open=None)
                                    )

                                    # Clear Redis tab mode keys for this org
                                    tab_mode_keys = await redis.keys(f"org:{org_id}:tab:*:mode")
                                    for key in tab_mode_keys:
                                        await redis.delete(key)
                                    
                                    client_to_update = await delayed_session.scalar(
                                        select(Client)
                                        .where(
                                            Client.id == org_id,
                                            Client.is_active == True,
                                            Client.last_manualmode_triggered_by.isnot(None),
                                        )
                                    )
                                    if client_to_update:
                                        client_to_update.last_manualmode_triggered_by = None
                                        print(f"Updated client {client_to_update.id}: last_manualmode_triggered_by cleared")
                                    
                                    print(f"Cleared entries and reset mode for org {org_id}")

                            else:

                              
                                # There are still active admins → propagate manualmode if needed
                                if manual_mode:
                                # Pick another still-active socket from the same org
                                    for s in active_sockets:
                                        other_conn = await redis.hgetall(f"connection:{s}")
                                        if other_conn:
                                            disconnected_at = other_conn.get(b"disconnected_at")
                                            if not disconnected_at or disconnected_at == b"null":
                                                # Found a valid still-connected admin → give them manual mode
                                                await redis.hset(f"connection:{s}", mapping={"manualmode_triggered": "true"})
                                                print(f"[Grace Period] Propagated manualmode_triggered from {sid} → {s}")
                                                break
                                # Finally, clean up this disconnected connection
                                await redis.srem(f"org:{org_id}:connections", sid)
                                await redis.delete(connection_key)
                                print(f"[Grace Period] Deleted disconnected user {user_id} (socket {sid}) from Redis")
                            break
                        except Exception as e:
                            try_count += 1
                            print(f"[Grace Period] Cleanup attempt {try_count} failed for socket {sid}: {e}")
                            if try_count < 2:
                                print("Retrying cleanup once more...")
                            else:
                                print("Cleanup failed after one retry, skipping.")
                finally:
                    await release_redis_lock(redis, lock_key, lock_id)
            asyncio.create_task(cleanup_after_grace_period())

        

    except Exception as e:
        print(f"Error handling disconnect for socket ID {sid}: {e}")


    ######################################         ######################################
    #   OVERALL MODE CHANGE      #########         #   OVERALL MODE CHANGE      #########
    ######################################         ######################################
    
def deduplicate_messages(recent_messages):
    unique_data = {}
    def make_hashable(value):
        """Recursively convert dictionaries to a hashable form (e.g., tuple or string)."""
        if isinstance(value, dict):
            return tuple(sorted((k, make_hashable(v)) for k, v in value.items()))
        elif isinstance(value, list):
            return tuple(make_hashable(v) for v in value)
        else:
            return value

    

    # Process each item in the data list
    for item in recent_messages:
        # Extract the data part (message or other information)
        data = item['data']
        
        # Handle the 'message' field if available
        message = data.get('message', data)  # If 'message' exists, use it; otherwise, use 'data'
        
        # Skip if the 'flag' field in the message is set to 'deleted'
        if message.get("flag") == "deleted":
            continue

        # Generate the unique key excluding the 'timestamp'
        # Example:

        #             from this:
        #             message = {
        #     'timestamp': '2025-03-13 19:30:10',
        #     'user_id': '558a54b6-9a7d-42e5-8b6f-75e7dc90941c',
        #     'org_id': '2',
        #     'user_message': 'nagyon jó ma',
        #     'bot_message': 'Awaiting Admin Response...',
        #     'latitude': 47.4984,
        #     'longitude': 19.0404,
        #     'location': 'Budapest'
        # }
        #   we have
        #             (
        #     ('bot_message', 'Awaiting Admin Response...'),
        #     ('latitude', 47.4984),
        #     ('location', 'Budapest'),
        #     ('longitude', 19.0404),
        #     ('org_id', '2'),
        #     ('user_id', '558a54b6-9a7d-42e5-8b6f-75e7dc90941c'),
        #     ('user_message', 'nagyon jó ma')
        # )


        unique_key = tuple(sorted((k, make_hashable(v)) for k, v in message.items() )) #if k != 'timestamp'))
        
        
        message_timestamp = float(item['timestamp'])
        
        
        if unique_key not in unique_data or unique_data[unique_key]['timestamp'] > message_timestamp:
            unique_data[unique_key] = item

    # Rebuild the original structure after deduplication
    final_data = []

    # Find the latest message (based on timestamp)
    latest_item = max(unique_data.values(), key=lambda x: float(x['timestamp']), default=None)


    # Rebuild the original structure from unique_data
    for unique_key, item in unique_data.items():
        # Recreate the structure using the original 'data' and other attributes
        data = item['data']
        
        # If 'message' is available in the data, ensure it's preserved correctly
        if 'message' in data:
            data = data['message']  # Preserve the message part
            data['manual_response'] = 'yes'
        
        final_data.append({
        
            'org_id': item['org_id'],
            'event_type': 'new_message',
            'data': data,
            'timestamp': item['timestamp']
        })
    
    final_data.sort(key=lambda x: float(x['timestamp']))
    return final_data





 #OVERALL  (createTABS handle manual mode)
@sio.on("mode_changed")
async def handle_mode_changed(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")
    if not app or not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return
    redis = app.state.redis_client
    cpu_pool = app.state.cpu_pool
    cpu_sem = app.state.cpu_sem

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    


    mode = data.get("mode")
    frontend_time = data.get("frontend_time")


    
    # --- Lookup connection info in Redis ---
    connection_key = f"connection:{sid}"
    connection_data = await redis.hgetall(connection_key)
    if not connection_data:
        print(f"[Redis] No connection found for SID {sid}")
        return

    org_id = connection_data.get(b"org_id")
    org_id = int(org_id.decode()) if org_id else None
        #org_modes[org_id] = mode
    
    
     # --- Update client mode ---
    try:
        await update_client_mode(org_id, mode)
    except Exception as e:
        print(f"Error updating client mode for org {org_id}: {e}")


    #emit('response_state_overall', {'org_id': org_id, 'state': mode}, to=chatbot_sid)
    await redis.publish("chatbot:state_updates", json.dumps({
            "org_id": org_id,
            "state": mode  # 'automatic' or 'manual'
        }))

    if mode == 'automatic':
        # Clear any stored admins or tab-related tracking for this org
        #orgs_triggered_create_tabs.pop(org_id, None)
        try:
            await redis.delete(f"user_mode_override:{org_id}")

        except Exception as e:
            print(f"Error deleting UserModeOverride for org {org_id}: {e}")


            # Delete all tab_mode keys for this org
        try:
            tab_mode_keys = await redis.keys(f"org:{org_id}:tab:*:mode")
            tab_mode_keys = [k.decode() if isinstance(k, bytes) else k for k in tab_mode_keys]

            if tab_mode_keys:
                await asyncio.gather(*(redis.delete(k) for k in tab_mode_keys))
                print(f"Deleted {len(tab_mode_keys)} tab_mode keys for org {org_id}")
        except Exception as e:
            print(f"Error deleting tab_mode keys for org {org_id}: {e}")
            


            
                

    if mode in ['automatic', 'manual']:
        print(f"Emitting mode_changed event to org_id {org_id} with mode: {mode}")
        asyncio.create_task(
            log_event(org_id, 'mode_changed', {'mode': mode}, frontend_time)
        )
                
       
        org_set_key = f"org:{org_id}:connections"
        active_sockets = await redis.smembers(org_set_key)
        active_sockets = [s.decode() for s in active_sockets if s.decode() != sid]

        
        
        for other_sid in active_sockets:
          
            try:
                await sio.emit("mode_changed", {"mode": mode}, to=other_sid)
            except Exception as emit_err:
                print(f"Emit error to SID {other_sid}: {emit_err}")


    if mode =='automatic':
        # Get the current timestamp
        # current_time = time.time()
        # recent_messages = [
        #     event for event in org_event_logs.get(org, [])
        #     if event['event_type'] == 'new_message' and (current_time - event['timestamp'] <= 900)
        # ]
        # recent_messages = sorted(recent_messages, key=lambda event: event['timestamp'])
        recent_messages = await get_recent_messages(org_id, minutes=15, mode='automatic')


        
        
        final_data = await run_cpu_task(deduplicate_messages, recent_messages, cpu_pool=cpu_pool, cpu_sem=cpu_sem)
        
        for e in final_data:
            asyncio.create_task(
                log_event(org_id, e['event_type'], e['data'])
            )
        # *: “unpacking operator”. It’s used to expand an iterable (like a list, tuple, or generator) into separate positional arguments. PL: numbers = [1, 2, 3] print(*numbers) result 1 2 3
      
        org_set_key = f"org:{org_id}:connections"
        active_sockets = await redis.smembers(org_set_key)
        active_sockets = [s.decode() for s in active_sockets if s.decode() != sid]

    
        # message_payload = []
        # for event in final_data:
        #     msg_with_ts = event['data'].copy()  # make a copy to avoid mutation
        #     msg_with_ts['timestamp'] = event['timestamp']
        #     message_payload.append(msg_with_ts)
        # SAME:  ** : creates a new dict that contains all keys/values from event['data'] plus a key 'timestamp' with the new value.
        message_payload = [
            {**event['data'], 'timestamp': event['timestamp']}
            for event in final_data
        ]
    
        if active_sockets :
            try:
                await asyncio.gather(
                    *(sio.emit('new_message_FirstUser', {'messages': message_payload}, to=sid) 
                    for sid in active_sockets ),
                    return_exceptions=True  # ensures one failed emit won't cancel others
                )
            except Exception:
                print(f"Emit error to some SID(s) for org_id {org_id}")
        else:
            print(f"No active SIDs to emit for org_id {org_id}")

    


                                            # RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
                                            
                                            #       RECTANGLE STATE - AUTOMATIC   UserModeOverride     R

                                            # RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR







@sio.on("update_response_state")  # the rectangles' state if it is manual or automatic
async def handle_update_response_state(sid, data):

    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")
    if not app or not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return
    redis = app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
    
    org_id=None
    try:
          connection_key = f"connection:{sid}"
          conn_data = await redis.hgetall(connection_key)

          if not conn_data:
              print(f"No connection found for socket_id {sid}")
              return

          # Extract org_id (and user_id if needed)
          org_id = int(conn_data.get(b"org_id", 0))
          if org_id is not None:
              org_id = int(org_id.decode())  # decode bytes → string → int
          else:
              org_id = 0  # or handle missing org_id


    except Exception as e:
      # Handle any unexpected errors
      print(f"An error occurred while processing the database operation: {e}")

    user_id = data.get('user_id')
    state = data.get('state')

    timestamp = data.get('frontend_time')  # ISO string
    

    asyncio.create_task(
        log_event(org_id, 'response_state_changed', {
            'user_id': user_id,
            'state': state
        }, timestamp)
    )
    # Handle database updates based on the state
      # "user_mode_override:42" = {
      #     "1001": "manual",
      #     "1002": "manual"
      # }
    try:
      key = f"user_mode_override:{org_id}"

      # Set or update the user's mode in the hash
      await redis.hset(key, user_id, "manual")

      # Set TTL for the entire hash (e.g., 6 hours)
      await redis.expire(key, 3600 * 6)  # 3600 sec × 6 = 6 hours

    except Exception as e:
          print(f"Error updating user_mode_override with TTL for org {org_id}, user {user_id}: {e}")


    # Broadcast the new state to all connected clients
    #emit('response_state_update', {'user_id': user_id, 'state': state}, room=org_id, include_self=False)

    try:
      org_key = f"org:{org_id}:connections"

      # Get all socket IDs for this org from Redis
      sids_bytes = await redis.smembers(org_key)
      sids = [s.decode() for s in sids_bytes]

      # Exclude the current sender's socket
      other_sids = [s for s in sids if s != sid]
      if other_sids:
          await asyncio.gather(
              *(sio.emit("response_state_update", {"user_id": user_id, "state": state}, to=s)
              for s in other_sids),
              return_exceptions=True
          )
        
    except Exception as db_err:
      print(f"Error fetching SIDs for org_id {org_id}: {db_err}")

    
    


    #emit('response_state_update2', {'user_id': user_id, 'org_id': org_id, 'state': state}, to=chatbot_sid)
    # PUBLISH TO REDIS INSTEAD OF EMIT TO SID
    try:
        await redis.publish(
            "chatbot:user_state_update",
            json.dumps(
                {
                    "user_id": user_id,
                    "org_id": org_id,
                    "state": state,
                    "timestamp": timestamp,
                }
            ),
        )
    except Exception as redis_err:
        print(f"Redis publish error: {redis_err}")



                                            # RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
                                            
                                            #     RECTANGLE STATE - OVERALMANUAL   UserModeOverride    R

                                            # RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
  


@sio.on("update_response_state_overallmanual")  # the rectangles' state if it is manual or automatic
async def handle_update_response_state_overallmanual(sid, data):

    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")
    if not app or not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return
    redis = app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
    
    org_id=None
    try:
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)

        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return

        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())



    except Exception as e:
        # Handle any unexpected errors
        print(f"An error occurred while processing the database operation: {e}")

    user_id = data.get('user_id')
    state = data.get('state')
    state = data.get('state')
    tabindex= data.get('tabindex')

    timestamp = data.get('frontend_time')  # ISO string
    

    asyncio.create_task(
        log_event(org_id, 'response_state_changed_overallmanual', {
            'user_id': user_id,
            'state': state,
            'tabindex':tabindex
        }, timestamp)
    )
    # Handle database updates based on the state
    try:
        if state:
            await redis.hset(f"user_mode_override:{org_id}", user_id, "manual")
        else:
            await redis.hdel(f"user_mode_override:{org_id}", user_id)


    except Exception as e:
        print(f"An error occurred while updating UserModeOverride for user {user_id} in org {org_id}: {e}")
    
    # Broadcast the new state to all connected clients
    #emit('response_state_update', {'user_id': user_id, 'state': state}, room=org_id, include_self=False)

    try:
        org_key = f"org:{org_id}:connections"
        sids_bytes = await redis.smembers(org_key)  # returns set of bytes

        # Decode to strings
        sids = [s.decode() for s in sids_bytes]

        other_sids = [s for s in sids if s != sid]
        if other_sids:
            await asyncio.gather(
                *(sio.emit("response_state_update_overallmanual", {"user_id": user_id, "state": state, "tabindex": tabindex, "timestamp": timestamp}, to=s)
                for s in other_sids),
                return_exceptions=True
            )
        
    except Exception as db_err:
        print(f"Error fetching SIDs for org_id {org_id}: {db_err}")

  
  


    #emit('response_state_update2', {'user_id': user_id, 'org_id': org_id, 'state': state}, to=chatbot_sid)
    # PUBLISH TO REDIS INSTEAD OF EMIT TO SID
    try:
        await redis.publish(
            "chatbot:user_state_update",
            json.dumps(
                {
                    "user_id": user_id,
                    "org_id": org_id,
                    "state": state,
                    "timestamp": timestamp,
                }
            ),
        )
    except Exception as redis_err:
        print(f"Redis publish error: {redis_err}")










                                            # RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
                                            
                                            #       RECTANGLE RESPONSE - AUTOMATIC MODE       R

                                            # RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR





@sio.on("admin_response")
async def handle_admin_response(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
    
    
    # Find the org associated with this socket ID
    try:
        connection_key = f"connection:{sid}"

        # Fetch all hash fields for this connection
        connection_data = await redis.hgetall(connection_key)

        # Check if connection exists
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return

        # Extract org_id and convert to integer
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())
    except Exception as e:
        print(f"Error fetching connection for socket_id {sid}: {e}")
        return


    
    user_id = data['user_id']
    response = data['response']
    timestamp=data['timestamp']

    message_for_log={
        "admin_response": response,
        "user_id": user_id,
        "userMessage" : "",
        "timestamp" : timestamp
    }

    asyncio.create_task(log_event(org_id, 'new_message', message_for_log))

    # Broadcast the response to all clients in the organization, excluding the sender
    #emit('response_update', {'user_id': user_id, 'response': response}, room=org_id, include_self=False)
    
    try:    # NOT SENDING TO ROOMS (because of REDIS) BUT TO SIDS DIRECTLY
        org_key = f"org:{org_id}:connections"
        sids_bytes = await redis.smembers(org_key)
        sids = [s.decode() for s in sids_bytes]
        other_sids = [s for s in sids if s != sid]
        if other_sids:
            await asyncio.gather(
                *(sio.emit("response_update", {"user_id": user_id, "response": response}, to=s)
                    for s in other_sids),
                return_exceptions=True
            )
    except Exception as db_err:
        print(f"Error fetching SIDs for org_id {org_id}: {db_err}")



#######  EDDIG KÉSZ !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   



# Handle input update event
@sio.on('update_colleagues_input')
async def handle_update_colleagues_input(sid, data):
    

    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
 
    org_id = None
    try:

        input_value = data.get('input_value')
        timestamp = data.get('timestamp')

        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())


 
    
    except Exception as e:
      # Handle any unexpected errors
      print(f"An error occurred while processing the database operation: {e}")

    # Log the event
    asyncio.create_task(log_event(org_id, 'colleagues_input_updated', {'input_value': input_value}, timestamp))

    # Broadcast the updated input to all clients in the organization except the sender
    #socketio.emit('colleagues_input_updated', input_value, room=org_id, include_self=False)
  
    # Broadcast to other SIDs in the same org (skip sender)
    try:
        org_key = f"org:{org_id}:connections"
        sids_bytes = await redis.smembers(org_key)
        sids = [s.decode() for s in sids_bytes]

        other_sids = [s for s in sids if s != sid]
        if other_sids:
            await asyncio.gather(
                *(sio.emit("colleagues_input_updated", input_value, to=s) for s in other_sids),
                return_exceptions=True
            )

    except Exception as db_err:
        print(f"Error fetching SIDs for org_id {org_id}: {db_err}")
        





# Handle one colleague addition
@sio.on("one_colleague_input")
async def handle_one_colleague_input(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

    socket_id = sid
    org_id = None
    try:
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())

    except Exception as e:
        print(f"Error handling one_colleague_input: {e}")
        return

    inputValueAddOne = data.get("inputValueAddOne")
    timestamp = data.get("timestamp")

    # Log event with timestamp
    asyncio.create_task(
        log_event(org_id, 'one_colleague_added', {'colleague_name': inputValueAddOne}, timestamp)
    )


    try:
        org_key = f"org:{org_id}:connections"
        sids_bytes = await redis.smembers(org_key)
        sids = [s.decode() for s in sids_bytes]
        other_sids = [s for s in sids if s != sid]
        if other_sids:
            await asyncio.gather(
                *(sio.emit("one_colleague_addition", inputValueAddOne, to=s) for s in other_sids),
                return_exceptions=True
            )

    except Exception as db_err:
        print(f"Error fetching SIDs for org_id {org_id}: {db_err}")

##################################################
#                     CHAT INTERNAL
##################################################


@sio.on("admin_internal_message")
async def handle_admin_internal_message(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

    user_id = data.get('user_id')
    message = data.get('message')
    timestamp=data.get('timestamp')
    print("--------------------")
    print(timestamp)

    if not user_id or not message:
        return

    try:

        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())

        async with async_session_scope() as session:
            

            # Fetch the name of the user (admin) sending the internal message
            user = await session.scalar(
                select(User).where(User.id == user_id, User.is_deleted == False)
            )
            if not user:
                print(f"User with ID {user_id} not found or deleted.")
                return

            sender_name = user.name or user.email  # fallback to email if name is missing
            frontend_time= timestamp
           
             # Log the event
            asyncio.create_task(
                log_event(
                    org_id,
                    "admin_internal_message",
                    {
                        "message": message,
                        "name": sender_name,
                        "sender_id": user_id,
                    },
                    frontend_time,
                )
            )

            # Get all other connections in the same org
            org_key = f"org:{org_id}:connections"
            sids_bytes = await redis.smembers(org_key)
            sids = [s.decode() for s in sids_bytes]
            other_sids = [s for s in sids if s != sid]


            
            if other_sids:
                await asyncio.gather(
                    *(
                        sio.emit(
                            "admin_internal_message",
                            {
                                "sender_id": user_id,
                                "name": sender_name,
                                "message": message,
                                "timestamp": timestamp,
                            },
                            to=target_sid,
                        )
                        for target_sid in other_sids
                    ),
                    return_exceptions=True,
                )

    except Exception as e:
        print(f"Error broadcasting admin message: {e}")



@sio.on("admin_internal_message_open")
async def handle_internal_message_open(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

    timestamp_ms = data.get("timestamp")  # JS time in ms

    if not timestamp_ms:
        return

    try:
        await redis.hset(
            f"connection:{sid}",
            mapping={"admin_internal_message_open": str(timestamp_ms)}
        )
        print(f"Updated admin_internal_message_open for sid {sid} to {timestamp_ms}")
    except Exception as e:
        print(f"Error updating admin_internal_message_open for sid {sid}: {e}")



@sio.on("admin_internal_message_close")
async def handle_internal_message_close(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
   
    timestamp_ms = data.get("timestamp")

    if not timestamp_ms:
        return

    try:
        # Save the close timestamp for this specific sid
        await redis.hset(
            f"connection:{sid}",
            mapping={"admin_internal_message_close": str(timestamp_ms)}
        )
        print(f"Updated admin_internal_message_close for sid {sid} to {timestamp_ms}")
    except Exception as e:
        print(f"Error updating admin_internal_message_close for sid {sid}: {e}")

# Handle one colleague removal
@sio.on("remove_onecolleague_input")
async def handle_remove_colleague_input(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

    # Find the org associated with this socket ID
    org_id = None

    try:
        # Get the org_id from the current connection
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())
    except Exception as e:
        print(f"Error handling one_colleague_input: {e}")
        return


    if not org_id:
        return  # Exit if org_id is not found in the session

    inputValueRemoveOne = data.get("colleague_name")
    timestamp = data.get("timestamp")
    # Log asynchronously (background task)
    asyncio.create_task(
        log_event(
            org_id,
            "one_colleague_removed",
            {"colleague_name": inputValueRemoveOne},
            timestamp,
        )
    )
    # Broadcast the updated input to all clients except the sender
    #socketio.emit('one_colleague_removal', inputValueRemoveOne , room=org_id, include_self=False)
    try:
        org_key = f"org:{org_id}:connections"
        sids_bytes = await redis.smembers(org_key)
        target_sids = [s.decode() for s in sids_bytes]

        tasks = [
            sio.emit(
                "one_colleague_removal",
                inputValueRemoveOne,
                to=target_sid,
            )
            for target_sid in target_sids
            if target_sid != sid
        ]

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as db_err:
        print(f"Error fetching SIDs for org_id {org_id}: {db_err}")



# Handle colleague addition with tabs
@sio.on("add_colleague")
async def handle_add_colleague(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
   
    
    # Find the org associated with this socket ID
    org_id = None
    try:
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())
    except Exception as e:
        print(f"Error handling add_colleague: {e}")
        return


    if not org_id:
        return  # Exit if org_id is not found in the session

    tabs = data.get('tabs', [])  # Get the list of tabs with name and uniqueId
    if tabs:
        # Log the event
        asyncio.create_task(
            log_event(org_id, 'colleague_added', {'tabs': tabs}, data.get('frontend_time'))
        )

        try:
            org_key = f"org:{org_id}:connections"
            sids_bytes = await redis.smembers(org_key)
            sids = [s.decode() for s in sids_bytes]

            other_sids = [s for s in sids if s != sid]
            if other_sids:
                await asyncio.gather(
                    *(sio.emit("colleague_added", {"tabs": tabs}, to=s) for s in other_sids),
                    return_exceptions=True
                )
        except Exception as db_err:
            print(f"Error fetching SIDs for org_id {org_id}: {db_err}")

# Handle colleague removal
@sio.on("remove_colleague")
async def handle_remove_colleague(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
    
    removed_colleague_name = data.get("colleagueName")
    timestamp = data.get("timestamp")
    org_id = None
    try:
        # Find the org associated with this socket ID
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())
    except Exception as e:
        print(f"Error handling remove_colleague: {e}")
        return

    if not org_id:
        return  # Exit if org_id is not found in the session
    
    # Log the event
    asyncio.create_task(
        log_event(org_id, 'colleague_removed', {'colleague_name': removed_colleague_name}, timestamp)
    )

    try:
        org_key = f"org:{org_id}:connections"
        sids_bytes = await redis.smembers(org_key)
        sids = [s.decode() for s in sids_bytes]

        other_sids = [s for s in sids if s != sid]
        if other_sids:
            results = await asyncio.gather(
                *(sio.emit(
                    "colleague_removed",
                    {"colleague_name": removed_colleague_name},
                    to=s
                ) for s in other_sids),
                return_exceptions=True
            )

            for idx, r in enumerate(results):
                if isinstance(r, Exception):
                    print(f"Emit error to SID {other_sids[idx]}: {r}")
    except Exception as db_err:
        print(f"Error fetching SIDs for org_id {org_id}: {db_err}")




@sio.on("admin_response_manual_for_logging")
async def handle_admin_response_manual_for_logging(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
    

    # Find the org associated with this socket ID
    org_id = None

    try:
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())
    except Exception as e:
        print(f"Error handling admin_response_manual_for_logging: {e}")
        return


    if not org_id:
        return  # Exit if org_id is not found in the session

    user_id = data.get('user_id')
    response = data.get('response')
    timestamp=data.get('timestamp')

    message_for_log={
        "admin_response": response,
        "user_id": user_id,
        "userMessage" : "",
        "timestamp" : timestamp
    }
    asyncio.create_task(log_event(org_id, "new_message", message_for_log))




    ################################             ################################

    ################################             ################################
    #   TABS CREATION      #########             #   TABS CREATION      #########
    ################################             ################################

    ################################             ################################






# Listen for tab creation events
@sio.on("createTabs")
async def handle_create_tabs(sid, data):

    
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client
    cpu_pool = app.state.cpu_pool
    cpu_sem = app.state.cpu_sem
  

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
    

    
    tabs = data.get('tabs', [])  # Get the list of tabs with name and uniqueId
    timestamp = data.get('frontend_time')


    
    # Find the org associated with this socket ID
    org = None
    user_id=None
    try:
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())
    except Exception as e:
        print(f"Error handling createTabs: {e}")
        return

    if not org:
        return  # Exit if org_id is not found in the session
        
   
    redis_key = f"messages:{org}:batch_temp"
    total_key = f"messages_total:{org}:batch_temp"

    # Debug Redis batch
    try:
        current_length = await redis.llen(redis_key)
        if current_length > 0:
            existing_messages = await redis.lrange(redis_key, 0, -1)
            print(f"Redis batch not empty for org {org}: {current_length} messages")
            for msg in existing_messages:
                print(json.loads(msg))
        else:
            print(f"Redis batch empty for org {org}")
    except Exception as e:
        print(f"Error checking Redis at start for org {org}: {e}")



    if tabs:
        # Log the event with the complete tab data
        asyncio.create_task(log_event(org, 'tabs_created', {'tabs': tabs}, timestamp))
   
        # Update Client.last_manualmode_triggered_by
        try:
            async with async_session_scope() as session:
                result = await session.execute(select(Client).where(Client.id == org))
                client = result.scalars().first()
                if client:
                    client.last_manualmode_triggered_by = user_id
        except Exception as e:
            print(f"Error updating client last_manualmode_triggered_by: {e}")
            return

        try:
            org_key = f"org:{org_id}:connections"
            sids_bytes = await redis.smembers(org_key)
            sids = [s.decode() for s in sids_bytes]

            tasks = [sio.emit("createTabs", {"tabs": tabs}, to=s) for s in sids if s != sid]
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for idx, r in enumerate(results):
                    if isinstance(r, Exception):
                        print(f"Emit error to SID {sids[idx]}: {r}")
        except Exception as db_err:
            print(f"Error fetching SIDs for org {org}: {db_err}")


        #if org_modes[org] == 'manual':
        if get_client_mode(org) == "manual":

            try:
                key = f"user_mode_override:{org_id}"

                # Set or update the user's mode in the hash
                await redis.hset(key, user_id, "manual")

                # Set TTL for the entire hash (e.g., 6 hours)
                await redis.expire(key, 3600 * 6)  # 3600 sec × 6 = 6 hours

            except Exception as e:
                print(f"Error updating user_mode_override with TTL for org {org_id}, user {user_id}: {e}")


            try:
                recent_messages = await get_recent_messages(org, minutes=15)
                
                def build_unique_messages_and_sorted(recent_messages):
                    unique_messages = {}
                    for event in recent_messages:
                        data = event["data"]
                        unique_key = tuple((key, str(data.get(key, None))) for key in sorted(data.keys()))
                        if unique_key not in unique_messages:
                            unique_messages[unique_key] = event

                    sorted_events = sorted(unique_messages.values(), key=lambda x: float(x["timestamp"]))
                    messages = []
                    total_length = len(sorted_events)
                    for event in sorted_events:
                        msg_with_ts = dict(event["data"])
                        msg_with_ts["timestamp"] = event["timestamp"]
                        msg_with_ts["total_messages"] = total_length
                        messages.append(msg_with_ts)
                    return messages

                messages = await run_cpu_task(build_unique_messages_and_sorted, recent_messages, cpu_pool=cpu_pool, cpu_sem=cpu_sem)
                
          

                org_connections_key = f"org:{org}:connections"

                # 1. Get all active socket IDs for this org
                sids_bytes = await redis.smembers(org_connections_key)
                sids = [s.decode() for s in sids_bytes]

                admin_sid = None

                # 2. Iterate through each connection to find the one for this user
                for sid in sids:
                    connection_key = f"connection:{sid}"
                    connection_data = await redis.hgetall(connection_key)
                    if not connection_data:
                        continue

                    # Compare user_id
                    user_id_bytes = connection_data.get(b"user_id")
                    if user_id_bytes and int(user_id_bytes.decode()) == int(user_id):
                        admin_sid = sid
                        break  # found the matching user, stop here

                if not admin_sid:
                    print(f"No active Redis connection found for user_id={user_id} in org={org}")



                if admin_sid:
                    # Clear previous Redis batch
                    try:
                        if await redis.llen(redis_key) > 0:
                            await redis.delete(redis_key, total_key)
                            print(f"Cleared Redis batch keys for org {org}")
                    except Exception as e:
                        print(f"Error clearing Redis keys for org {org}: {e}")

                 
                    await sio.emit("new_message_FirstUser", {"messages": messages}, to=admin_sid)
                else:
                    print(f"SID not found for admin user_id {user_id} in org {org}")

            except Exception as e:
                print(f"Error handling manual mode message batching for org {org}: {e}")


@sio.on("admin_response_manual")
async def handle_admin_response_manual(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client
    cpu_pool = app.state.cpu_pool
    cpu_sem = app.state.cpu_sem

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

  
    org_id=None
    try:
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())

    except Exception as e:
        print(f"Error handling admin_response_manual (DB lookup): {e}")
        return

    if not org_id:
        return  # Exit if org_id is not found in the session


    user_id = data.get('user_id')
    response = data.get('response')
    tab_index = data.get('tabIndex')
    asyncio.create_task(log_event(org_id, 'admin_response_manual_M', {
        'user_id': user_id, 
        'response': response, 
        'tabIndex': tab_index
    }))
    
     # Broadcast to other clients
    try:
        org_key = f"org:{org_id}:connections"
        sids_bytes = await redis.smembers(org_key)
        sids = [s.decode() for s in sids_bytes]

        other_sids = [s for s in sids if s != sid]
        if other_sids:
            results = await asyncio.gather(
                *(sio.emit(
                    "admin_response_broadcast_Manual",
                    {"user_id": user_id, "response": response, "tabIndex": tab_index},
                    to=s
                ) for s in other_sids),
                return_exceptions=True
            )
            for idx, r in enumerate(results):
                if isinstance(r, Exception):
                    print(f"Emit error to SID {other_sids[idx]}: {r}")
    except Exception as db_err:
        print(f"Error fetching SIDs for org_id {org_id}: {db_err}")



@sio.on("show_edit_tabs")
async def handle_show_edit_tabs(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

    org_id=None
    try:
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())
    except Exception as e:
        print(f"Error handling show_edit_tabs (DB lookup): {e}")
        return

    if not org_id:
        return  # Exit if org_id is not found in the session
    
    timestamp = data.get("timestamp")
    asyncio.create_task(log_event(org_id, 'show_edit_tabs', {}, timestamp))

    # Broadcast the event to all connected clients
    #emit('show_edit_tabs', room=org_id, include_self=False)

    try:
        org_key = f"org:{org_id}:connections"
        sids_bytes = await redis.smembers(org_key)
        sids = [s.decode() for s in sids_bytes]

        other_sids = [s for s in sids if s != sid]
        if other_sids:
            results = await asyncio.gather(
                *(sio.emit("show_edit_tabs", to=s) for s in other_sids),
                return_exceptions=True
            )
            for idx, r in enumerate(results):
                if isinstance(r, Exception):
                    print(f"Emit error to SID {other_sids[idx]}: {r}")

    except Exception as db_err:
        print(f"Error fetching SIDs for org_id {org_id}: {db_err}")


@sio.on("clear_input_field")
async def handle_clear_input_field(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client


    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

   
    org_id = None    
    try:
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())
    except Exception as e:
        print(f"Error handling clear_input_field (DB lookup): {e}")
        return


   
    
    timestamp = data.get('frontend_time') if data else None

    asyncio.create_task(log_event(org_id, 'clear_input_field', {}, timestamp))
    # Broadcast the event to all connected clients
    #emit('clear_input_field', room=org_id, include_self=False)

    try:
        org_key = f"org:{org_id}:connections"
        sids_bytes = await redis.smembers(org_key)
        sids = [s.decode() for s in sids_bytes]

        other_sids = [s for s in sids if s != sid]
        if other_sids:
            results = await asyncio.gather(
                *(sio.emit("clear_input_field", to=s) for s in other_sids),
                return_exceptions=True
            )
            for idx, r in enumerate(results):
                if isinstance(r, Exception):
                    print(f"Emit error to SID {other_sids[idx]}: {r}")
    except Exception as db_err:
        print(f"Error fetching SIDs for org_id {org_id}: {db_err}")
    

@sio.on("show_tabs_input")
async def handle_show_tabs_input(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client


    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

    
    
    org_id=None
    try:
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())
    except Exception as e:
        print(f"Error handling show_tabs_input (DB lookup): {e}")
        return
    if not org_id:
        return  # Exit if org_id is not found in the session
    
    timestamp = data.get('frontend_time') if data else None
    asyncio.create_task(log_event(org_id, 'show_tabs_input_', {}, timestamp))
    # Broadcast the event to all connected clients
    #emit('show_tabs_input', room=org_id, include_self=False)
    # Broadcast to other clients
    try:
        org_key = f"org:{org_id}:connections"
        sids_bytes = await redis.smembers(org_key)
        sids = [s.decode() for s in sids_bytes]

        other_sids = [s for s in sids if s != sid]
        if other_sids:
            results = await asyncio.gather(
                *(sio.emit("show_tabs_input", to=s) for s in other_sids),
                return_exceptions=True
            )
            for idx, r in enumerate(results):
                if isinstance(r, Exception):
                    print(f"Emit error to SID {other_sids[idx]}: {r}")
    
    except Exception as db_err:
        print(f"Error fetching SIDs for org_id {org_id}: {db_err}")


@sio.on("update_tab_name")
async def handle_update_tab_name(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client


    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

  
    
    org_id=None

    try:
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())
    except Exception as e:
        print(f"Error handling update_tab_name (DB lookup): {e}")
        return


    if not org_id:
        return  # Exit if org_id is not found in the session

    unique_id = data.get('uniqueId')
    new_name = data.get('newName')
    timestamp = data.get('frontend_time')

    if unique_id and new_name:
        # Log the event for debugging or record-keeping
        asyncio.create_task(
            log_event(org_id, 'tab_name_updated', {'uniqueId': unique_id, 'newName': new_name}, timestamp)
        )

        # Broadcast the updated tab name to all clients except the sender
        #emit('tab_name_updated', {'uniqueId': unique_id, 'newName': new_name}, room=org_id, include_self=False)
        try:
            org_key = f"org:{org_id}:connections"
            sids_bytes = await redis.smembers(org_key)
            sids = [s.decode() for s in sids_bytes]

            other_sids = [s for s in sids if s != sid]
            if other_sids:
                results = await asyncio.gather(
                    *(sio.emit("tab_name_updated", {"uniqueId": unique_id, "newName": new_name}, to=s) for s in other_sids),
                    return_exceptions=True
                )
                for idx, r in enumerate(results):
                    if isinstance(r, Exception):
                        print(f"Emit error to SID {other_sids[idx]}: {r}")
        except Exception as db_err:
            print(f"Error fetching SIDs for org_id {org_id}: {db_err}")

@sio.on("log_message_distribution")
async def handle_message_distribution(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client


    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
   
    
    org_id = None
    try:
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())

    except Exception as e:
        print(f"Error handling log_message_distribution for socket {sid}: {e}")
        return


    if not org_id:
        return  # Exit if org_id is not found in the session

    message = data.get('message')
    tab_uniqueId = data.get('tab_uniqueId')
    special_arg = data.get('specialArg')  # Extract the specialArg parameter if present
    timestamp=data.get('timestamp')

    # Prepare the log data
    log_data = {
        'message': message,
        'tab_uniqueId': tab_uniqueId,
    }
    
    if special_arg:  # Add specialArg only if it exists
        log_data['specialArg'] = special_arg

    # Log the event
    asyncio.create_task(log_event(org_id, "message_distribution", log_data, timestamp))


    # Prepare the emit data
    emit_data = {
        'message': message,  #{'timestamp': '', 'user_message': '', 'admin_response': 'asdfasdfa', 'user_id': '1'}
        'tab_uniqueId': tab_uniqueId,
    }
    if special_arg:  # Add specialArg only if it exists
        emit_data['specialArg'] = special_arg

    # Emit the message to other clients
    #socketio.emit('message_distribution', emit_data, room=org_id, include_self=False)
    
    try:
        org_key = f"org:{org_id}:connections"
        sids_bytes = await redis.smembers(org_key)
        sids = [s.decode() for s in sids_bytes]

        other_sids = [s for s in sids if s != sid]
        if other_sids:
            results = await asyncio.gather(
                *(sio.emit("message_distribution", emit_data, to=s) for s in other_sids),
                return_exceptions=True
            )
            for idx, r in enumerate(results):
                if isinstance(r, Exception):
                    print(f"Emit error to SID {other_sids[idx]}: {r}")

    except Exception as db_err:
        print(f"Error fetching SIDs for org_id {org_id}: {db_err}")
    







@sio.on("store_message_to_redis")  # Az összes többi usernek kiküldi a fő kivételével manualmódban
async def handle_store_message_to_redis(sid, data):
    
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client
    cpu_pool = app.state.cpu_pool
    cpu_sem = app.state.cpu_sem

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
    
    print(" *** BENT VAGYUNK A REDISRE MENTÉSEN ***!!!")
    print("DATA:\n", data)
    socket_id = sid
    org_id = None
    try:
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_bytes = connection_data.get(b"org_id")
        if not org_id_bytes:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_bytes.decode())
    except Exception as e:
        print(f"Error accessing DB for socket ID: {e}")
        return

    if not org_id:
        return

    emitData = data.get('emitData', {})
    message = emitData.get('message')
    tab_uniqueId = emitData.get('tab_uniqueId')
    special_arg = emitData.get('specialArg')

    # Use frontend timestamp if provided
    timestamp = data.get('timestamp') or message.get('timestamp') if message else None


    if not message or not tab_uniqueId:
        print("Missing required fields (message or tab_uniqueId)")
        return

    if 'timestamp' not in message or not message['timestamp']:
        message['timestamp'] = time.time()
    
    message['tab_uniqueId'] = tab_uniqueId

    # Include special_arg inside the message before saving to Redis so that it is available when reading later
    if special_arg:
        message['specialArg'] = special_arg

    
    redis_key = f'messages:{org_id}:batch_temp'
    #list is temporary and only used until a batch is ready to be emitted to clients.
    total_key = f'messages_total:{org_id}:batch_temp'
    #determine when the batch is complete: when LLEN(redis_key) >= total_messages, emit the batch to all clients.

    try:
        await redis.rpush(redis_key, json.dumps(message))

        total_messages = emitData.get('total_messages')
        if total_messages:
            total_messages = int(total_messages)
            await redis.set(total_key, total_messages, ex=300)
        else:
            total_messages = int(await redis.get(total_key) or 0)

        current_length = await redis.llen(redis_key)
        print("JELENLEGI MESSAGE FELTÖLTÉS: ", current_length)
        emit_lock_key = f"batch_emitted:{org_id}:batch_temp"
        lock_value = str(uuid.uuid4())  # 🔐 unique lock ID for this worker

        # Try to acquire the lock (nx=True ensures only one process gets it)
        lock_set = await redis.set(emit_lock_key, lock_value, nx=True, ex=10)

        if current_length >= total_messages > 0 and lock_set:
            try:
                # Set lock to avoid duplicate batch emissions (expires in 10 seconds)
                
                all_messages_raw = await redis.lrange(redis_key, 0, -1)
                def prepare_emit_batch(raw_messages):
                    all_messages = [json.loads(m) for m in raw_messages]
                    all_messages.sort(key=lambda m: m.get("timestamp", 0))

                    emit_batch = []
                    for msg in all_messages:
                        emit_data = {
                            "message": msg,
                            "tab_uniqueId": msg.get("tab_uniqueId"),
                        }
                        if "specialArg" in msg:
                            emit_data["specialArg"] = msg["specialArg"]
                        emit_batch.append(emit_data)
                    return emit_batch

                emit_batch = await run_cpu_task(prepare_emit_batch, all_messages_raw, cpu_pool=cpu_pool, cpu_sem=cpu_sem)

                # Fetch all SIDs for the org
                org_key = f"org:{org_id}:connections"
                sids_bytes = await redis.smembers(org_key)
                sids = [s.decode() for s in sids_bytes]


                other_sids = [s for s in sids if s != socket_id]
                if other_sids:
                    results = await asyncio.gather(
                        *(sio.emit("batch_ready_for_distribution", emit_batch, to=s) for s in other_sids),
                        return_exceptions=True
                    )
                    for idx, r in enumerate(results):
                        if isinstance(r, Exception):
                            print(f"Emit error to SID {other_sids[idx]}: {r}")

                try:
                    await redis.delete(redis_key)
                    await redis.delete(total_key)
                except Exception as del_err:
                    print(f"Error deleting Redis keys: {del_err}")
            finally:
                # Safe lock deletion You only delete the lock if you still own it. With the concrete value nut just adding simple 1 a basic (non-unique) lock — value = "1"
                try:
                    stored_value = await redis.get(emit_lock_key)
                    if stored_value and stored_value.decode() == lock_value:
                        await redis.delete(emit_lock_key)
                        print(f"[Lock] Released lock safely for org {org_id}")
                    else:
                        print(f"[Lock] Lock expired or taken by another worker, skipping delete")
                except Exception as lock_err:
                    print(f"[Lock] Error releasing lock for {org_id}: {lock_err}")


    except Exception as e:
        print(f"Error processing message to Redis: {e}")

    # Logging with specialArg included if present
    log_data = {
        'message': message,
        'tab_uniqueId': tab_uniqueId,
    }
    if special_arg:
        log_data['specialArg'] = special_arg

    try:
        await log_event(org_id, 'message_distribution', log_data, timestamp)
    except Exception as e:
        print(f"Error logging message_distribution: {e}")






@sio.on("tab_mode_changed")
async def handle_tab_mode_change(sid, data):
    """
    Handles when a user switches a tab between manual/automatic mode.
    Broadcasts the new state to other users in the same org.
    """
    cookies = sio.environ.get(sid, {}).get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]
                    break

    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client


    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return

    org_id = None
    user_id = None
    try:
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)

        if not connection_data:
            print(f"[Redis][tab_mode_change] No connection found for SID {sid}")
            return

        # Extract org_id and user_id safely
        org_id_bytes = connection_data.get(b"org_id")
        user_id_bytes = connection_data.get(b"user_id")

        if not org_id_bytes or not user_id_bytes:
            print(f"[Redis][tab_mode_change] Connection for SID {sid} is missing org_id or user_id")
            return

        org_id = int(org_id_bytes.decode())
        user_id = int(user_id_bytes.decode())
    except Exception as e:
        print(f"DB lookup error in handle_tab_mode_change: {e}")
        return

    if not org_id:
        print(f"[tab_mode_change] Missing org_id for SID {sid}")
        return

    # Extract info from event
    mode = data.get("mode")  # 'automatic' or 'manual'
    tab_id = data.get("tab_id")
    frontend_time = data.get("frontend_time")  # ISO UTC string

    if not (mode and tab_id):
        print(f"[tab_mode_change] Missing data: {data}")
        return

    # Log event
    asyncio.create_task(
        log_event(org_id, "tab_mode_changed", {"mode": mode, "tab_id": tab_id}, frontend_time)
    )

    # Update Redis shared state
    await redis.set(f"org:{org_id}:tab:{tab_id}:mode", mode, ex=3600*6) 

    # Broadcast change to all other connected clients in the same org
    try:
        org_key = f"org:{org_id}:connections"
        sids_bytes = await redis.smembers(org_key)
        sids = [s.decode() for s in sids_bytes]

        other_sids = [s for s in sids if s != sid]
        if other_sids:
            results = await asyncio.gather(
                *(
                    sio.emit(
                        "tab_mode_changed",
                        {"tab_id": tab_id, "mode": mode},
                        to=s,
                    )
                    for s in other_sids
                ),
                return_exceptions=True,
            )

            for idx, r in enumerate(results):
                if isinstance(r, Exception):
                    print(f"Emit error to SID {other_sids[idx]}: {r}")

    except Exception as err:
        print(f"Error broadcasting tab_mode_changed for org {org_id}: {err}")


@sio.on("resolve_pending_allocation")
async def handle_pending_allocation(sid, data):
    print("Received tab allocation:", data)
    environ = sio.get_environ(sid)
    app = environ.get("asgi.scope", {}).get("app")

    if not app:
        print("FastAPI app not available")
        return

    if not hasattr(app.state, "redis_client"):
        print("Redis not available")
        return

    redis = app.state.redis_client


    await redis.publish(
        "chatbot:pending_allocations",
        json.dumps({
            "type": "pending_allocation",
            "tab_mode": data["tab_mode"],
            "user_id": data["user_id"],
            "org_id": data["org_id"],
            "message": data["message"],
            "timestamp": data["timestamp"],
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "location": data.get("location"),
        })
    )

    # Store backup in Redis for failover
    await redis.setex(
        f"pending:{data['user_id']}:{data['temp_message_id']}",
        60,
        json.dumps({
            "user_id": data["user_id"],
            "org_id": data["org_id"],
            "message": data["message"],
            "tab_mode": data["tab_mode"],
            "timestamp": data["timestamp"],
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "location": data.get("location"),
        }),
    )

    print(f"Published pending_allocation for user {data['user_id']}")


