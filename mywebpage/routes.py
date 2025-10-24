#https://learn.microsoft.com/en-us/training/entra-external-identities/1-introduction
#https://www.facebook.com/JTCguitar
#https://www.facebook.com/guitarsalon

# FastAPI
from fastapi import FastAPI, APIRouter, Query, Depends, Request, Form, HTTPException, status
from fastapi.responses import RedirectResponse, JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi_login import LoginManager
from fastapi_login.exceptions import InvalidCredentialsException

# Imports from mywebpage
from mywebpage.datatransformation_v2 import datatransformation_for_chartjs
from mywebpage.datatransformation_v2_weekly import datatransformation_for_chartjs_weekly
from mywebpage.datatransformation_detaileduserdata import datatransformation_for_chartjs_detailed, build_coords_from_sources_async
from mywebpage import templates, fastapi_app,s, session_scope, redis_client 
from jwt.algorithms import RSAAlgorithm
from mywebpage.chats import fetch_topic_classification_counts
# Socket.IO (your ASGI version, not flask-socketio)
from mywebpage import sio  
from urllib.parse import parse_qs
# Database session
from mywebpage import async_session_scope, AsyncSessionLocal, Base
from sqlalchemy import text


# Mail (replace flask_mail → fastapi-mail)
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig, MessageType
from pydantic import BaseSettings, BaseModel


import aioredis
import os
import secrets
import json
import jwt
from sqlalchemy.future import select
import asyncio
from azure.monitor.opentelemetry import configure_azure_monitor
import logging
from opentelemetry.sdk._logs import LoggingHandler
import uuid
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature



import httpx
from fastapi import HTTPException
from sqlalchemy.orm import selectinload
from fastapi.responses import PlainTextResponse



from mywebpage.elephantsql import UserModeOverride, Client, Subscription, Role, User, Connections, ChatAppConnection, ChatHistory, AdminResponses, OrgEventLog, update_client_mode, get_chatapp_sid, enrich_event_with_local_timestamp
from mywebpage.chats import fetch_chat_messages
from datetime import datetime, timedelta 
from mywebpage.mainpulation_weeklyreport import user_querry_forquickreview, locationranking, longitude_latitude, longitude_latitude_detailed, fetch_chat_messages_weekly
import json
import os
from sqlalchemy import func, delete, update
from sqlalchemy.exc import SQLAlchemyError
from itsdangerous import SignatureExpired, BadSignature

import jwt
from fastapi_csrf_protect import CsrfProtect

import json
from authlib.integrations.flask_client import OAuth
from urllib.parse import urlencode
import secrets
import re
# from flask_socketio import SocketIO, emit, join_room
import time

from sqlalchemy.orm import joinedload
from sqlalchemy.exc import OperationalError  #catch db op error

from datetime import datetime, timezone

import pytz


from concurrent.futures import ProcessPoolExecutor
import torch
from transformers import AutoTokenizer



router = APIRouter()
s = URLSafeTimedSerializer("your-secret-key")  # same as in invite_user,  # creates signed and tamper-proof url for registration for new users


fastapi_app.include_router(router)

FLASH_EXPIRE_SECONDS = 60

# GMAIL

class MailSettings(BaseSettings):
    MAIL_USERNAME: str = "banaiviktor11@gmail.com"
    MAIL_PASSWORD: str = "fgbtaomxbnpjumck"
    MAIL_FROM: str = "banaiviktor11@gmail.com"
    MAIL_PORT: int = 587
    MAIL_SERVER: str = "smtp.gmail.com"
    MAIL_TLS: bool = True
    MAIL_SSL: bool = False

mail_settings = MailSettings()

conf = ConnectionConfig(
    MAIL_USERNAME = mail_settings.MAIL_USERNAME,
    MAIL_PASSWORD = mail_settings.MAIL_PASSWORD,
    MAIL_FROM = mail_settings.MAIL_FROM,
    MAIL_PORT = mail_settings.MAIL_PORT,
    MAIL_SERVER = mail_settings.MAIL_SERVER,
    MAIL_TLS = mail_settings.MAIL_TLS,
    MAIL_SSL = mail_settings.MAIL_SSL,
    USE_CREDENTIALS = True
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

    fm = FastMail(conf)
    await fm.send_message(message)
fast_mail = FastMail(conf)


async def run_cpu_task(func, *args):
    """Run a CPU-heavy function safely using the process pool and semaphore."""
    app = fastapi_app  # or pass the app explicitly if needed
    async with app.state.cpu_sem:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(app.state.cpu_pool, func, *args)


SECRET = os.environ.get("SECRET_KEY", secrets.token_urlsafe(32))

def setup_logging():
    # Configure Azure Monitor
    configure_azure_monitor()

    logger = logging.getLogger("myapp")  # root logger
    logger.setLevel(logging.INFO)

    # Console handler (for Log Stream)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    # OpenTelemetry logging handler (sends logs to App Insights)
    if not any(isinstance(h, LoggingHandler) for h in logger.handlers):
        logger.addHandler(LoggingHandler(level=logging.INFO))

    return logger


logger = setup_logging()

#---------------   OAuth STATE ---------------

STATE_KEY_PREFIX = "oauth_state:"
SESSION_KEY_PREFIX = "user_session:"
SESSION_TTL = 3600  # 1 hour


async def save_oauth_state(state: str, session_id: str):
    if fastapi_app.state.redis_client:
        # Store the state for 5 minutes
        await fastapi_app.state.redis_client.setex(f"{STATE_KEY_PREFIX}{session_id}", 300, state)

async def load_oauth_state(session_id: str) -> str | None:
    if fastapi_app.state.redis_client:
        return await fastapi_app.state.redis_client.get(f"{STATE_KEY_PREFIX}{session_id}")
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



class UserProxy:
    def __init__(self, id: int, email: str, role: str, client_id: int, name: str, is_active: bool = True):
        self.id = id
        self.email = email
        self.role = role
        self.client_id = client_id
        self.is_active = is_active
        self.name = name

    def is_authenticated(self) -> bool:
        return True

    def is_active_user(self) -> bool:
        return self.is_active

    def is_anonymous(self) -> bool:
        return False

    def get_id(self) -> str:
        return str(self.id)

    def to_dict(self) -> dict:
        """Store in Redis as JSON"""
        return {
            "id": self.id,
            "email": self.email,
            "role": self.role,
            "client_id": self.client_id,
            "name": self.name,
            "is_active": self.is_active
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)


async def get_current_user(request: Request) -> dict | None:
    session_id = request.cookies.get("session_id")
    redis = request.app.state.redis_client

    if not session_id or not redis:
        return None

    user_data = await redis.hgetall(f"session:{session_id}")
    if not user_data:
        return None

    return {
        "id": user_data.get("user_id"),
        "org_id": user_data.get("user_org"),
        "role": user_data.get("user_role"),
        "name": user_data.get("name"),
        "email": user_data.get("email"),
        "language": user_data.get("language") or "hu",
        "first_character": user_data.get("first_character"),
    }

async def login_required(user: dict = Depends(get_current_user)) -> dict:
    """Raises 401 if user is not logged in."""
    if not user:
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
                #raw_str = raw.decode() if isinstance(raw, bytes) else str(raw)
                flash_message = json.loads(raw)
            except Exception:
                flash_message = None
            await redis.delete(f"flash:{flash_id}")


    First_character = None
    subscription = None
    service_message = None
    chat_control_access = metrics_access = advanced_ai_access = False
    user_role = None

   
    
    if user:
        if not await redis.exists(f"session:{session_id}"):  #boolian false or true
            # Session expired → redirect to logout
            return RedirectResponse(url="/logout", status_code=302)
        
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

    # Fetch coordinates in memory-efficient way
    ip_data = await build_coords_from_sources_async(
        client_id=user["org_id"],
        start_dt=from_,
        end_dt=to_,
        table_name="chat_messages",
        redis=redis,
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
    session_id = request.cookies.get("session_id")

    user = await get_current_user(request)
    if not user:
        return templates.TemplateResponse("map.html", {"request": request, "ip_data": []})

    ip_data = await longitude_latitude(user["org_id"], redis)
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
        "chat_control": subscription.can_access_chat_control and user_role in ["Manager", "Team Leader", "Administrator"],
        "chatbot_metrics": subscription.can_access_chatbot_metrics and user_role in ["Manager", "Team Leader"],
        "advanced_ai": subscription.can_access_advanced_ai and user_role in ["Manager", "Team Leader"],
    }
    return permissions.get(feature, False)


@router.get("/serviceselector", response_class=HTMLResponse)
async def serviceselector_vbanai(
    request: Request,
    current_user: dict = Depends(login_required),  # ensures user is logged in
):
    redis = request.app.state.redis_client
    session_id = request.cookies.get("session_id")
    session_key = f"session:{session_id}"

    # If session expired in Redis → logout
    if not await redis.exists(session_key):
        return RedirectResponse(url="/logout", status_code=302)
    

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
            metrics_access = has_permission(user_role, subscription, "chatbot_metrics")
            advanced_ai_access = has_permission(user_role, subscription, "advanced_ai")
        else:
            # Default to no access if no subscription is found
            chat_control_access = False
            metrics_access = False
            advanced_ai_access = False
        
        # Additional session-related data
        return templates.TemplateResponse(
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
            "metrics_access": metrics_access,
            "advanced_ai_access": advanced_ai_access,
            "language": language,
            "user_id": user_id,
        },
    )

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
            return RedirectResponse(url="/logout", status_code=302)



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

    return JSONResponse(users_data)

ROLE_TRANSLATION_MAP = {
    "Menedzser": "Manager",
    "Csopotvezető": "Team Leader",
    "Adminisztrátor": "Administrator"
}

@router.api_route("/manager-dashboard", methods=["POST"], response_class=HTMLResponse)
async def manager_dashboard(
    request: Request,
    csrf_protect: CsrfProtect = Depends(), # Depends() is FastAPI’s way of saying: Before running this route, call some other function/class and inject its return value here.
    csrf_token: str = Form(...), # Form(...) REGUIRED, with no default means required. If the CSRF token is missing, FastAPI rejects the request before your route runs.
    #form_type: str | None = Form(None),   # OPTIONAL, form_type: str | None = Form(None),   # It looks inside the submitted form (request.form() under the hood). It tries to find a field called "form_type". If it exists, it converts it to a str and gives it to you in the parameter form_type. If it doesn’t exist, it uses the default (None in your case).
    current_user: dict = Depends(login_required),  # ensures user is logged in
):
  # Ensure that only users with the "Manager" role can access this page
  # if current_user.role != 'Team Leader' or current_user.role != 'Manager':
  #   flash("You do not have permission to access this page.", 'danger')
  #   return redirect(url_for('index'))
  csrf_protect.validate_csrf(csrf_token, request)
  
  form = await request.form()
  form_type = form.get("form_type")
  messages = []

  if form_type == "invite_user":
      try:
          # Collect all fields
          
          emails = form.get("emails")
          names = form.get("names", "")
          raw_role_name = form.get("role")
          role_name = ROLE_TRANSLATION_MAP.get(raw_role_name, raw_role_name)

          email_list = [e.strip() for e in emails.replace(',', '\n').splitlines() if e.strip()]
          name_list = [n.strip() for n in names.split(',') if n.strip()]

          if len(email_list) != len(name_list):
              messages.append({
                  'text_en': 'The number of names must match the number of emails.',
                  'text_hu': 'Ugyanannyi email címet kell beírnod, ahány nevet választottál!',
                  'category': 'danger'
              })
              return JSONResponse({'messages': messages, 'success': False})

          async with async_session_scope() as db_session:
              # Find role
              result = await db_session.execute(
                  select(Role).where(func.lower(Role.role_name) == role_name.lower())
              )
              role = result.scalar_one_or_none()
              if not role:
                  messages.append({
                      'text_en': 'Invalid role selected.',
                      'text_hu': 'Érvénytelen munkakört választottál.',
                      'category': 'danger'
                  })

              new_users = []
              for email, name in zip(email_list, name_list):
                  if not is_valid_email(email):
                      messages.append({
                          'text_en': f'The email {email} is not valid.',
                          'text_hu': f'Az email cím {email} nem érvényes.',
                          'category': 'danger'
                      })
                      continue

                  exists = await db_session.execute(
                      select(User).where(User.email == email)
                  )
                  if exists.scalar_one_or_none():
                      messages.append({
                          'text_en': f'The email {email} is already registered.',
                          'text_hu': f'Az email címet {email} már regisztráltad.',
                          'category': 'danger'
                      })
                      continue

                  new_user = User(
                      email=email,
                      name=name,
                      client_id=current_user["client_id"],
                      role_id=role.id,
                      is_active=False
                  )
                  new_users.append(new_user)

                  # Token + registration link
                  token = s.dumps({'email': email}, salt="email-confirm")
                  registration_link = request.url_for("register_confirm", token=token)

                  try:
                      await send_email(
                          subject="Account Registration Invitation",
                          recipients=[email],
                          body=f"Hello {name},\n\nPlease complete your registration: {registration_link}"
                      )
                      messages.append({
                          'text_en': f'User invited successfully: {email} ({name}).',
                          'text_hu': f'Sikeresen meghívtad: {email} ({name}).',
                          'category': 'success'
                      })
                  except Exception as e:
                      messages.append({
                        'text_en': f'An error occurred while sending the email to {email}: {str(e)}.',
                        'text_hu': f'Hiba történt az email küldése közben: {email}.',
                        'category': 'danger'
                    })

              if new_users:
                  try:
                      db_session.add_all(new_users)
                     
                  except SQLAlchemyError as e:
                      await db_session.rollback()
                      messages.append({
                          'text_en': f"An error occurred while saving users: {str(e)}",
                          'text_hu': "Hiba történt a mentés közben.",
                          'category': 'danger'
                      })

      except Exception as e:
          messages.append({
              'text_en': f'An error occurred: {str(e)}.',
              'text_hu': f'Hiba történt!: {str(e)}.',
              'category': 'danger'
          })

      success = any(m['category'] == 'success' for m in messages)
      return JSONResponse({'messages': messages, 'success': success})

   
  elif form_type == "manage_user":
      try:

        emails = form.get('emails')
        role_name = form.get('role')
        selected_user_email = form.get("selected_user")
        selected_role_name = form.get("selected_role")
        
        if not selected_user_email or not selected_role_name:
              messages.append({
                  'text_en': "Please select both a user and a role.",
                  'text_hu': "Felhasználót és pozíciót is választanod kell!",
                  'category': 'danger'
              })
              return JSONResponse({'messages': messages, 'success': False})

        async with async_session_scope() as db_session:
            result = await db_session.execute(
                select(Role).where(Role.role_name == selected_role_name)
            )
            role = result.scalar_one_or_none()
            if not role:
                messages.append({
                    'text_en': "Invalid role selected.",
                    'text_hu': "Nem megfelelő pozíciót választottál.",
                    'category': 'danger'
                })
                return JSONResponse({'messages': messages, 'success': False})

            result = await db_session.execute(
                select(User).where(User.email == selected_user_email)
            )
            user = result.scalar_one_or_none()
            if user:
                old_role = user.role.role_name if user.role else "No Role"
                user.role_id = role.id

                try:
                    await send_email(
                        subject="Role Change Notification",
                        recipients=[user.email],
                        body=f"Hello,\n\nYour role has been updated from '{old_role}' to '{selected_role_name}'."
                    )
                except Exception as e:
                    print(f"Error sending role-change email: {e}")

                messages.append({
                    'text_en': f"User {user.email} role updated to {selected_role_name}.",
                    'text_hu': f"A felhasználó {user.email} pozíciója frissítve lett: {selected_role_name}.",
                    'category': 'success'
                })
               
            else:
                messages.append({
                    'text_en': "User not found.",
                    'text_hu': "A felhasználó nincs benne az adatbázisba.",
                    'category': 'danger'
                })
      except Exception as e:
          messages.append({'text_en': f'Error: {str(e)}', 'text_hu': f'Hiba történt: {str(e)}', 'category': 'danger'})

      success = any(m['category'] == 'success' for m in messages)
      return JSONResponse({'messages': messages, 'success': success})


      
  elif form_type == "remove_user":  
      try:
         
          emails = form.get('emails')
          role_name = form.get('role')
          selected_user_email = form.get("selected_user")


          if not selected_user_email:
              messages.append({
                  'text_en': "Please select a user to remove.",
                  'text_hu': "Válaszd ki a munkatársat, akit törölni akarsz!",
                  'category': 'danger'
              })
              return JSONResponse({'messages': messages, 'success': False})

          async with async_session_scope() as db_session:
              result = await db_session.execute(
                  select(User).where(User.email == selected_user_email)
              )
              user = result.scalar_one_or_none()
              if user:
                  user.is_deleted = True
                  user.deleted_at = datetime.utcnow()
                 

                  try:
                      await send_email(
                          subject="Account Removal Notification",
                          recipients=[user.email],
                          body="Hello,\n\nYour account has been removed by the manager."
                      )
                  except Exception as e:
                      print(f"Error sending removal email: {e}")

                  messages.append({
                      'category': 'success',
                      'text_en': f"User {user.email} removed successfully.",
                      'text_hu': f"A felhasználó {user.email} törölve lett."
                  })
              else:
                  messages.append({
                      'category': 'danger',
                      'text_en': "User not found.",
                      'text_hu': "A felhasználó nem található."
                  })
      except Exception as e:
          messages.append({'text_en': f'Error: {str(e)}', 'text_hu': f'Hiba történt: {str(e)}', 'category': 'danger'})

      success = any(m['category'] == 'success' for m in messages)
      return JSONResponse({'messages': messages, 'success': success})







@router.get("/register/confirm")
async def register_confirm(request: Request, token: str):
    redis = request.app.state.redis_client  # aioredis client
    flash_id = str(uuid.uuid4())  # unique ID for this flash message
    flash_message = {"text": "", "category": ""}

    try:
        data = s.loads(token, salt="email-confirm", max_age=3600)
        email = data["email"]
    except SignatureExpired:
        flash_message = {"text": "The registration link has expired. Please request a new one.", "category": "danger"}
        await redis.set(f"flash:{flash_id}", json.dumps(flash_message), ex=60)  # expires in 60 seconds
        return RedirectResponse(url=f"/?flash_id={flash_id}")
    except BadSignature:
        flash_message = {"text": "The registration link is invalid.", "category": "danger"}
        await redis.set(f"flash:{flash_id}", json.dumps(flash_message), ex=60)
        return RedirectResponse(url=f"/?flash_id={flash_id}")

    # Activate user
    async with async_session_scope() as db_session:
        result = await db_session.execute(select(User).where(User.email == email))
        user = result.scalar_one_or_none()

        if not user:
            flash_message = {"text": "The user does not exist.", "category": "danger"}
        elif user.is_active:
            flash_message = {"text": "Account is already activated. You can log in now.", "category": "info"}
        else:
            user.is_active = True
            db_session.add(user)
        
            flash_message = {"text": "Your account has been activated. You can now log in.", "category": "success"}

    # Store flash message in Redis, flash_id is appended to the URL
    await redis.set(f"flash:{flash_id}", json.dumps(flash_message), ex=60)
    return RedirectResponse(url=f"/?flash_id={flash_id}")



#----------------------
#   PREDICTION MODULE
#----------------------

@router.get("/predictive_dashboard", response_class=HTMLResponse)
async def predictive_dashboard(user: dict = Depends(role_required("Manager"))):
    # Determine message based on user's language
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


#----------------------
#   WEEKLY SHORT
#----------------------

@router.get("/unauthorized", response_class=PlainTextResponse)
async def unauthorized():
    return "You are not authorized to view this page", 403

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_vbanai(request: Request, user: dict = Depends(role_required("Manager", "Team Leader"))):
    
    session_id = request.cookies.get("session_id")
    redis = request.app.state.redis_client
    if not await redis.exists(f"session:{session_id}"):  #boolian false or true
            # Session expired → redirect to logout
            return RedirectResponse(url="/logout", status_code=302)


    First_character = user.get("first_character")
    user_role = user.get("role") or "User"
    language = user.get("language", "hu")

    #usernumber_previousweek, usernumber, average_userquerry, today, previous_monday=await asyncio.to_thread(user_querry_forquickreview, user["org_id"], "chat_messages")
    #top3locations = await asyncio.to_thread(locationranking, user["org_id"])
    
    
    # Run CPU-bound helpers concurrently
    user_results, top3locations, mainChartData0 = await asyncio.gather(
        user_querry_forquickreview(user["org_id"]),
        locationranking(user["org_id"]),
        datatransformation_for_chartjs_weekly(user["org_id"]), return_exceptions=True  #Then handle exceptions individually. If one CPU task fails, asyncio.gather doesn't cancel all.
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
    manufacturer = await run_cpu_task(calculate_manufacturer, mainChartData0)
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
    if not await redis.exists(f"session:{session_id}"):  #boolian false or true
            # Session expired → redirect to logout
            return RedirectResponse(url="/logout", status_code=302)


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
    if not await redis.exists(f"session:{session_id}"):  #int 1 or 0
        # Session expired → redirect to logout
        return RedirectResponse(url="/logout", status_code=302)


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
    rows, columns = await fetch_chat_messages(start_utc, end_utc, user["org_id"], client_timezone, frequency, redis)
    
    
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
        return RedirectResponse(url="/logout", status_code=302)


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

def parse_int(form, key, default=0):
    try:
        return int(form.get(key, default))
    except (TypeError, ValueError):
        return default

@router.post("/topicMonitoring", response_class=HTMLResponse)
async def topic_monitoring(
    request: Request,
    user: dict = Depends(role_required("Manager", "Team Leader"))
):
    
    session_id = request.cookies.get("session_id")
    redis = request.app.state.redis_client
    if not await redis.exists(f"session:{session_id}"):  #boolian false or true
            # Session expired → redirect to logout
            return RedirectResponse(url="/logout", status_code=302)

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
                "language": user.get("language")
            }
        )
    except Exception as e:
        # Log the exception to the console
        print(f"An error occurred in the topicMonitoring route: {str(e)}")
        # Optionally, you can return an error response to the client
        return JSONResponse({'error': 'An internal server error occurred'}, status_code=500)


@router.get("/productbreakdown_topics", response_class=HTMLResponse)
async def topic_monitoring(
    request: Request,
    user: dict = Depends(role_required("Manager", "Team Leader"))
):
    
    session_id = request.cookies.get("session_id")
    redis = request.app.state.redis_client
    if not await redis.exists(f"session:{session_id}"):  #boolian false or true
            # Session expired → redirect to logout
            return RedirectResponse(url="/logout", status_code=302)

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
    redis = request.app.state.redis_client
    if not await redis.exists(f"session:{session_id}"):  #boolian false or true
            # Session expired → redirect to logout
            return RedirectResponse(url="/logout", status_code=302)


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
        final_transformed_data, data_for_final_transformation_copy, timestamp, start_end_date_byfrequency, usernumber, querry_on_average, changesinusernumber, locations = await datatransformation_for_chartjs_detailed(
            user["org_id"],
            start_utc.year, start_utc.month, start_utc.day, start_utc.hour, start_utc.minute, start_utc.second,
            end_utc.year, end_utc.month, end_utc.day, end_utc.hour, end_utc.minute, end_utc.second,
            frequency,
            'chat_messages', redis, topic)
        

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

        manufacturer_final = await run_cpu_task(process_manufacturer, data_for_final_transformation_copy)

      
        # Convert start/end dates asynchronously
        async def convert_dates(dates, tz):
            converted = []
            for start_str, end_str in dates:
                start_local = convert_utc_str_to_local(start_str, tz)
                end_local = convert_utc_str_to_local(end_str, tz)
                converted.append([start_local, end_local])
            return converted

        converted_start_end_date = await run_cpu_task(convert_dates, start_end_date_byfrequency, client_tz)

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


@fastapi_app.api_route("/login/external", methods=["GET", "POST"])
async def login_external(request: Request):
    
    # Fetch configuration values from environment variables
    tenant = os.environ.get("B2C_TENANT", "your_tenant_name")  # E.g., "redrainaib2ctenant"
    client_id = os.environ.get("B2C_CLIENT_ID", "your_client_id")
    policy = os.environ.get("B2C_POLICY", "your_policy_name")
    redirect_uri="https://redrain1230.loophole.site/auth"
    
    
   
    # Generate a unique state value and store it in the session
    state = secrets.token_urlsafe(16)  # Generate a secure random state
    request.session['oauth_state'] = state  # Later, when the user comes back from Microsoft, you compare the state returned with this saved state. Match → safe, Mismatch → possible attack.

    session_id = request.session.get("session_id")
    if not session_id:
        session_id = secrets.token_urlsafe(16)
        request.session['session_id'] = session_id  # storing session_id in the session dictionary created/managed by SessionMiddleware
    if fastapi_app.state.redis_client:  # redisre is mentjük multiworker setup miatt, mert A user starts login on Worker 1, state is generated and stored in request.session. Microsoft redirects back after login — the request might go to Worker 2.
        await fastapi_app.state.redis_client.setex(f"{STATE_KEY_PREFIX}{session_id}", 300, state)

    


   


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





#redirection happens here
@router.get("/auth")
async def auth(
    request: Request,
    state: str = Query(...),  #means this query parameter is required.
    code: str = Query(None)   # means this query parameter is optional (default None).
):
    #if I have redirect url: https://your-app.com/auth?state=abc123&code=xyz987 state = "abc123" and code = "xyz987"  by this Query  , None means default value is None but if we have code it will be the value

    redis = request.app.state.redis_client
    session_id = request.cookies.get("session_id")

    # Load expected state from Redis Verify state (CSRF protection)
    stored_state = await load_oauth_state(session_id)
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

    try:
        claims = jwt.decode(
            id_token,
            key=key,
            algorithms=["RS256"],
            audience=client_id,
            issuer=f"https://{tenant}.b2clogin.com/{tenant}.onmicrosoft.com/{policy}/v2.0/"
        )
    except jwt.PyJWTError as e:
        return JSONResponse({"error": f"Invalid ID token: {str(e)}"}, status_code=400)

    email = claims.get('emails')[0] if 'emails' in claims and claims['emails'] else None
    if not email:
        return JSONResponse({"error": "Email not found in ID token"}, status_code=400)


  

    # --- Lookup client ---
    client_id = await find_client_by_email(email)  # should be async if using async DB
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

        if not user:
            flash_id = str(uuid.uuid4())
            await redis.setex(f"flash:{flash_id}", 30, "Your email is not registered. Please contact your company.")
            return RedirectResponse(url=f"/?flash_id={flash_id}", status_code=302)

        if user.is_deleted or not user.is_active:
            flash_id = str(uuid.uuid4())
            await redis.setex(f"flash:{flash_id}", 30, "Your account is not active or deleted.")
            return RedirectResponse(url=f"/?flash_id={flash_id}", status_code=302)

        # Save user info in Redis


        await redis.hset(f"session:{session_id}", mapping={
            "user_id": str(user.id),
            "user_org": str(client_id),
            "language": user.language or "hu",
            "user_role": user.role.role_name if user.role else "Unknown",
            "first_character": email[0].upper(),
            "email": email,
            "name": user.name
        })
        await redis.expire(f"session:{session_id}", SESSION_TTL)
                
   
    try:
        
        # --- Notify others via async Socket.IO ---
        async with async_session_scope() as db_session:
            result = await db_session.execute(
                select(Connections.socket_id).where(Connections.org_id == client_id)
            )
            sids = result.scalars().all()

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
    response = RedirectResponse(url="/serviceselector_vbanai", status_code=302)
    response.set_cookie("session_id", session_id, httponly=True, max_age=SESSION_TTL)  # pass session_id to frontend
    return response


@router.api_route("/logout", methods=["GET", "POST"])
async def logout(request: Request):

    redis = request.app.state.redis_client
    session_id = request.cookies.get("session_id")

    tenant = os.environ.get("B2C_TENANT")
    policy = os.environ.get("B2C_POLICY")
    redirect_uri = str(request.url_for("index"))

    # Construct the B2C logout URL
    logout_url = f"https://{tenant}.b2clogin.com/{tenant}.onmicrosoft.com/{policy}/oauth2/v2.0/logout?post_logout_redirect_uri={redirect_uri}"

    response = RedirectResponse(url=logout_url, status_code=302)
    response.delete_cookie("session_id") 

    # Clear the local session
    if not session_id:
        print("[Logout] No session_id found in cookies. Redirecting anyway.")
        return response

    # --- Load session details from Redis ---
    # user_id = await redis.get(f"session:{session_id}:user_id")
    # org_id = await redis.get(f"session:{session_id}:user_org")

    user_id = await redis.hget(f"session:{session_id}", "user_id")
    org_id = await redis.hget(f"session:{session_id}", "user_org")

    if user_id:
        user_id = user_id if isinstance(user_id, bytes) else str(user_id)
    if org_id:
        org_id = org_id if isinstance(org_id, bytes) else str(org_id)

    print(f"[Logout] Before clearing session: user_id={user_id}, org_id={org_id}")

    # --- Clear Redis session keys ---
    await redis.delete(f"session:{session_id}")

    if user_id and org_id:
        redis_key = f"online:{org_id}:{user_id}"
        try:
            await redis.delete(redis_key)
            print(f"[Redis] Deleted key {redis_key}")

            # --- Socket.IO notification ---
            async with async_session_scope() as db_session:
                result = await db_session.execute(
                    select(Connections.socket_id).where(Connections.org_id == org_id)
                )
                sids = result.scalars().all()

            for sid in sids:
                try:
                    await sio.emit(
                        "user_online_status_changed",
                        {"user_id": user_id, "is_online": False},
                        to=sid,
                    )
                except Exception as emit_err:
                    print(f"Emit error to SID {sid}: {emit_err}")

            print(f"[Redis] Marked user {user_id} of org {org_id} as offline.")

        except Exception as e:
            print(f"[Redis Error] Failed to delete online status: {e}")

    # Remove connection details from the database
    if user_id:
          # --- DB cleanup ---
        async with async_session_scope() as db_session:
            connection = await db_session.scalar(
                select(Connections).where(Connections.user_id == user_id)
            )

            if connection:
                org_id = connection.org_id
                await db_session.delete(connection)
            
                print(f"Admin {user_id} from org {org_id} disconnected")

                # Check for remaining active connections
                active_connections = await db_session.scalar(
                    select(func.count()).select_from(Connections).where(Connections.org_id == org_id)
                )

                if active_connections == 0:
                    await db_session.execute(delete(OrgEventLog).where(OrgEventLog.org_id == org_id))
                    await db_session.execute(
                        update(Client).where(Client.id == org_id).values(mode="automatic")
                    )
                    await db_session.execute(
                        delete(UserModeOverride).where(UserModeOverride.client_id == org_id)
                    )

                    client_to_update = await db_session.scalar(
                        select(Client).where(
                            Client.id == org_id,
                            Client.is_active == True,
                            Client.last_manualmode_triggered_by.isnot(None),
                        )
                    )

                    if client_to_update:
                        client_to_update.last_manualmode_triggered_by = None
                        print(f"Updated client {client_to_update.id}: set last_manualmode_triggered_by to None.")

                  
                    print(f"Cleared entries and reset mode to 'automatic' for org {org_id}")

            else:
                print(f"No connection found for user {user_id}. No action taken.")


    # Add a flash message in Redis
    if redis:
        flash_id = str(uuid.uuid4())
        flash_message = {
            "text": "You have been logged out successfully.",
            "category": "success",
        }
        await redis.setex(f"flash:{flash_id}", FLASH_EXPIRE_SECONDS, json.dumps(flash_message))

        # Append flash_id to the redirect URL
       
        if "?" in response.headers["Location"]:
            response.headers["Location"] += f"&flash_id={flash_id}"
        else:
            response.headers["Location"] += f"?flash_id={flash_id}"

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














                                    #---------------------------
                                    #----    ADMIN PAGE     ----
                                    #---------------------------   
                                    
                                    
                                    #---------------------------
                                    #----    ADMIN PAGE     ----
                                    #---------------------------

import json
import time

from flask_session import Session
from datetime import datetime, timedelta



###########################################################################
#                            REDIS
###########################################################################





import threading
import time
import json
from datetime import datetime, timedelta


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



def classify_topic(input_text: str) -> str:
    tokenizer = fastapi_app.state.topic_tokenizer
    model = fastapi_app.state.topic_classifier_model

    inputs = tokenizer(
        input_text,
        truncation=True,
        padding=True,
        return_tensors="pt",
        max_length=128
    )

    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.softmax(outputs.logits, dim=-1)
        pred = torch.argmax(probs, dim=-1).item()

    # Map back to label string
    reverse_label_mapping = {0: "Termékérdeklődés",
                             1: "Vásárlási szándék",
                             2: "Ár és promóció",
                             3: "Panaszok és problémák",
                             4: "Szolgáltatás",
                             5: "Egyéb"}
    return reverse_label_mapping[pred]




async def classify_and_save(payload: dict, redis_client):
    """
    Wait for chatbot DB insert, then classify topic and update topic_classification field.
    """
    try:
        message = payload["message"]
        user_id = message["user_id"]
        org_id = message["org_id"]
        standalone_prediction = message["standalone_prediction"]
        user_message = message["user_message"]
        non_standalone_input = message["input_for_not_standalone_topic_classification"]
        bot_message = message["bot_message"]
        saved_flag_key = message.get("saved_flag_key")

        # --- Wait until chatbot side finishes DB insert ---
        if saved_flag_key:
            print(f"Waiting for Redis save flag: {saved_flag_key}")
            for _ in range(60):  # wait up to 30 seconds
                exists = await redis_client.exists(saved_flag_key)
                if exists:
                    print(f"Redis save flag detected: {saved_flag_key}")
                    break
                await asyncio.sleep(0.5)
            else:
                print(f"Timeout waiting for DB save flag for {saved_flag_key}")
                return
        else:
            print("No saved_flag_key provided in payload; proceeding anyway.")

        # --- Choose input for topic classification ---
        classification_input = (
            user_message if standalone_prediction == 0 else non_standalone_input
        )

        # --- Run model inference (non-blocking) ---
        try:
            topic = await run_cpu_task(classify_topic, classification_input)
            print(f"Predicted topic: {topic}")
        except Exception as e:
            print("Topic classification failed")
            topic = "unknown"

        # --- Update the database record ---
        async with async_session_scope() as session:
            try:
                stmt = (
                    update(ChatHistory)
                    .where(
                        ChatHistory.user_id == user_id,
                        ChatHistory.client_id == org_id,
                        ChatHistory.message == user_message,
                        ChatHistory.response == bot_message,
                    )
                    .values(topic_classification=topic)
                    .execution_options(synchronize_session="fetch")
                )
                result = await session.execute(stmt)
                await session.commit()

                if result.rowcount == 0:
                    print(
                        f"No chat record found for "
                        f"user_id={user_id}, org_id={org_id}, message={user_message[:50]}"
                    )
                else:
                    print(f"✅ Topic classification saved for {result.rowcount} record(s).")

            except SQLAlchemyError as e:
                await session.rollback()
                print(f"Database update failed: {e}")
                raise

    except Exception as e:
        print(f"classify_and_save() failed: {e}")







INACTIVITY_TIMEOUT_SECONDS = 30


async def redis_listener():
    redis_client = fastapi_app.state.redis_client

    while True:  # Outer loop -> reconnects after inactivity or errors
        try:
            pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
            await pubsub.subscribe("chatbot:messages")
            print("Chatbot subscribed to Redis channel 'chatbot:messages'")

            last_activity = datetime.utcnow()

            while True:  # Inner loop -> process messages
                now = datetime.utcnow()

                # inactivity check
                if (now - last_activity) > timedelta(seconds=INACTIVITY_TIMEOUT_SECONDS):
                    print(f"[WARNING] No message or heartbeat in {INACTIVITY_TIMEOUT_SECONDS} seconds.")
                    break  # break inner loop → reconnect in outer loop

                # Poll for message (non-blocking with timeout)
                message = await pubsub.get_message(timeout=1.0)

                if message is None:
                    await asyncio.sleep(0.1)  # small pause when idle
                    continue

                if message["type"] != "message":
                    continue

                try:
                    data = json.loads(message["data"])
                    last_activity = datetime.utcnow()  # reset inactivity timer

                    # Handle heartbeat
                    if data.get("type") in ("heartbeat", "admin_heartbeat"):
                        print(f"[Heartbeat] Received at {data.get('timestamp')}")
                        continue

                    print("MESSAGE FROM REDIS:", data)

                    org_id = str(data.get("message", {}).get("org_id"))
                    msg = data.get("message", {})

                    if "timestamp" in msg:
                        msg["timestamp"] = normalize_timestamp(msg["timestamp"])

                    event = await log_event(org_id, "new_message", msg)
                    mode = get_client_mode(org_id)

                    print("event!!!", event)

                    # === AUTOMATIC MODE ===
                    if mode == "automatic":


                         # -------- Emit message to all connected admins in the org --------

                        try:
                            async with async_session_scope() as session:
                                result = await session.execute(
                                    select(Connections.socket_id).where(Connections.org_id == org_id)
                                )
                                sids = [row[0] for row in result.fetchall()]
                                for sid in sids:
                                    try:
                                        await sio.emit(
                                            "new_message_FirstUser",
                                            {"messages": [event["data"]]},
                                            to=sid,
                                        )
                                    except Exception as emit_err:
                                        print(f"Emit error to SID {sid}: {emit_err}")
                        except OperationalError as db_err:
                            print(f"[DB ERROR - Automatic] OperationalError: {db_err}")
                        except Exception as db_err:
                            print(f"[DB ERROR - Automatic] General DB error: {db_err}")

                        # --- Run classification & saving in background ---
                        asyncio.create_task(
                        classify_and_save(data, redis_client)
)
                    
                    # === MANUAL MODE ===
                    elif mode == "manual":
                        try:
                            async with async_session_scope() as session:
                                result = await session.execute(
                                    select(Client.last_manualmode_triggered_by).where(
                                        Client.id == org_id, Client.is_active.is_(True)
                                    )
                                )
                                admin_user_id = result.scalar()

                                if admin_user_id:
                                    result = await session.execute(
                                        select(Connections.socket_id)
                                        .where(
                                            Connections.user_id == admin_user_id,
                                            Connections.org_id == org_id,
                                        )
                                        .order_by(Connections.socket_id.desc())
                                    )
                                    admin_sid = result.scalar()

                                    if admin_sid:
                                        await sio.emit(
                                            "new_message_FirstUser",
                                            {"messages": [event["data"]]},
                                            to=admin_sid,
                                        )
                                        print(f"Emitted to SID {admin_sid} for user {admin_user_id}")
                                    else:
                                        print(f"No active SID found for user_id {admin_user_id} in org {org_id}")
                                else:
                                    print(f"No admin user_id found for org_id {org_id}")
                        except OperationalError as e:
                            print(f"[DB ERROR - Manual] OperationalError: {e}")
                        except Exception as e:
                            print(f"[DB ERROR - Manual] General DB error: {e}")

                        # --- Run classification & saving in background ---
                        asyncio.create_task(
                        classify_and_save(data, redis_client)
                        )
                except Exception as e:
                    print(f"Error processing message from Redis: {e}")

        except Exception as outer_e:
            print(f"[Redis Listener] Connection lost, retrying in 5s... Error: {outer_e}")
            await asyncio.sleep(5)
        finally:
            try:
                if pubsub:
                    await pubsub.close()
            except:
                pass


async def send_admin_heartbeat():
    redis_client = fastapi_app.state.redis_client
    while True:
        try:
            heartbeat_data = {
                "type": "admin_heartbeat",
                "timestamp": datetime.utcnow().isoformat()
            }
            await redis_client.publish("chatbot:admin_heartbeat", json.dumps(heartbeat_data))
            print(f"[Admin Heartbeat] Sent at {heartbeat_data['timestamp']}")
            await asyncio.sleep(10)  # normal interval
        except (aioredis.exceptions.ConnectionError, asyncio.TimeoutError) as e:
            # transient failure → retry sooner
            print(f"[Admin Heartbeat Transient Error] {e}, retrying in 2s")
            await asyncio.sleep(2)
        except Exception as e:
            # permanent / unknown failure → log and wait longer
            print(f"[Admin Heartbeat Fatal Error] {e}, retrying in 30s")
            await asyncio.sleep(30)


redis_password=os.environ.get("redis_password")
redis_host = "aichatbotredis111.redis.cache.windows.net"
redis_port = 6380
redis_url = f"rediss://default:{redis_password}@{redis_host}:{redis_port}"




###########################################################################
#                 End of REDIS CONFIG
###########################################################################


@router.get("/chatAdminPager", response_class=HTMLResponse)
async def serviceselector_vbanai(
    request: Request,
    user: dict = Depends(login_required),  # ensures user is logged in
):
 
    redis = request.app.state.redis_client
    session_id = request.cookies.get("session_id")
    session_key = f"session:{session_id}"

    # If session expired in Redis → logout
    if not await redis.exists(session_key) or not await redis.hget(session_key, "user_id"):
        return RedirectResponse(url="/logout", status_code=302)
    

    user_id = user["id"]
    user_org = user["org_id"]
    language = user.get("language", "hu")
    first_character = user.get("first_character")

    
    return templates.TemplateResponse(
        "admin_dashboard.html",
        {
            "request": request,
            "user_org": user_org,
            "First_character": first_character,
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



async def should_show_internal_alert(user_id: str, org_id: str) -> bool:
    try:
        async with async_session_scope() as session:
            # Fetch user
            result = await session.execute(
                select(User).where(User.id == user_id)
            )
            user = result.scalar_one_or_none()
            if not user:
                return False

            open_time = user.admin_internal_message_open
            close_time = user.admin_internal_message_close

            # Get latest internal message event for org
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
        print(f"Error checking internal alert for user {user_id}: {e}")
        return False



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
async def connect(sid, environ):
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b'cookie':
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    redis = fastapi_app.state.redis_client
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
                remaining = await session.scalar(
                    select(func.count()).select_from(Connections)
                    .where(Connections.org_id == org_int, Connections.disconnected_at.is_(None))
                )

                if remaining == 0:
                    # Perform missing cleanup if necessary
                    print(f"[Connect] Performing missing cleanup for org {org_int}")
                    await session.execute(delete(OrgEventLog).where(OrgEventLog.org_id == org_int))
                    await session.execute(update(Client).where(Client.id == org_int).values(mode="automatic"))
                    await session.execute(delete(UserModeOverride).where(UserModeOverride.client_id == org_int))
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
            # Check for existing connection
            existing = await session.execute(
                select(Connections)
                .where(Connections.user_id == user_id_int)
                .where(Connections.org_id == org_int)
            )
            existing_conn = existing.scalar_one_or_none()

            if not existing_conn:
                connection = Connections(
                    socket_id=socket_id,
                    user_id=user_id_int,
                    org_id=org_int,
                    manualmode_triggered=False
                )
                session.add(connection)
                print(f"Connection saved: {connection}")
            else:
                # Update socket_id on reconnect
                existing_conn.socket_id = socket_id
                existing_conn.disconnected_at = None
                print(f"Reconnection: Updated socket_id for user {user_id_int} in org {org_int}")

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



@sio.on("history_ready")
async def handle_history_ready(sid, data):
    socket_id = data.get("socket_id")
    org = data.get("org")
    user_id = data.get("user_id")
    org=int(org)
    user_id=int(user_id)

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
                    await session.execute(
                        update(Connections)
                        .where(
                            Connections.org_id == org,
                            Connections.socket_id == socket_id,
                        )
                        .values(manualmode_triggered=(mode == "manual"))
                    )
                    print(f"Updated mode {mode} for org {org} socket {socket_id}")

                enriched_events.append(event)

        show_alert = await should_show_internal_alert(int(user_id), int(org))

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
    redis = fastapi_app.state.redis_client
    try:
        async with async_session_scope() as session:
            # Check if the disconnecting socket belongs to an administrator
            connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not connection:
                return

            if connection:
                org_id = connection.org_id
                user_id = connection.user_id
                user_id = int(user_id)
                org_id = int(org_id)
                connection.disconnected_at = datetime.utcnow()
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
                                async with async_session_scope() as delayed_session:
                                    conn = await delayed_session.scalar(
                                        select(Connections).where(Connections.socket_id == sid)
                                    )
                                    remaining = await delayed_session.scalar(
                                        select(func.count()).select_from(Connections)
                                        .where(Connections.org_id == org_id, Connections.disconnected_at.is_(None))
                                    )
                                    print(f"[Grace Period] Active admins for org {org_id}: {remaining}")

                                    if conn and conn.disconnected_at is None:
                                        return  # connection reconnected

                                    if remaining == 0:
                                        # No active connections → reset mode and clear entries
                                        await delayed_session.execute(
                                            delete(OrgEventLog).where(OrgEventLog.org_id == org_id)
                                        )
                                        await delayed_session.execute(
                                            update(Client).where(Client.id == org_id).values(mode="automatic")
                                        )
                                        await delayed_session.execute(
                                            delete(UserModeOverride).where(UserModeOverride.client_id == org_id)
                                        )
                                        await delayed_session.execute(
                                            update(User)
                                            .where(User.client_id == org_id, User.is_deleted.is_(False))
                                            .values(admin_internal_message_open=None)
                                        )

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
                                        if conn and conn.manualmode_triggered:
                                            next_admin = await delayed_session.scalar(
                                                select(Connections)
                                                .where(
                                                    Connections.org_id == org_id,
                                                    Connections.disconnected_at.is_(None),
                                                    Connections.socket_id != sid
                                                )
                                            )
                                            if next_admin:
                                                next_admin.manualmode_triggered = True
                                                print(f"[Grace Period] Propagated manualmode_triggered to user {next_admin.user_id}")
                                        if conn:
                                            await delayed_session.delete(conn)
                                            print(f"[Grace Period] Deleted disconnected user {user_id} from connections")
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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    


    mode = data.get("mode")
    frontend_time = data.get("frontend_time")


    
    # Find the org associated with this socket ID
    org_id = None

    # for org, sockets in org_to_user_map.items():
    #     if socket_id in sockets:
    #         org_id = org
    #         break
    try:
        async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if existing_connection:
                org_id=existing_connection.org_id
            else:
                org_id=None
                print(f"No connection found for socket_id {sid}")
                return
        #org_modes[org_id] = mode
        await update_client_mode(org_id, mode)
    
    except Exception as e:
      # Handle any unexpected errors
      print(f"An error occurred while processing the database operation: {e}")



    #emit('response_state_overall', {'org_id': org_id, 'state': mode}, to=chatbot_sid)
    await redis.publish("chatbot:state_updates", json.dumps({
            "org_id": org_id,
            "state": mode  # 'automatic' or 'manual'
        }))

    if mode == 'automatic':
        # Clear any stored admins or tab-related tracking for this org
        #orgs_triggered_create_tabs.pop(org_id, None)
        try:
            async with async_session_scope() as session:
                await session.execute(
                    delete(UserModeOverride).where(UserModeOverride.client_id == org_id)
                )
        except Exception as e:
            print(f"Error deleting UserModeOverride for org {org_id}: {e}")



            
                

    if mode in ['automatic', 'manual']:
        print(f"Emitting mode_changed event to org_id {org_id} with mode: {mode}")
        asyncio.create_task(
            log_event(org_id, 'mode_changed', {'mode': mode}, frontend_time)
        )
                
       
        #update_client_mode(org_id, mode)
        try:
          async with async_session_scope() as session:
                sids = (await session.execute(
                    select(Connections.socket_id).where(Connections.org_id == org_id)
                )).scalars().all()

                for other_sid in sids:
                    if other_sid == sid:
                        continue
                    try:
                        await sio.emit("mode_changed", {"mode": mode}, to=other_sid)
                    except Exception as emit_err:
                        print(f"Emit error to SID {other_sid}: {emit_err}")
        except Exception as db_err:
            print(f"Error fetching SIDs for org_id {org_id}: {db_err}")

        


    if mode =='automatic':
        # Get the current timestamp
        # current_time = time.time()
        # recent_messages = [
        #     event for event in org_event_logs.get(org, [])
        #     if event['event_type'] == 'new_message' and (current_time - event['timestamp'] <= 900)
        # ]
        # recent_messages = sorted(recent_messages, key=lambda event: event['timestamp'])
        recent_messages = await get_recent_messages(org_id, minutes=15, mode='automatic')


        

        final_data = await run_cpu_task(deduplicate_messages, recent_messages)
        
        for e in final_data:
            asyncio.create_task(
                log_event(org_id, e['event_type'], e['data'])
            )
        # *: “unpacking operator”. It’s used to expand an iterable (like a list, tuple, or generator) into separate positional arguments. PL: numbers = [1, 2, 3] print(*numbers) result 1 2 3
        try:
            async with async_session_scope() as session:
                sids = (await session.execute(
                    select(Connections.socket_id).where(Connections.org_id == org_id)
                )).scalars().all()
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
            
                if sids:
                    try:
                        await asyncio.gather(
                            *(sio.emit('new_message_FirstUser', {'messages': message_payload}, to=sid) 
                            for sid in sids),
                            return_exceptions=True  # ensures one failed emit won't cancel others
                        )
                    except Exception:
                        print(f"Emit error to some SID(s) for org_id {org_id}")
                else:
                    print(f"No active SIDs to emit for org_id {org_id}")

        except Exception as db_err:
            print(f"Error fetching SIDs for org_id {org_id}: {db_err}")



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

  redis = fastapi_app.state.redis_client

  # Validate session
  if not session_id or not await redis.exists(f"session:{session_id}"):
      await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
      await sio.disconnect(sid)
      return  # Stop further processing
  
  
  org_id=None
  try:
      async with async_session_scope() as session:
          existing_connection = await session.scalar(
              select(Connections).where(Connections.socket_id == sid)
          )
          if existing_connection is None:
              print(f"No connection found for socket_id {sid}")
              return
          org_id = existing_connection.org_id


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
  try:
    async with async_session_scope() as session:
        if state:
            override = UserModeOverride(user_id=user_id, client_id=org_id, mode="manual")
            session.add(override)
        else:
            # Remove user from user_mode_overrides for automatic mode
            await session.execute(
                delete(UserModeOverride).where(
                    (UserModeOverride.user_id == user_id)
                    & (UserModeOverride.client_id == org_id)
                )
            )

  except Exception as e:
    print(f"An error occurred while updating UserModeOverride for user {user_id} in org {org_id}: {e}")
  
  # Broadcast the new state to all connected clients
  #emit('response_state_update', {'user_id': user_id, 'state': state}, room=org_id, include_self=False)

  try:
      async with async_session_scope() as session:
          sids = (
              await session.execute(
                  select(Connections.socket_id).where(Connections.org_id == org_id)
              )
          ).scalars().all()

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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
    
    
    # Find the org associated with this socket ID
    try:
        async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not existing_connection:
                print(f"No connection found for socket_id {sid}")
                return
            org_id = existing_connection.org_id
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
        async with async_session_scope() as session:
            result = await session.execute(
                select(Connections.socket_id).where(Connections.org_id == org_id)
            )
            sids = result.scalars().all()
            other_sids = [s for s in sids if s != sid]
            if other_sids:
                await asyncio.gather(
                    *(sio.emit("response_update", {"user_id": user_id, "response": response}, to=s)
                      for s in other_sids),
                    return_exceptions=True
                )
    except Exception as db_err:
        print(f"Error fetching SIDs for org_id {org_id}: {db_err}")

        

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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
 
    org_id = None
    try:
      input_value = data.get('input_value')
      timestamp = data.get('timestamp')

      # Find org_id for this socket
      async with async_session_scope() as session:
          existing_connection = await session.scalar(
              select(Connections).where(Connections.socket_id == sid)
          )
          if not existing_connection:
              print(f"No connection found for socket_id {sid}")
              return
          org_id = existing_connection.org_id
 
    
    except Exception as e:
      # Handle any unexpected errors
      print(f"An error occurred while processing the database operation: {e}")

    # Log the event
    asyncio.create_task(log_event(org_id, 'colleagues_input_updated', {'input_value': input_value}, timestamp))

    # Broadcast the updated input to all clients in the organization except the sender
    #socketio.emit('colleagues_input_updated', input_value, room=org_id, include_self=False)
  
    # Broadcast to other SIDs in the same org (skip sender)
    try:
        async with async_session_scope() as session:
            result = await session.execute(
                select(Connections.socket_id).where(Connections.org_id == org_id)
            )
            sids = result.scalars().all()

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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

    socket_id = sid
    org_id = None
    try:
        async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not existing_connection:
                print(f"No existing connection found for socket ID: {sid}")
                return
            org_id = existing_connection.org_id
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
        async with async_session_scope() as session:
            result = await session.execute(
                select(Connections.socket_id).where(Connections.org_id == org_id)
            )
            sids = result.scalars().all()
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

    redis = fastapi_app.state.redis_client

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
        async with async_session_scope() as session:
            sender_conn = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not sender_conn:
                print(f"Sender not found for SID {sid}")
                return
            org_id = sender_conn.org_id

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
            other_sids = (
                await session.scalars(
                    select(Connections.socket_id)
                    .where(Connections.org_id == org_id)
                    .where(Connections.socket_id != sid)
                )
            ).all()

            
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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

    user_id = data.get("user_id")
    timestamp_ms = data.get("timestamp")  # JS time in ms

    if not user_id or not timestamp_ms:
        return

    try:
        open_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        async with async_session_scope() as session:
            result = await session.execute(
                select(User).where(User.id == user_id, User.is_deleted == False)
            )
            user = result.scalars().first()
            if user:
                user.admin_internal_message_open = open_time
                print(f"Updated admin_internal_message_open for user {user_id} to {open_time}")
    except Exception as e:
        print(f"Error updating admin_internal_message_open: {e}")


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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

    user_id = data.get("user_id")
    timestamp_ms = data.get("timestamp")

    if not user_id or not timestamp_ms:
        return

    try:
        close_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        async with async_session_scope() as session:
            user = await session.scalar(
                select(User).where(User.id == user_id, User.is_deleted == False)
            )
            if not user:
                print(f"User {user_id} not found or deleted.")
                return

            user.admin_internal_message_close = close_time
            print(f"Updated admin_internal_message_close for user {user_id} to {close_time}")

    except Exception as e:
        print(f"Error updating admin_internal_message_close: {e}")


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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

    # Find the org associated with this socket ID
    org_id = None

    try:
        # Get the org_id from the current connection
        async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not existing_connection:
                print(f"No existing connection found for socket ID: {sid}")
                return

            org_id = existing_connection.org_id
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
        async with async_session_scope() as session:
            target_sids = (
                await session.scalars(
                    select(Connections.socket_id).where(Connections.org_id == org_id)
                )
            ).all()

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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
   
    
    # Find the org associated with this socket ID
    org_id = None
    try:
      async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not existing_connection:
                print(f"No existing connection found for socket ID: {sid}")
                return
            org_id = existing_connection.org_id
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
            async with async_session_scope() as session:
                sids = (
                    await session.scalars(
                        select(Connections.socket_id).where(Connections.org_id == org_id)
                    )
                ).all()

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

    redis = fastapi_app.state.redis_client

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
        async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not existing_connection:
                print(f"No existing connection found for socket ID: {sid}")
                return
            org_id = existing_connection.org_id
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
        async with async_session_scope() as session:
            sids = (
                await session.scalars(
                    select(Connections.socket_id).where(Connections.org_id == org_id)
                )
            ).all()

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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
    

    # Find the org associated with this socket ID
    org_id = None

    try:
        # Find the org associated with this socket ID
        async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not existing_connection:
                print(f"No existing connection found for socket ID: {sid}")
                return
            org_id = existing_connection.org_id
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

    redis_client = fastapi_app.state.redis_client
  

    # Validate session
    if not session_id or not await redis_client.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
    

    
    tabs = data.get('tabs', [])  # Get the list of tabs with name and uniqueId
    timestamp = data.get('frontend_time')


    
    # Find the org associated with this socket ID
    org = None
    user_id=None
    try:
        async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not existing_connection:
                print(f"No existing connection found for socket ID: {sid}")
                return
            org = existing_connection.org_id
            user_id = existing_connection.user_id
    except Exception as e:
        print(f"Error handling createTabs: {e}")
        return

    if not org:
        return  # Exit if org_id is not found in the session
    
   
    redis_key = f"messages:{org}:batch_temp"
    total_key = f"messages_total:{org}:batch_temp"

    # Debug Redis batch
    try:
        current_length = await redis_client.llen(redis_key)
        if current_length > 0:
            existing_messages = await redis_client.lrange(redis_key, 0, -1)
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
            async with async_session_scope() as session:
                sids = (await session.scalars(
                    select(Connections.socket_id).where(Connections.org_id == org)
                )).all()

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

                messages = await run_cpu_task(build_unique_messages_and_sorted, recent_messages)
                
          

                async with async_session_scope() as session:
                    admin_sid = await session.scalar(
                        select(Connections.socket_id)
                        .where(Connections.user_id == user_id, Connections.org_id == org)
                        .order_by(Connections.socket_id.desc())
                    )


                if admin_sid:
                    # Clear previous Redis batch
                    try:
                        if await redis_client.llen(redis_key) > 0:
                            await redis_client.delete(redis_key, total_key)
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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

  
    org_id=None
    try:
        async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not existing_connection:
                print(f"No existing connection found for socket ID: {sid}")
                return
            org_id = existing_connection.org_id
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
        async with async_session_scope() as session:
            result = await session.execute(
                select(Connections.socket_id).where(Connections.org_id == org_id)
            )
            sids = result.scalars().all()

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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

    org_id=None
    try:
        async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not existing_connection:
                print(f"No existing connection found for socket ID: {sid}")
                return
            org_id = existing_connection.org_id
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
        async with async_session_scope() as session:
            sids = (
                await session.scalars(
                    select(Connections.socket_id).where(Connections.org_id == org_id)
                )
            ).all()

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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

   
    org_id = None    
    try:
        async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not existing_connection:
                print(f"No existing connection found for socket ID: {sid}")
                return
            org_id = existing_connection.org_id
    except Exception as e:
        print(f"Error handling clear_input_field (DB lookup): {e}")
        return


   
    
    timestamp = data.get('frontend_time') if data else None

    asyncio.create_task(log_event(org_id, 'clear_input_field', {}, timestamp))
    # Broadcast the event to all connected clients
    #emit('clear_input_field', room=org_id, include_self=False)

    try:
        async with async_session_scope() as session:
            sids = (
                await session.scalars(
                    select(Connections.socket_id).where(Connections.org_id == org_id)
                )
            ).all()

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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

    
    
    org_id=None
    try:
        async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not existing_connection:
                print(f"No existing connection found for socket ID: {sid}")
                return
            org_id = existing_connection.org_id
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
        async with async_session_scope() as session:
            sids = (
                await session.scalars(
                    select(Connections.socket_id).where(Connections.org_id == org_id)
                )
            ).all()

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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    

  
    
    org_id=None

    try:
        async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not existing_connection:
                print(f"No existing connection found for socket ID: {sid}")
                return
            org_id = existing_connection.org_id
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
            async with async_session_scope() as session:
                sids = (
                    await session.scalars(
                        select(Connections.socket_id).where(Connections.org_id == org_id)
                    )
                ).all()

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

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
   
    
    org_id = None
    try:
        async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not existing_connection:
                print(f"No existing connection found for socket ID: {sid}")
                return

            org_id = existing_connection.org_id

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
        async with async_session_scope() as session:
            sids = (
                await session.scalars(
                    select(Connections.socket_id).where(Connections.org_id == org_id)
                )
            ).all()

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

    redis_client = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis_client.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
    
    print(" *** BENT VAGYUNK A REDISRE MENTÉSEN ***!!!")
    print("DATA:\n", data)
    socket_id = sid
    org_id = None
    try:
        async with async_session_scope() as session:
            existing_connection = await session.scalar(
                select(Connections).where(Connections.socket_id == sid)
            )
            if not existing_connection:
                print(f"No existing connection found for socket ID: {socket_id}")
                return
            org_id = existing_connection.org_id
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
    total_key = f'messages_total:{org_id}:batch_temp'

    try:
        await redis_client.rpush(redis_key, json.dumps(message))

        total_messages = emitData.get('total_messages')
        if total_messages:
            total_messages = int(total_messages)
            await redis_client.set(total_key, total_messages, ex=300)
        else:
            total_messages = int(await redis_client.get(total_key) or 0)

        current_length = await redis_client.llen(redis_key)
        print("JELENLEGI MESSAGE FELTÖLTÉS: ", current_length)
        emit_lock_key = f"batch_emitted:{org_id}:batch_temp"
        lock_set = await redis_client.set(emit_lock_key, "1", nx=True, ex=10)
        if current_length >= total_messages > 0 and lock_set:
            # Set lock to avoid duplicate batch emissions (expires in 10 seconds)
            
            all_messages_raw = await redis_client.lrange(redis_key, 0, -1)
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

            emit_batch = await run_cpu_task(prepare_emit_batch, all_messages_raw)

            # Fetch all SIDs for the org
            async with async_session_scope() as session:
                result = await session.execute(
                    select(Connections.socket_id).where(Connections.org_id == org_id)
                )
                sids = result.scalars().all()

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
                await redis_client.delete(redis_key)
                await redis_client.delete(total_key)
            except Exception as del_err:
                print(f"Error deleting Redis keys: {del_err}")

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
