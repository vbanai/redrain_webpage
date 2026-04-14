#https://learn.microsoft.com/en-us/training/entra-external-identities/1-introduction
#https://www.facebook.com/JTCguitar
#https://www.facebook.com/guitarsalon
############
#REDIS KEYS:
############

#  state:{session_id} created  await redis.setex(f"{STATE_KEY_PREFIX}{session_id}", 300, state) TTL: 300 seconds  Deleted after successful /auth.
#  flash:{uuid} await redis.setex(f"flash:{flash_id}", 30, "Message") TTL: 30 seconds
#  session:{session_id}  expiry: await redis.expire(f"session:{session_id}", SESSION_TTL
#                     fields:
#                             user_id
#                             user_org
#                             language
#                             user_role
#                             first_character
#                             email
#                             name
#                             last_active

# online:{org_id}:{user_id}  created: await redis.setex(online_key, SESSION_TTL, 1) Quick lookup of who is online in an org.
# connection:{socket_id}  TTL: 6 hours
#                     fields:
#                             user_id
#                             user_org
#                             manualmode_triggered
#                             disconnected_at
#                             admin_internal_message_open
#                             admin_internal_message_close

# org:{org_id}:connections  value: { socket_id_1, socket_id_2, ... }
# user_mode_override:{org_id}  TTL 6 óra  ITT EZ A chatbot userekre vonatkozik, hogy kinál van manual vagy automatic beállítva
# org:{org_id}:tab:{tab_id}:mode  TTL 6 óra

# messages:{org}:batch_temp
# messages_total:{org}:batch_temp
# tenant:{org_id}:user:{uid}:recent_msgs in function: _update_redis_batch  we use this for chat history for recurrent users

# client:{org_id}:state   fields: mode, last_manualmode_triggered_by
# client:42:state
# mode = manual
# last_manualmode_triggered_by = viktor
#we have two state global manual or global automatic, and in case of each global state we can set manual or automatic for each user, of course if global is automatic, we won't have key "client:{client_id}:state, but we can set individual user override keys as manual. If it is global manual mode we will have key client:{client_id}:state, initially we won't have user override keys, but we can set user_override _ key as automatic for users



#############
#LOGOUT LOGIC
#############

# we have two different hearbeat for pages like serviceselector where no socketio here we have http route and we check idle
#    second is constatly emit signal from page with socketio to be refreshed and not interrupt the admin work with logouts

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
from mywebpage.elephantsql import Client, Subscription, SubscriptionPrice, ClientBehaviorHistory, ClientConfigHistory, Role, User, OrgEventLog, update_client_mode, enrich_event_with_local_timestamp, get_client_code_by_client_id
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



BLOB_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME_PHOTOS")

blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)



stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")
webhook_secret = os.getenv("STRIPE_WEBHOOK_SECRET")
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
SESSION_TTL = 60 * 60   # 24 hour
SESSION_TTL_COOKIE=60 * 60


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







##########   ##########   ##########   ##########   ##########   ##########
# ROUTES #   # ROUTES #   # ROUTES #   # ROUTES #   # ROUTES #   # ROUTES #
##########   ##########   ##########   ##########   ##########   ##########  

##########   ##########   ##########   ##########   ##########   ##########
# ROUTES #   # ROUTES #   # ROUTES #   # ROUTES #   # ROUTES #   # ROUTES #
##########   ##########   ##########   ##########   ##########   ##########  

##########   ##########   ##########   ##########   ##########   ##########
# ROUTES #   # ROUTES #   # ROUTES #   # ROUTES #   # ROUTES #   # ROUTES #
##########   ##########   ##########   ##########   ##########   ##########  

MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB
ALLOWED_MIME_TYPES = {
    # Images
    "image/jpeg", "image/png", "image/gif",
    # PDF
    "application/pdf",
    # Word
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    # Excel
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}
ALLOWED_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif",
    ".pdf",
    ".doc", ".docx",
    ".xls", ".xlsx",
}

  

@router.post("/api/upload_file")
async def upload_file(
    file: UploadFile = File(...),
    org_id: str = Form(...),
    user_id: str = Form(...)
):
    """
    Uploads a file to Azure Blob Storage under:
    fileuploads/org_id/user_id/unique_filename
    """
    try:
        # Validate type
        if file.content_type not in ALLOWED_MIME_TYPES:
            raise HTTPException(status_code=400, detail="invalid_format")

        # Validate extension
        extension = os.path.splitext(file.filename)[1].lower()
        if extension not in ALLOWED_EXTENSIONS:
            raise HTTPException(status_code=400, detail="invalid_format")

        # Validate size
        file_bytes = await file.read()
        if len(file_bytes) > MAX_FILE_SIZE:
            raise HTTPException(status_code=400, detail="file_too_large")
        
        client_code = await get_client_code_by_client_id(int(org_id))
        # Create unique blob name
        blob_name = f"fileuploads/{client_code}/{user_id}/{uuid.uuid4()}{extension}"

        # Upload to Azure
        blob_client = container_client.get_blob_client(blob_name)
        await blob_client.upload_blob(
            file_bytes,
            overwrite=True,
            content_type=file.content_type
        )

        blob_url = blob_client.url

        return {
            "file_url": blob_url,
            "org_id": org_id,
            "user_id": user_id,
            "file_name": file.filename,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@router.api_route("/debug/delete-redis-keys", methods=["GET", "POST"])
async def delete_test_redis_keys(request: Request):
    redis = request.app.state.redis_client
    keys_to_delete = [
        "connection:qee7sM6AwBI1LoF-AAA5",
        "org:2:connections"

    ]

    deleted_keys = []
    for key in keys_to_delete:
        if await redis.exists(key):
            await redis.delete(key)
            deleted_keys.append(key)

    return JSONResponse({
        "message": f"Deleted {len(deleted_keys)} keys",
        "deleted_keys": deleted_keys
    })

@router.get("/debug/redis-keys")
async def debug_redis_keys(request: Request):
    redis = request.app.state.redis_client
    keys = []
    async for key in redis.scan_iter("*"):
        keys.append(key)
    return {"keys": keys}



@router.get("/debug/org-connections/{org_id}")
async def debug_org_connections(org_id: int, request: Request):
    redis = request.app.state.redis_client

    key = f"org:{org_id}:connections"

    # Get all socket IDs stored in the set
    socket_ids = await redis.smembers(key)

    # Redis may return bytes → convert to string
    socket_ids = [sid.decode() if isinstance(sid, bytes) else sid for sid in socket_ids]

    return {
        "redis_key": key,
        "connections_count": len(socket_ids),
        "socket_ids": socket_ids
    }


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
                flash_message = json.loads(raw)
            except Exception:
                flash_message = None

            await redis.delete(f"flash:{flash_id}")


    First_character = None
    subscription = None
    service_message = None
    chat_control_access = metrics_access = advanced_ai_access = False
    user_role = None

   
    
    
    if session_id and not await redis.exists(f"session:{session_id}"):
        response = RedirectResponse(url="/", status_code=302)
        response.delete_cookie("session_id")
        return response
    
    if user:    
        First_character = user.get("first_character")
        user_role = user.get("role")
        language = user.get("language", "hu")

        async with async_session_scope() as db_session:
            # Fetch client + subscription
            client = await db_session.scalar(
                select(Client).options(joinedload(Client.subscription)).where(Client.id == int(user["org_id"]))
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
        client_id=int(user["org_id"]),
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

    ip_data = await longitude_latitude(int(user["org_id"]), redis, cpu_pool=cpu_pool, cpu_sem=cpu_sem,)
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
    user_org = int(current_user["org_id"])
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
    client_id = int(user.get("org_id"))
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
            is_online = bool(await redis.exists(redis_key))
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
                          f"https://http://localhost:8001/register/confirm?token={token}"
                        #  f"https://redrain1230_viktor.loophole.site/register/confirm?token={token}"
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
                      client_id=int(current_user["org_id"]),
                      role_id=role.id,
                      is_active=False
                  )
                  new_users.append(new_user)
                  print(f"[DEBUG] New user object created: {new_user}")

                  # Token + registration link
                  token = s.dumps({'email': email, 'lang': lang}, salt="email-confirm")
                  registration_link = f"https://http://localhost:8001/register/confirm?token={token}"
                  #registration_link = f"https://redrain1230_viktor.loophole.site/register/confirm?token={token}"
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
    user_org = int(user["org_id"])
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
    user_org = int(user["org_id"])
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
    user_org = int(current_user["org_id"])
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
    user_org = int(current_user["org_id"])
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
    user_org = int(current_user["org_id"])
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
    user_org = int(current_user["org_id"])
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
        user_querry_forquickreview(int(user["org_id"]), redis, cpu_pool=cpu_pool, cpu_sem=cpu_sem),
        locationranking(int(user["org_id"]), redis, cpu_pool=cpu_pool, cpu_sem=cpu_sem),
        datatransformation_for_chartjs_weekly(int(user["org_id"]), cpu_pool=cpu_pool, cpu_sem=cpu_sem), return_exceptions=True  #Then handle exceptions individually. If one CPU task fails, asyncio.gather doesn't cancel all.
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
              .where(Client.id == int(user["org_id"]))  # user from Depends
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
            select(Client).where(Client.id == int(user["org_id"]))
        )
        client = result.scalar_one_or_none()
        client_timezone = client.timezone if client and client.timezone else "UTC"

    # Run blocking function in thread
    rows, columns = await fetch_chat_messages_weekly(
        start_date,
        end_date,
        int(user["org_id"]),
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
            select(Client).where(Client.id == int(user["org_id"]))
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
    rows, columns = await fetch_chat_messages(start_utc, end_utc, int(user["org_id"]), client_timezone, frequency, redis, cpu_pool=cpu_pool, cpu_sem=cpu_sem)
    
    
    # Cached topic counts
    topic_key = f"topic_counts:{int(user['org_id'])}:{start_utc.isoformat()}:{end_utc.isoformat()}"

    cached = await redis.get(topic_key)
    if cached:
        topic_counts = json.loads(cached)
    else:
        topic_counts = await fetch_topic_classification_counts(start_utc, end_utc, int(user["org_id"]))
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
            select(Client).where(Client.id == int(user["org_id"]))
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
    rows, columns = await fetch_chat_messages(start_utc, end_utc, int(user["org_id"]), client_timezone, frequency, redis, topic)
    
    # Cached topic counts
    topic_key = f"topic_counts:{int(user['org_id'])}:{start_utc.isoformat()}:{end_utc.isoformat()}"

    cached = await redis.get(topic_key)
    if cached:
        topic_counts = json.loads(cached)
    else:
        topic_counts = await fetch_topic_classification_counts(start_utc, end_utc, int(user["org_id"]))
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
                    .where(Client.id == int(user["org_id"]))
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
            int(user["org_id"]),
            start_utc.year, start_utc.month, start_utc.day, start_utc.hour, start_utc.minute, start_utc.second,
            end_utc.year, end_utc.month, end_utc.day, end_utc.hour, end_utc.minute, end_utc.second,
            frequency,
            "chat_messages", redis
        )
        
        topic = "topic_all"
        redis_key = (
            f"deepinsight:{int(user['org_id'])}:" 
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
                    .where(Client.id == int(user["org_id"]))
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

        redis_key = f"deepinsight:{int(user['org_id'])}:{topic}:{start_ts}:{end_ts}"

        cached = await redis.get(redis_key)
        if cached:
            data = json.loads(cached)
        else:
            # only compute if cache missing (should not happen)
            data = await datatransformation_for_chartjs(
                int(user["org_id"]),
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
        topic_key = f"topic_counts:{int(user['org_id'])}:{start_utc.isoformat()}:{end_utc.isoformat()}"

        cached = await redis.get(topic_key)
        if cached:
            topic_counts = json.loads(cached)
        else:
            topic_counts = await fetch_topic_classification_counts(start_utc, end_utc, int(user["org_id"]))
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
                    .where(Client.id == int(user["org_id"]))
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
            int(user["org_id"]),
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
                .where(Client.id == int(user["org_id"]))
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
            int(user["org_id"]),
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
    redirect_uri = "https://redrain1230_viktor.loophole.site/auth"
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
#The middleware will serialize request.session into a cookie in the HTTP response automatically, even if your response is a redirect.
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

    """
    
    # Fetch configuration values from environment variables
    tenant = os.environ.get("B2C_TENANT", "your_tenant_name")  # E.g., "redrainaib2ctenant"
    client_id = os.environ.get("B2C_CLIENT_ID", "your_client_id")
    policy = os.environ.get("B2C_POLICY", "your_policy_name")
    redirect_uri="https://redrain1230_viktor.loophole.site/auth"
    
    
   
    # Generate a unique state value and store it in the session
    state = secrets.token_urlsafe(16)  # Generate a secure random state
    
      # Later, when the user comes back from Microsoft, you compare the state returned with this saved state. Match → safe, Mismatch → possible attack.
    

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
    else:
        print("Redis unavailable")
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

    """
    return await fake_login(request)


async def fake_login(request: Request):
    """
    Fake login for dev: simulate OAuth + Redis session + cookies
    """

 
    email = request.query_params.get("email") 

    # 2️⃣ Session middleware
    session_id = request.session.get("session_id")
    if not session_id:
        session_id = secrets.token_urlsafe(16)
        request.session["session_id"] = session_id

    redis = request.app.state.redis_client

    # 3️⃣ Find client
    client_id = await find_client_by_email(email)
    if not client_id:
        return JSONResponse({"error": f"Client not found for {email}"}, status_code=400)

    # 4️⃣ Find user in DB
    async with async_session_scope() as db_session:
        result = await db_session.execute(
            select(User).options(joinedload(User.role)).where(User.email == email)
        )
        user = result.scalar_one_or_none()

        if not user:
            return JSONResponse({"error": "User not found"}, status_code=400)

        if user.is_deleted or not user.is_active:
            return JSONResponse({"error": "User inactive"}, status_code=400)

        lang = user.language or "hu"

        # 5️⃣ Save session in Redis (same as real flow)
        now = int(time.time())
        await redis.hset(f"session:{session_id}", mapping={
            "user_id": str(user.id),
            "user_org": str(client_id),
            "language": lang,
            "user_role": user.role.role_name if user.role else "Unknown",
            "first_character": email[0].upper(),
            "email": email,
            "name": user.name,
            "last_active": now
        })
        await redis.expire(f"session:{session_id}", SESSION_TTL)
        await redis.setex(f"online:{client_id}:{user.id}", SESSION_TTL, 1)

    # 6️⃣ Notify via Socket.IO
    try:
        sids = await redis.smembers(f"org:{client_id}:connections")
        for sid in sids:
            await sio.emit(
                'user_online_status_changed',
                {'user_id': user.id, 'is_online': True},
                to=sid
            )
    except Exception as e:
        print("Socket error:", e)

    # 7️⃣ Return redirect + cookies
    response = RedirectResponse(url="/serviceselector", status_code=302)
    secure = request.url.scheme == "https"

    response.set_cookie(
        "session_id",
        session_id,
        httponly=True,
        secure=secure,
        samesite="Lax",
        max_age=SESSION_TTL_COOKIE
    )
    response.set_cookie(
        "lang",
        lang,
        httponly=False,
        secure=secure,
        samesite="Lax",
        max_age=SESSION_TTL_COOKIE
    )

    return response

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
        await redis.setex(online_key, SESSION_TTL, 1) # Set the key to some value (e.g., 1) and give it a TTL
                                
   
    try:
        
        # --- Notify others via async Socket.IO SIDEK ---
        org_key = f"org:{client_id}:connections"
        sids= await redis.smembers(org_key)  
        
   

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


#  we update: last_active
#             expire(session)


@router.post("/heartbeat")   # heartbeat from normal http pages no websocket like service selector
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
    #  ackground job checks: if now - last_active > IDLE_TIMEOUT:
    #  So this line tells your system: “User was active JUST NOW”

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
    await redis.expire(session_key, SESSION_TTL)  #resets expiration timer
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
async def logout(request: Request, reason: str = Query("manual")):
    """
    Fake logout for dev: clears session + cookies + socket notifications without hitting Azure.
    """
    
    print("STARTING FAKE LOGOUT!!!")
    redis = request.app.state.redis_client
    session_id = request.cookies.get("session_id")

    if not session_id:
        return RedirectResponse(url="/", status_code=302)

    # Load session info
    user_id = await redis.hget(f"session:{session_id}", "user_id")
    org_id = await redis.hget(f"session:{session_id}", "user_org")


    # Remove session from Redis
    await redis.delete(f"session:{session_id}")

    # Add flash message
    lang = request.cookies.get("lang", "hu")
    flash_id = str(uuid.uuid4())
    flash_message = {
        "text": "You have been logged out successfully." if lang == "en" else "Sikeresen kijelentkeztél.",
        "category": "success"
    }
    await redis.setex(f"flash:{flash_id}", FLASH_EXPIRE_SECONDS, json.dumps(flash_message))


    # Emit socket events for this session
    org_sids = await redis.smembers(f"org:{org_id}:connections")
    print("sids")
    print(org_sids)

    still_online = False

    for sid in org_sids:
        conn = await redis.hgetall(f"connection:{sid}")
        if conn.get("user_id") == str(user_id):
            still_online = True
            break

    if not still_online:
        await redis.delete(f"online:{org_id}:{user_id}")
                          

    session_sids = []
    for sid in org_sids:
        conn = await redis.hgetall(f"connection:{sid}")
        if not conn:
            continue  # skip current session
        
        #session_sids → sockets of THIS browser/device
        if conn.get("session_id") == session_id:
            session_sids.append(sid)

        # other_user_sids → all sockets of this user (all devices)
        # if conn.get("user_id") == user_id:
        #     other_user_sids.append(sid)

    
    # Delete the current session
    await redis.delete(f"session:{session_id}")

    for sid in org_sids:
        conn = await redis.hgetall(f"connection:{sid}")
        if conn.get("session_id") == session_id:
            try:
                await sio.call(
                    "force_logout_index",
                    {"reason": reason, "flash_id": flash_id},
                    to=sid,
                    timeout=2
                )
            except Exception:
                print("[Logout] client did not ACK, forcing disconnect anyway")

            # Ensure socket disconnect anyway
            print("MEgyünk ide?")
            await sio.disconnect(sid)

    try:
        sids = await redis.smembers(f"org:{org_id}:connections")

        await asyncio.gather(
            *[
                sio.emit(
                    'user_online_status_changed',
                    {'user_id': user_id, 'is_online': False},
                    to=sid
                )
                for sid in sids
            ],
            return_exceptions=True
        )

    except Exception as e:
        print("Socket error:", e)


    # Delete cookies and redirect to home
    response = RedirectResponse(url=f"/?flash_id={flash_id}", status_code=302)
    response.delete_cookie("session_id")
    response.delete_cookie("lang")
    return response


#    REAL LOGOUT
# @router.api_route("/logout", methods=["GET", "POST"])
# async def logout(request: Request, 
#                  current_user: dict | None = Depends(get_current_user),
#                  reason: str = Query("manual")):
    
    

 

    
    
#     print("scheme:", request.url.scheme)
#     print("index:", request.url_for("index"))


#     redis = request.app.state.redis_client
#     session_id = request.cookies.get("session_id")

#     tenant = os.environ.get("B2C_TENANT")
#     policy = os.environ.get("B2C_POLICY")




#     redirect_uri = quote("https://redrain1230_viktor.loophole.site/", safe="")
    



#     lang = request.cookies.get("lang", "hu")  # fallback to cookie if user not present
#     if lang not in ["en", "hu"]:
#         lang = "hu"

  

#     # --- Load session details from Redis ---
#     # user_id = await redis.get(f"session:{session_id}:user_id")
#     # org_id = await redis.get(f"session:{session_id}:user_org")

   
#     user_id = await redis.hget(f"session:{session_id}", "user_id")
#     org_id = await redis.hget(f"session:{session_id}", "user_org")

#     if not user_id or not org_id:
#         return RedirectResponse(
#         url="https://redrain1230_viktor.loophole.site/",
#         status_code=302
#     )

#     print(f"[Logout] Before clearing session: user_id={user_id}, org_id={org_id}")
#     org_id_int = int(org_id) if org_id else None

  
#     org_sids = await redis.smembers(f"org:{org_id_int}:connections")
#     session_sids = []
#     #other_user_sids = []

#     for sid in org_sids:
#         conn = await redis.hgetall(f"connection:{sid}")
#         if not conn:
#             continue  # skip current session
        
#         #session_sids → sockets of THIS browser/device
#         if conn.get("session_id") == session_id:
#             session_sids.append(sid)

#         # other_user_sids → all sockets of this user (all devices)
#         # if conn.get("user_id") == user_id:
#         #     other_user_sids.append(sid)

    
#     # Delete the current session
#     await redis.delete(f"session:{session_id}")

#     # after we loop through the sids belong to this session(browser, device) and delete then separately and do the cleaning, 
#     # disconnect handles everything, simple reconnection, sid deletion and logout as well
#     for sid in session_sids:
#         print("SID LOGOUT: ", sid)
#         await sio.emit("force_logout_index", {"reason": reason}, to=sid)
#         await sio.disconnect(sid)

    

#     print(f"User {user_id} logged out (session {session_id})")



#     # Add a flash message in Redis
#     lang = request.cookies.get("lang", "hu") 

#     flash_id = str(uuid.uuid4())
#     if reason == "expired":
#         flash_message = {
#             "text": "Your session expired due to inactivity. You have been logged out automatically." if lang == "en" else "Az időkorlát lejárt, ezért a kijelentkezés automatikusan megtörtént.",
#             "category": "warning"
#         }
#     else:
#         flash_message = {
#             "text": "You have been logged out successfully." if lang == "en" else "Sikeresen kijelentkeztél.",
#             "category": "success"
#         }
#     await redis.setex(f"flash:{flash_id}", FLASH_EXPIRE_SECONDS, json.dumps(flash_message))

#     # Embed flash_id directly in post_logout_redirect_uri
#     redirect_uri = quote(f"https://redrain1230_viktor.loophole.site/?flash_id={flash_id}", safe="")
#     logout_url = (
#         f"https://{tenant}.b2clogin.com/{tenant}.onmicrosoft.com/"
#         f"{policy}/oauth2/v2.0/logout?post_logout_redirect_uri={redirect_uri}"
#     )

#     response = RedirectResponse(url=logout_url)
#     response.delete_cookie("session_id")
    
#         # if "?" in response.headers["Location"]:
#         #     response.headers["Location"] += f"&flash_id={flash_id}"
#         # else:
#         #     response.headers["Location"] += f"?flash_id={flash_id}"

#     # Redirect to the B2C logout URL
#     return response




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









def format_money(amount, currency):
    if currency == "HUF":
        return f"{amount:,.0f} Ft".replace(",", " ")
    if currency == "EUR":
        return f"{amount/100:.0f} €"
    if currency == "USD":
        return f"${amount/100:.0f}"
    return f"{amount} {currency}"


@router.get("/subscription", response_class=HTMLResponse)
async def subscription(request: Request,
    csrf_protect: CsrfProtect=Depends(),
    current_user: dict = Depends(login_required),
    ):

    
    email = current_user["email"]
    email_prefix = email.split("@")[0] if email else ""
    user_id = current_user["id"]
    user_org = int(current_user["org_id"])
    name = current_user["name"]
    user_role = current_user["role"]
    language = current_user.get("language", "hu")
    first_character = current_user.get("first_character")

    if language not in ["en", "hu"]:
      print("[DEBUG] Invalid lang detected, defaulting to 'hu'")
      language = "hu"

    client = None
    subscription = None
    price = None
  
    client_name = "Your Company"
    service_message = "Please contact Red Rain to select a service."


    async with async_session_scope() as db_session:
        client = await db_session.scalar(
            select(Client)
            .options(joinedload(Client.subscription))
            .where(Client.id == user_org)
        )

        if client:
            client_name = client.client_name
            subscription = client.subscription
            currency = client.currency if client else "HUF"


            if subscription:
                # fetch active price from SubscriptionPrice table
                active_price = await db_session.scalar(
                    select(SubscriptionPrice)
                    .where(
                        SubscriptionPrice.subscription_id == subscription.id,
                        SubscriptionPrice.currency == client.currency,
                        SubscriptionPrice.active == True
                    )
                )
                if active_price:
                    price_per_seat_minor = active_price.amount  # integer in cents or HUF
                    total_seats = client.seats or 1

                    # Convert to major units if currency uses cents
                    if client.currency in ("USD", "EUR"):
                        price_per_seat = price_per_seat_minor / 100
                    else:
                        price_per_seat = price_per_seat_minor

                    price = price_per_seat * total_seats  # final price as float

                    # Optional: format nicely for display
                    if client.currency in ("USD", "EUR"):
                        price = f"{price:.2f}"  # 2 decimal places
                    else:
                        price = str(int(price))  # no decimals for HUF

        else:
            client_name = "Your Company"
            service_message = "Please contact Red Rain to select a service."
    

        tier_prices = {}

        rows = await db_session.execute(
            select(Subscription, SubscriptionPrice)
            .join(
                SubscriptionPrice,
                Subscription.id == SubscriptionPrice.subscription_id
            )
            .where(
                SubscriptionPrice.currency == client.currency,
                SubscriptionPrice.active == True
            )
        )

        TIER_KEY = {
            1: "basic",
            2: "gold",
            3: "platinum",
        }

        for subscription_lineitem, price_lineitem in rows:
            key = TIER_KEY.get(subscription_lineitem.id)
            if not key:
                continue

            unit_seat_price = price_lineitem.amount
            included_seats = subscription_lineitem.base_seats
            tier_total_price = unit_seat_price * included_seats

            tier_prices[f"{key}_price"] = format_money(tier_total_price, client.currency)
            tier_prices[f"{key}_extra"] = format_money(unit_seat_price, client.currency)
        
  
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
            "tier_prices": tier_prices,
            "client_currency": currency,
            
            "language": language,
            "user_id": user_id,
            "csrf_token": csrf_token,  # <-- pass CSRF to template
        },
    )
    csrf_protect.set_csrf_cookie(signed_token, response)  # <-- set cookie
    return response




# STRIP PROCESS FOR NEW CUSTOMERS:
# SYNC STRIPE INTERNAL OPERATIONS
# ────────────────────────────────

# 1. Create subscription object   OBJ which is a json with id, "status":"incomplete", period start end etc.
#    → status = incomplete

# 2. Create draft invoice object

# 3. Attempt payment on invoice

# 4. Finalize invoice
#    → invoice.status = paid
#    → invoice now contains period.start / period.end
#    → invoice is immutable accounting record


# ASYNC STRIPE OPERATIONS
# ────────────────────────────────

# 5. Emit invoice.paid webhook
#    → your handler receives:
#      obj = invoice object

#    IMPORTANT:
#    subscription may still be incomplete at this exact moment


# 6. Activate subscription
#    → subscription.status = active

# 7. Populate subscription period fields
#    → subscription.current_period_start
#    → subscription.current_period_end

# 8. Emit subscription.updated webhook

#create_checkout_session = store pending request
@router.post("/subscription/create_checkout_session")
async def create_checkout_session(
    client_id: int = Form(...),
    plan_id: int = Form(...),
    additional_seats: int = Form(...),
    auto_upgrade: Optional[int] = Form(0)
):
    print("IDEBE0001?")
    async with async_session_scope() as db:

        client = await db.scalar(select(Client).where(Client.id == client_id))
        plan = await db.scalar(select(Subscription).where(Subscription.id == plan_id,))

        if not client or not plan:
            return {"error": "Client or plan not found"}

        if additional_seats < 0:
            additional_seats = 0

        total_quantity = plan.base_seats + additional_seats

        price = await db.scalar(
            select(SubscriptionPrice).where(
                SubscriptionPrice.subscription_id == plan.id,
                SubscriptionPrice.currency == client.currency,
                SubscriptionPrice.active == True
            )
        )

        if not price:
            return {"error": f"No active price for currency {client.currency}"}

        stripe_price_id = price.stripe_price_id
        current_plan_id = client.subscription_id
        new_plan_id = plan.id
        




        ####################################################
        # Create Stripe customer if needed
        # METADATA 1 CUSTOMER lives: On the Stripe Customer object.
        # can be accessed:
        # customer = stripe.Customer.retrieve(stripe_customer_id)
        # print(customer.metadata.get("client_id"))  # gives your client.id
        # not related to subsription
        ####################################################

        if not client.stripe_customer_id:
            customer = stripe.Customer.create(
                email=client.billing_email,
                name=client.client_name,
                metadata={"client_id": client.id},
            )
            client.stripe_customer_id = customer.id

        # ----------------------------------
        # CASE 1 — EXISTING SUBSCRIPTION → UPGRADE
        # ----------------------------------
        if client.stripe_subscription_id:
            try:
                print(f"Upgrading subscription: {client.stripe_subscription_id}")

                stripe_sub = stripe.Subscription.retrieve(client.stripe_subscription_id)

                item = stripe_sub["items"]["data"][0]
                item_id = item["id"]
                current_amount = item["price"]["unit_amount"]

                # Modify subscription modify call does not wait for payment success only returns the subscription object
                stripe.Subscription.modify(
                    client.stripe_subscription_id,
                    items=[{
                        "id": item_id,
                        "price": stripe_price_id,
                        "quantity": total_quantity,
                    }],
                    proration_behavior="create_prorations",
                )
                print(f"Subscription modify called successfully")

                # Immediately update DB (safe fields only)
                client.subscription_id = plan.id
                client.seats = total_quantity
                client.subscription_status = "active"
                client.auto_upgrade = bool(auto_upgrade)
                print(f" DB updated with plan and seats")

       

                await db.flush()
                print(f"DB flush completed")

                if new_plan_id > current_plan_id:
                    redirect_type = "upgrade"
                elif new_plan_id < current_plan_id:
                    redirect_type = "downgrade"
                else:
                    redirect_type = "same"


            except Exception as e:
                print(f"Upgrade failed: {e}")
                return {"status": "upgrade_failed", "error": str(e)}

            return {
                "checkout_url":
                 f"https://http://localhost:8001/subscription?status=success&type={redirect_type}&client_id={client.id}"
              #  f"https://redrain1230_viktor.loophole.site/subscription?status=success&type={redirect_type}&client_id={client.id}"
            }

        # ----------------------------------
        # CASE 2 — NEW SUBSCRIPTION → CHECKOUT
        # ----------------------------------
        else:
            client.auto_upgrade = bool(auto_upgrade)
            await db.flush() #other queries in the same session can immediately see the changes,  Sends pending changes (INSERTs/UPDATEs/DELETEs) to the database but does not commit the transaction.
            session = stripe.checkout.Session.create(  #creates a Stripe object stored on Stripe’s servers
                                            # inside the object Stripe saves success_url, cancel_url, etc.
                customer=client.stripe_customer_id,
                payment_method_types=["card"],
                line_items=[{
                    "price": stripe_price_id,
                    "quantity": total_quantity,
                }],
                mode="subscription",
                allow_promotion_codes=True,


                ################################
                # Checkout session metadata 2 
                # Passes info to Stripe about the subscription + later after checkout the obj will be enhanced incuding subscriptipné 
                # being created, so webhook can read it later 
                # when the session completes.
                # Can be access: 
                # metadata = obj.get("metadata")  de ez más obj , mint ami a webhookban van
                # client_id = metadata.get("client_id")
                # plan_id = metadata.get("plan_id")
                # seats = metadata.get("seats")
                ################################

                metadata={  # this metadata lives here:  checkout.session.metadata
                    "client_id": str(client.id),
                    "plan_id": str(plan.id),
                    "seats": str(additional_seats),
                    "currency": client.currency,
                },
                success_url = f"http://localhost:8001/subscription?status=success&type=new&client_id={client.id}",
                cancel_url=f"http://localhost:8001/subscription?status=failure&client_id={client.id}"


                # success_url = f"https://redrain1230_viktor.loophole.site/subscription?status=success&type=new&client_id={client.id}",
                # cancel_url=f"https://redrain1230_viktor.loophole.site/subscription?status=failure&client_id={client.id}"
            )
            # at this point: client.stripe_subscription_id = NULL. not created yet
            return {"checkout_url": session.url}  # we go to stripe page with this

    ################################################################
    # 3.METADATA
    # After a customer completes checkout, Stripe creates a subscription object:
    # Automatically created by Stripe when the checkout session completes, Stripe copies the checkout session metadata
    # Where it lives: On the Subscription object (subscription.metadata).
    # Purpose: Permanent metadata for that specific subscription.
    ###########################################################

# Stripe calling my server, not browser
# Entering the webhook does not always mean payment is complete
# The webhook fires whenever Stripe sends an event.
# obj (created by stripe after checkout) exists as soon as the webhook fires — but some fields are populated progressively, depending on the event.

# The user entered their card details on Stripe’s hosted page (session.url).
# Stripe charges the card or sets up the subscription. Stripe internally creates the subscription object, sets subscription.id, current_period_start, current_period_end, etc.
# Stripe also associates your metadata (client_id, plan_id, seats) with the subscription.
# Stripe redirects the user If payment is successful, the user’s browser is redirected to your success_url you provided in the checkout session. If canceled, redirected to cancel_url.

#Stripe independently sends an HTTP POST request to your /stripe/webhook endpoint. This happens regardless of the user being redirected


# OBJECTS

# When Stripe calls webhook, it sends you a JSON message like this:
# {
#   "id": "evt_123",
#   "type": "invoice.paid",
#   "data": {
#     "object": { ... SOME STRIPE OBJECT ... }
#   }
# }

# event
#  └── data
#       └── object   ← THIS is obj


# obj = event["data"]["object"] “Give me the Stripe object that triggered this event.” That object is NOT always the same type.It changes depending on the event.

# Case 1: checkout.session.completed checkout.session.completed
# obj:
# {
#   "object": "checkout.session",
#   "id": "cs_123",
#   "subscription": "sub_ABC",
#   "metadata": {
#     "client_id": "2",
#     "plan_id": "3",
#     "seats": "5"
#   }
# }
# so here: obj = checkout session
# Case 2: invoice.paid  
#   obj = invoice   "subscription": "sub_ABC"
# {
#   "object": "invoice",
#   "id": "in_456",
#   "subscription": "sub_ABC",
#   "customer": "cus_999",
#   "metadata": {}
# }

# SUBSCRIPTION object
#The subscription is a separate object stored in Stripe.
#To get it, you must explicitly ask Stripe stripe_sub = stripe.Subscription.retrieve("sub_ABC")

# {
#   "object": "subscription",
#   "id": "sub_ABC",
#   "metadata": {
#     "plan_id": "3",
#     "seats": "5"
#   },
#   "current_period_start": 1770809307,
#   "current_period_end": 1773228507
# }




@router.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    print("NA??")
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, webhook_secret
        )
    except stripe.error.SignatureVerificationError:
        return {"status": "invalid_signature"}

    event_type = event["type"]
    obj = event["data"]["object"]  # this is JSON payload, the invoice of the webhook Depends on the event_type. Webhooks can send many types of events (checkout.session.completed, invoice.paid, etc.). For checkout.session.completed: obj is a Checkout Session object. for invoice.paid: obj is an Invoice object. If event_type == "checkout.session.completed", this metadata is exactly what I set in checkout.Session.create, it is copied here If event_type == "invoice.paid", obj.get("metadata") usually is empty,
    metadata = obj.get("metadata", {})
    

    async with async_session_scope() as db:

        # -------------------------------
        # CHECKOUT COMPLETED (first activation) first-time subscription or payment.
        # -------------------------------
        if event_type == "checkout.session.completed":
            metadata = obj.get("metadata", {}) # we got this metadata: checkout.session.metadata
            client_id = metadata.get("client_id")
            plan_id = metadata.get("plan_id")
            seats = int(metadata.get("seats", 0))

            client = await db.scalar(select(Client).where(Client.id==int(client_id)))
            if not client:
                return {"status": "client_not_found"}

            # Only fill subscription_id / seats / stripe_subscription_id if missing
            if not client.stripe_subscription_id:
                client.stripe_subscription_id = obj.get("subscription")
            if not client.subscription_id:
                client.subscription_id = int(plan_id)
            if not client.seats:
                client.seats = seats
  

            print("IDEBE01?")

            await db.flush()
            return {"status": "activated"}

        # -------------------------------
        # PAYMENT SUCCESS
        # -------------------------------
        if event_type == "invoice.paid":
            print("\n========== INVOICE.PAID ==========")

            stripe_subscription_id = (
                obj.get("subscription")
                or obj.get("parent", {})
                    .get("subscription_details", {})
                    .get("subscription")
            )

            print("Subscription ID:", stripe_subscription_id)

            if not stripe_subscription_id:
                print("Not a subscription invoice")
                return {"status": "ignored"}

            client = await db.scalar(
                select(Client).where(
                    Client.stripe_subscription_id == stripe_subscription_id
                )
            )

            if not client:
                stripe_customer_id = obj.get("customer") #ez a stripe_customer_id
                if stripe_customer_id:
                    customer = stripe.Customer.retrieve(stripe_customer_id)
                    # {   Ez a customer  EZT MÉG AZ ELŐZŐ ROUTBAN CSINÁLTAM
                    #     "id": "cus_ABC123",
                    #     "email": "viktor@example.com",
                    #     "name": "Viktor",
                    #     "metadata": {
                    #         "client_id": "2"
                    #     }
                    #     }
                    client_id = customer.metadata.get("client_id")
                    if client_id:
                        client = await db.scalar(
                            select(Client).where(Client.id == int(client_id))
                        )
            if not client:
                return {"status": "client_not_found"}
            
            if client.subscription_status == "active":
                print("Already active, skipping DB update")
                return {"status": "already_active"}

            start_ts = None
            end_ts = None
            seats = 0 
            subscription_price = None

            try:
                # Try subscription first
                stripe_sub = stripe.Subscription.retrieve(stripe_subscription_id)
                print("JSON!!!!")
                print(stripe_sub)
                if stripe_sub['items']['data']:
                    item = stripe_sub['items']['data'][0]
                    seats = item.get('quantity', 0)
                    price_id = item.get('price', {}).get('id')
                else:
                    seats = 0
                    price_id = None

                subscription_price = await db.scalar(
                    select(SubscriptionPrice).where(SubscriptionPrice.stripe_price_id == price_id)
                )


                # stripe_sub: subscription = {  EZT a c
                #     "id": "sub_1SzbarRy7qqP1p01cT1msqBd",
                #     "object": "subscription",
                #     "status": "active",
                #     "customer": "cus_Qx82KzA1",
                #     "current_period_start": 1770809307,
                #     "current_period_end": 1773228507,
                #     "metadata": {
                #         "plan_id": "3",
                #         "seats": "5"
                #     },
                #     "items": {
                #         "data": [
                #         {
                #             "price": {"id": "price_ABC"},
                #             "quantity": 5
                #         }
                #         ]
                #     }
                #     }
                start_ts = getattr(stripe_sub, "current_period_start", None)
                end_ts = getattr(stripe_sub, "current_period_end", None)

                print("Subscription period:", start_ts, end_ts)

            except Exception as e:
                print("Subscription fetch failed:", e)

            # Fallback to invoice lines if missing
            if not start_ts or not end_ts:
                print("Using invoice fallback")

                lines = obj.get("lines", {}).get("data", [])

                if lines:
                    period = lines[0].get("period", {})
                    start_ts = period.get("start")
                    end_ts = period.get("end")

            print("Final timestamps:", start_ts, end_ts)

            if start_ts:
                client.subscription_start_date = datetime.fromtimestamp(start_ts)

            if end_ts:
                client.subscription_end_date = datetime.fromtimestamp(end_ts)

           

            if not client.stripe_subscription_id:
                client.stripe_subscription_id = stripe_subscription_id
            client.seats = seats
            client.subscription_id = subscription_price.subscription_id if subscription else None
            client.subscription_status = "active"
            await db.flush()

            print("DB updated")
            print("========== DONE ==========\n")

            return {"status": "paid_confirmed"}
        


        # -------------------------------
        # PAYMENT FAILED
        # -------------------------------
        if event_type == "invoice.payment_failed":

            stripe_subscription_id = obj.get("subscription")

            client = await db.scalar(
                select(Client).where(
                    Client.stripe_subscription_id == stripe_subscription_id
                )
            )

            if client:
                client.subscription_status = "past_due"
                await db.flush()

            return {"status": "payment_failed"}

        # -------------------------------
        # SUBSCRIPTION CANCELED
        # -------------------------------
        if event_type == "customer.subscription.deleted":

            stripe_subscription_id = obj.get("id")

            client = await db.scalar(
                select(Client).where(
                    Client.stripe_subscription_id == stripe_subscription_id
                )
            )

            if client:
                client.subscription_status = "canceled"
                client.subscription_end_date = datetime.utcnow()
                await db.flush()

            return {"status": "canceled"}

    return {"status": "ignored"}

@router.get("/api/client/subscription-status")  #we need this as we don't see outside what is happening in stripe webhook
async def subscription_status(client_id: int):
    async with async_session_scope() as db:
        client = await db.scalar(select(Client).where(Client.id==client_id))
        if not client:
            return {"status": "not_found"}
        return {"status": client.subscription_status}
    

    
@router.post("/cancel_subscription")
async def cancel_subscription(request: Request):
    async with async_session_scope() as db:
        data = await request.json()
        client_id = data.get("client_id")

        if client_id is None:
            raise HTTPException(status_code=400, detail="client_id is required")

        # Convert to int if it came as a string
        try:
            client_id = int(client_id)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="client_id must be an integer")
       
        client = await db.scalar(select(Client).where(Client.id == client_id))

        if not client or not client.stripe_subscription_id:
            return {"error": "No active subscription"}
        print("bejött???")
        try:
            stripe.Subscription.modify(
                client.stripe_subscription_id,
                cancel_at_period_end=True
            )

            client.subscription_status = "cancel_scheduled"
            await db.flush()

            return {"status": "scheduled"}

        except Exception as e:
            return {"error": str(e)}

    



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





###########################################################################
#                 End of REDIS CONFIG
###########################################################################



@router.post("/api/upload_image")
async def upload_image(
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user)
):
    """
    Uploads a file to Azure Blob Storage under:
    imageuploads/org_id/user_id/unique_filename
    """

    org_id = int(user["org_id"])
    user_id = user["user_id"]

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
    user_org = int(user["org_id"])
    language = user.get("language", "hu")
    first_character = user.get("first_character")
    user_name=user["name"]

    
    return templates.TemplateResponse(
        "admin_dashboard.html",
        {
            "request": request,
            "user_org": user_org,
            "first_character": first_character,
            "language": language,
            "user_id": user_id,
            "user_name": user_name
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
                select(Client.mode).where(Client.id == int(org_id))
            )
            mode = result.scalar_one_or_none()

            if mode:
                print(f"Retrieved mode for org_id={int(org_id)}: {mode}")
                return mode
            else:
                print(f"No mode found for org_id={int(org_id)}. Defaulting to 'automatic'.")
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
                org_id=int(org_id),
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
                    OrgEventLog.org_id == int(org_id),
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
                    OrgEventLog.org_id == int(org_id),
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
                    "org_id": int(event.org_id),
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
                    OrgEventLog.org_id == int(org_id),
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





@sio.on("admin_response_to_the_chatbot")
async def handle_admin_response(sid, data):
    """Handle admin messages sent to chatbot and forward them to Redis with admin name and org_id."""

    print(f"Admin response received from sid={sid}: {data}")

    # --- Retrieve org_id and user_id from Connections ---
    org_id = None
    user_id = None
    admin_name = None
    redis=None
    attachment=None

    try:
        # Fetch connection data from Redis
        fastapi_app = sio.fastapi_app
        if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
            print("Redis not ready yet")
            await sio.disconnect(sid)
            return

        redis = fastapi_app.state.redis_client

        connection_key = f"connection:{sid}"
        conn_data = await redis.hgetall(connection_key)

        if not conn_data:
            print(f"No connection found for socket ID: {sid}")
            return

        # Extract org_id and user_id safely
        org_id_str = conn_data.get("org_id")
        org_id = int(org_id_str) if org_id_str else None

        user_id_str = conn_data.get("user_id")
        user_id = int(user_id_str) if user_id_str else None

        admin_name = None
        if user_id:
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
    
    attachment = data.get("attachment")

    # --- Enrich message data ---
    data["admin_name"] = admin_name
    data["org_id"] = org_id
    data["attachment"]=attachment

    # --- Publish to Redis ---
    try:
        environ = sio.get_environ(sid)
        fastapi_app = sio.fastapi_app

        if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
            print("Redis not ready yet")
            await sio.disconnect(sid)
            return
        
    
        redis = fastapi_app.state.redis_client

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
    await redis.eval(script, 1, key, lock_id)

@sio.event
async def connect(sid, environ, auth=None): # connect() is special → Socket.IO passes environ directly
    #other events (@sio.on(...)) are not → you must fetch it yourself with sio.get_environ(sid)
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    # app = environ.get("asgi.scope", {}).get("app")
    # redis = app.state.redis_client
  
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client
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
    

    # lock_key = f"org_connect_cleanup_lock:{org_int}"
    # lock_id = str(uuid.uuid4())
    # acquired = await acquire_redis_lock(redis, lock_key, lock_id, expire=30)
    # if acquired:
    #     try:
    #         async with async_session_scope() as session:
    #             # Check if cleanup is needed (no active connections, but stale entries exist)
    #             active_sockets = await redis.scard(f"org:{org_int}:connections")  # get only the count

    #             if active_sockets == 0:
    #                 # Perform missing cleanup if necessary
    #                 print(f"[Connect] Performing missing cleanup for org {org_int}")
    #                 await session.execute(delete(OrgEventLog).where(OrgEventLog.org_id == org_int))
    #                 await session.execute(update(Client).where(Client.id == org_int).values(mode="automatic"))
    #                 await redis.delete(f"user_mode_override:{org_int}")
               
    #                 client_to_update = await session.scalar(
    #                     select(Client)
    #                     .where(Client.id == org_int, Client.is_active.is_(True), Client.last_manualmode_triggered_by.isnot(None))
    #                 )
    #                 if client_to_update:
    #                     client_to_update.last_manualmode_triggered_by = None
    #                     print(f"[Connect] Updated client {client_to_update.id}: last_manualmode_triggered_by cleared")
    #     finally:
    #         await release_redis_lock(redis, lock_key, lock_id)
    # else:
    #     print(f"[Connect] Another worker is handling cleanup for org {org_int}")


    
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
                "session_id": session_id,
                "admin_internal_message_open": "null",  # initially closed
                "admin_internal_message_close": "null",
            })
            await redis.sadd(org_set_key, socket_id)
            await redis.expire(connection_key, 10 * 60)  # optional TTL for safety
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


# this does : 1.) refresh TTL: await redis.expire(connection_key, 10 * 60)
#             2.) refresh org-related TTL:  tab_mode_keys → expire 6h
#                                           user_mode_override → expire 6h
#            3.) update session activity  await redis.hset(session_key, "last_active", now)
#                                         await redis.expire(session_key, SESSION_TTL)

@sio.event    # update TTLs  for socketio enabled page  Keeps the connection alive
async def heartbeat(sid):
    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client
    # THIS IS FOR SMALL INTERNET BLIP OR REFRESH WHEN  THERE IS NO DISCONNECT
    connection_key = f"connection:{sid}"

    # Refresh TTL for the connection itself
    await redis.expire(connection_key, 10 * 60)  # 6 hours

    # Get org_id from the connection hash
    connection_data = await redis.hgetall(connection_key)

    org_id_str = connection_data.get("org_id")
    if not org_id_str:
        print(f"[Heartbeat] No org_id found for socket {sid}")
        return

    org_id = int(org_id_str) 

    # Refresh TTL for all tab_mode keys for this org
    #tab_mode_keys = await redis.keys(f"org:{org_id}:tab:*:mode")
    #this is better with scan_iter  non blocking and faster
    tab_mode_keys = []
    async for key in redis.scan_iter(f"org:{org_id}:tab:*:mode"):
        tab_mode_keys.append(key)
    

    # Refresh TTL concurrently
    if tab_mode_keys:
        await asyncio.gather(*(redis.expire(k, 3600 * 6) for k in tab_mode_keys))
        print(f"[Heartbeat] Refreshed TTL for {len(tab_mode_keys)} tab_mode keys in org {org_id}")


    # Refresh TTL for user_mode_override key if it exists
    user_mode_key = f"user_mode_override:{org_id}"
    
    await redis.expire(user_mode_key, 3600 * 6)  # Extend TTL
    print(f"[Heartbeat] Refreshed TTL for user_mode_override key for org {org_id}")

    
    session_id = connection_data.get("session_id")

    if session_id:
        session_key = f"session:{session_id}"

        session_data = await redis.hgetall(session_key)

        if not session_data:
            print(f"[Heartbeat] Session expired for {sid}")
            await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
            await sio.disconnect(sid)
            return

        now = int(time.time())
        await redis.hset(session_key, "last_active", now)
        await redis.expire(session_key, SESSION_TTL)  # reset with TTL
        

@sio.on("history_ready")
async def handle_history_ready(sid, data):
    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
    print("histoizunk?")
    redis = fastapi_app.state.redis_client
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
        print("ide még bejövünk?")
        missed_event_dicts = await get_sorted_event_logs(org)
        print(missed_event_dicts)
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
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
      
        return
    
   
    redis = fastapi_app.state.redis_client



    try:
        # Get connection info from Redis
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)

        if not connection_data:
            async for key in redis.scan_iter("org:*:connections"):
                await redis.srem(key, sid)
            print(f"[Redis] No connection record found for {sid}, skipping.")
            return
       
        
        if connection_data:
            # Decode Redis fields
            org_id = int(connection_data["org_id"])
            user_id = int(connection_data["user_id"])
            manual_mode = connection_data.get("manualmode_triggered") == "true"
            session_id = connection_data.get("session_id")

            org_sids = await redis.smembers(f"org:{org_id}:connections")
            print("ORG SET BEFORE CLEAN:", org_sids)
            for s in list(org_sids):
                exists = await redis.exists(f"connection:{s}")
                print("Checking", s, "exists:", exists)

                if not exists:
                    print("Removing stale sid:", s)
                    await redis.srem(f"org:{org_id}:connections", s)
            
                session_id = await redis.hget(f"connection:{s}", "session_id")
                if not session_id or not await redis.exists(f"session:{session_id}"):
                    print("Removing stale sid:", s)

                    await redis.delete(f"connection:{s}")
                    await redis.srem(f"org:{org_id}:connections", s)



            # Mark disconnected_at in Redis
            disconnected_at = datetime.utcnow().isoformat()
            await redis.hset(connection_key, "disconnected_at", disconnected_at)
            print(f"Admin {user_id} from org {org_id} disconnected with socket ID {sid}")

            async def cleanup_after_grace_period():
                print(f"[Grace Period] Starting cleanup for {sid}")
                await asyncio.sleep(15)  # Grace period

           
               
                print("ide??")
                try_count = 0
                while try_count < 2:
                    try: 
                        print("kezdődik")  
                        conn = await redis.exists(connection_key)
                        if not conn:
                            print("Connection already cleaned up.")
                            return

                        # Check for other active admins (still connected)
                        deleted=await redis.delete(f"connection:{sid}")
                        print("Deleted result:", deleted)
                        await redis.srem(f"org:{org_id}:connections", sid)
                        print("About to delete", f"connection:{sid}")
                        

                        org_sids = await redis.smembers(f"org:{org_id}:connections")
                      

                        print(f"[Grace Period] Active sockets for org {org_id}: {len(org_sids)}")
                        

                        still_online = False

                        for sid_ in org_sids:
                            conn = await redis.hgetall(f"connection:{sid_}")
                            if conn and conn.get("user_id") == str(user_id):
                                still_online = True
                                break
                        
                        if not still_online:
                            await redis.delete(f"online:{org_id}:{user_id}")
                          
                            for sid_ in org_sids:
                                await sio.emit(
                                    "user_online_status_changed",
                                    {"user_id": user_id, "is_online": False},
                                    to=sid_
                                )

                            print(f"Admin {user_id} from org {org_id} went offline")
                        else:
                            print(f"Admin {user_id} from org {org_id} disconnected from session {session_id}, but remained active sessions")

                        for si in list(org_sids):
                            conn_sess = await redis.hget(f"connection:{si}", "session_id")
                            if not conn_sess or not await redis.exists(f"session:{conn_sess}"):
                                print("Grace-period: Removing stale sid:", si)
                                await redis.delete(f"connection:{si}")
                                await redis.srem(f"org:{org_id}:connections", si)
                        
                        

                        org_sids = await redis.smembers(f"org:{org_id}:connections")
                        valid_sids = []

                        for s in org_sids:
                            exists = await redis.exists(f"connection:{s}")
                            if exists:
                                valid_sids.append(s)
                            else:
                                await redis.srem(f"org:{org_id}:connections", s)

                        org_sids = valid_sids

                        print("sidek", org_sids) 
                        if len(org_sids)==0:
                            # here we lock, in order that one cleaning per org will be achieved
                            lock_key = f"org_cleanup_lock:{org_id}"
                            lock_id = str(uuid.uuid4())

                            acquired = await acquire_redis_lock(redis, lock_key, lock_id)
                            if not acquired:
                                print(f"[Grace Period] Another worker is already cleaning org {org_id}")
                                return  # Another worker is handling cleanup
                            # No active connections → reset mode and clear entries

                            
                            try:
                               
                                async with async_session_scope() as delayed_session:
                                    await delayed_session.execute(
                                        delete(OrgEventLog).where(OrgEventLog.org_id == org_id)
                                    )
                                    await delayed_session.execute(
                                        update(Client).where(Client.id == org_id).values(mode="automatic")
                                    )
                                    await redis.delete(f"user_mode_override:{org_id}")

                                    # DELETE CLIENT STATE FROM REDIS
                                    await redis.delete(f"client:{org_id}:state")

                                    await redis.delete(f"user_mode_override:{org_id}")

                        
                                    # Clear Redis tab mode keys for this org
                                    async for key in redis.scan_iter(f"org:{org_id}:tab:*:mode"):
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

                            except Exception as e:
                                print(f"[DB] Cleanup failed for org {org_id}: {e}")
                            finally:
                                await release_redis_lock(redis, lock_key, lock_id)
                        else:

                            
                            # There are still active admins → propagate manualmode if needed
                            if manual_mode:
                            # Pick another still-active socket from the same org
                                for s in org_sids:
                                    other_conn = await redis.hgetall(f"connection:{s}")
                                    if other_conn:
                                        disconnected_at = other_conn.get("disconnected_at")
                                        if not disconnected_at or disconnected_at == "null":
                                            # Found a valid still-connected admin → give them manual mode
                                            await redis.hset(f"connection:{s}", mapping={"manualmode_triggered": "true"})
                                            print(f"[Grace Period] Propagated manualmode_triggered from {sid} → {s}")
                                            break
                            # Finally, clean up this disconnected connection
                            
                            print(f"[Grace Period] Deleted disconnected user {user_id} (socket {sid}) from Redis")
                        break
                    except Exception as e:
                        try_count += 1
                        print(f"[Grace Period] Cleanup attempt {try_count} failed for socket {sid}: {e}")
                        if try_count < 2:
                            print("Retrying cleanup once more...")
                        else:
                            print("Cleanup failed after one retry, skipping.")
                
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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None

    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client
    cpu_pool = fastapi_app.state.cpu_pool
    cpu_sem = fastapi_app.state.cpu_sem

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

    org_id_str = connection_data.get("org_id")
    org_id = int(org_id_str) if org_id_str else None
    if org_id is None:
        print(f"[Redis] org_id missing in connection {sid}")
        return
    
    
     # --- Update client mode ---
    try:
        await update_client_mode(org_id, mode)
    except Exception as e:
        print(f"Error updating client mode for org {org_id}: {e}")
    
    
    await redis.hset(   # No TTL
            f"client:{org_id}:state",
            mapping={
                "mode": mode  # "manual" or "automatic"
            }
        )

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
            
            if tab_mode_keys:
                await asyncio.gather(*(redis.delete(k) for k in tab_mode_keys))
                print(f"Deleted {len(tab_mode_keys)} tab_mode keys for org {org_id}")
        except Exception as e:
            print(f"Error deleting tab_mode keys for org {org_id}: {e}")
            

    else:
        try:
            await redis.delete(f"user_mode_override:{org_id}")

        except Exception as e:
            print(f"Error deleting UserModeOverride for org {org_id}: {e}")


            
                

    if mode in ['automatic', 'manual']:
        print(f"Emitting mode_changed event to org_id {org_id} with mode: {mode}")
        asyncio.create_task(
            log_event(org_id, 'mode_changed', {'mode': mode}, frontend_time)
        )
                
       
        org_set_key = f"org:{org_id}:connections"
        active_sockets = await redis.smembers(org_set_key)
        active_sockets = [s for s in active_sockets if s != sid]
        
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
        active_sockets = [s for s in active_sockets if s != sid]
    
        # message_payload = []
        # for event in final_data:
        #     msg_with_ts = event['data'].copy()  # make a copy to avoid mutation
        #     msg_with_ts['timestamp'] = event['timestamp']
        #     message_payload.append(msg_with_ts)
        # SAME:  ** : creates a new dict that contains all keys/values from event['data'] plus a key 'timestamp' with the new value.
        message_payload = [
        {**event['data'], 'timestamp': event['timestamp'].isoformat() if isinstance(event['timestamp'], datetime.datetime) else event['timestamp']}
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







@sio.on("update_response_state")  # the rectangles' state if it is manual(true in automatic) or automatic(false) based on what is written on the button
async def handle_update_response_state(sid, data):
    print("NA???", data)

    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)

    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client

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
          org_id_str = conn_data.get("org_id")
          org_id = int(org_id_str) if org_id_str else 0


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
      if state:
          await redis.hset(key, user_id, "manual")

          # Set TTL for the entire hash (e.g., 6 hours)
          await redis.expire(key, 3600 * 6)  # 3600 sec × 6 = 6 hours
      else:
          await redis.hdel(key, user_id)
          

    except Exception as e:
          print(f"Error updating user_mode_override with TTL for org {org_id}, user {user_id}: {e}")


    # Broadcast the new state to all connected clients
    #emit('response_state_update', {'user_id': user_id, 'state': state}, room=org_id, include_self=False)

    try:
      org_key = f"org:{org_id}:connections"

      # Get all socket IDs for this org from Redis
      sids = await redis.smembers(org_key)
    
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



#                                             # RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
                                            
#                                             #     RECTANGLE STATE - OVERALMANUAL   UserModeOverride    R

#                                             # RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
  


# @sio.on("update_response_state_overallmanual")  # the rectangles' state if it is manual or automatic
# async def handle_update_response_state_overallmanual(sid, data):

#     environ = sio.get_environ(sid)  # get environ first
#     cookies = environ.get("asgi.scope", {}).get("headers", [])
#     session_id = None
#     for key, value in cookies:
#         if key == b"cookie":
#             cookie_str = value.decode()
#             for c in cookie_str.split(";"):
#                 if c.strip().startswith("session_id="):
#                     session_id = c.strip().split("=")[1]

#     environ = sio.get_environ(sid)
#     fastapi_app = sio.fastapi_app

#     if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
#         print("Redis not ready yet")
#         await sio.disconnect(sid)
#         return
    
   
#     redis = fastapi_app.state.redis_client

#     # Validate session
#     if not session_id or not await redis.exists(f"session:{session_id}"):
#         await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
#         await sio.disconnect(sid)
#         return  # Stop further processing
    
    
#     org_id=None
#     try:
#         connection_key = f"connection:{sid}"
#         connection_data = await redis.hgetall(connection_key)

#         if not connection_data:
#             print(f"[Redis] No connection found for socket_id {sid}")
#             return

#         org_id_str = connection_data.get("org_id")
#         if not org_id_str:
#             print(f"[Redis] Connection for socket_id {sid} has no org_id")
#             return

#         org_id = int(org_id_str)



#     except Exception as e:
#         # Handle any unexpected errors
#         print(f"An error occurred while processing the database operation: {e}")

#     user_id = data.get('user_id')
#     state = data.get('state')
#     state = data.get('state')
#     tabindex= data.get('tabindex')

#     timestamp = data.get('frontend_time')  # ISO string
    

#     asyncio.create_task(
#         log_event(org_id, 'response_state_changed_overallmanual', {
#             'user_id': user_id,
#             'state': state,
#             'tabindex':tabindex
#         }, timestamp)
#     )
#     # Handle database updates based on the state
#     try:
#         # if state:
#         #     await redis.hset(f"user_mode_override:{org_id}", user_id, "manual")
#         # else:
#         #     await redis.hdel(f"user_mode_override:{org_id}", user_id)

#         if state:
#             await redis.hset(f"user_mode_override:{org_id}", user_id, "manual")

#             # Set TTL for the entire hash (e.g., 6 hours)
#             await redis.expire(key, 3600 * 6)  # 3600 sec × 6 = 6 hours
#         else:
#             await redis.hdel(f"user_mode_override:{org_id}", user_id)


#     except Exception as e:
#         print(f"An error occurred while updating UserModeOverride for user {user_id} in org {org_id}: {e}")
    
#     # Broadcast the new state to all connected clients
#     #emit('response_state_update', {'user_id': user_id, 'state': state}, room=org_id, include_self=False)

#     try:
#         org_key = f"org:{org_id}:connections"
#         sids = await redis.smembers(org_key)  # returns set of bytes

#         other_sids = [s for s in sids if s != sid]
#         if other_sids:
#             await asyncio.gather(
#                 *(sio.emit("response_state_update_overallmanual", {"user_id": user_id, "state": state, "tabindex": tabindex, "timestamp": timestamp}, to=s)
#                 for s in other_sids),
#                 return_exceptions=True
#             )
        
#     except Exception as db_err:
#         print(f"Error fetching SIDs for org_id {org_id}: {db_err}")

  
  


#     #emit('response_state_update2', {'user_id': user_id, 'org_id': org_id, 'state': state}, to=chatbot_sid)
#     # PUBLISH TO REDIS INSTEAD OF EMIT TO SID
#     try:
#         await redis.publish(
#             "chatbot:user_state_update",
#             json.dumps(
#                 {
#                     "user_id": user_id,
#                     "org_id": org_id,
#                     "state": state,
#                     "timestamp": timestamp,
#                 }
#             ),
#         )
#     except Exception as redis_err:
#         print(f"Redis publish error: {redis_err}")










                                            # RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
                                            
                                            #       RECTANGLE RESPONSE - AUTOMATIC MODE       R

                                            # RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR





@sio.on("admin_response")
async def handle_admin_response(sid, data):
    print("jövünk az updatebe??? data: ", data)
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client

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
        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)

    except Exception as e:
        print(f"Error fetching connection for socket_id {sid}: {e}")
        return


    
    user_id = data['user_id']
    response = data['response']
    timestamp=data['timestamp']
    attachment = data.get("attachment")
    admin_name=data.get("admin_name")
    if attachment and not attachment.get("data"):
        attachment = None

    message_for_log={
        "admin_response": response,
        "user_id": user_id,
        "user_message": json.dumps({"text": "", "attachment": attachment}) if attachment else "",
        "userMessage" : "",
        "timestamp" : timestamp,
        "attachment": attachment,
        "admin_name": admin_name

    }

    asyncio.create_task(log_event(org_id, 'new_message', message_for_log))

    # Broadcast the response to all clients in the organization, excluding the sender
    #emit('response_update', {'user_id': user_id, 'response': response}, room=org_id, include_self=False)
    
    try:    # NOT SENDING TO ROOMS (because of REDIS) BUT TO SIDS DIRECTLY
        org_key = f"org:{org_id}:connections"
        sids = await redis.smembers(org_key)
        other_sids = [s for s in sids if s != sid]
        if other_sids:
            if attachment:
                await asyncio.gather(
                    *(sio.emit("response_update", {"user_id": user_id, "response": response, "user_message": json.dumps({"text": "", "attachment": attachment}) if attachment else "", "timestamp": timestamp, "attachment": attachment, "admin_name": admin_name}, to=s)
                        for s in other_sids),
                    return_exceptions=True
                )
            
            else:
                await asyncio.gather(
                    *(sio.emit("response_update", {"user_id": user_id, "response": response, "timestamp": timestamp, "attachment": attachment, "admin_name": admin_name}, to=s)
                        for s in other_sids),
                    return_exceptions=True
                )
    except Exception as db_err:
        print(f"Error fetching SIDs for org_id {org_id}: {db_err}")





# Handle input update event
@sio.on('update_colleagues_input')
async def handle_update_colleagues_input(sid, data):
    

    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
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

        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)


 
    
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
        sids = await redis.smembers(org_key)
        
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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client

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
        
        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)

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
        sids = await redis.smembers(org_key)
    
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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
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

        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)

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
            sids = await redis.smembers(org_key)
           
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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client

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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client

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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
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
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)
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
        target_sids = await redis.smembers(org_key)
      
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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client

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
        
        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)
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
            sids = await redis.smembers(org_key)
            
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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
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
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)
        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return
        
        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)
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
        sids = await redis.smembers(org_key)
     
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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)

    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client

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
        
        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)
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

    
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)

    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client
    thread_pool = fastapi_app.state.thread_pool
    thread_sem = fastapi_app.state.thread_sem
  

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return  # Stop further processing
    
    

    
    tabs = data.get('tabs', [])  # Get the list of tabs with name and uniqueId
    timestamp = data.get('frontend_time')


    

     # Retrieve connection info
    try:
        connection_key = f"connection:{sid}"
        connection_data = await redis.hgetall(connection_key)

        if not connection_data:
            print(f"[Redis] No connection found for socket_id {sid}")
            return

        org_id_str = connection_data.get("org_id")
        user_id_str = connection_data.get("user_id")

        if not org_id_str or not user_id_str:
            print(f"[Redis] Missing org_id or user_id for socket {sid}")
            return

        org_id = int(org_id_str)
        user_id = int(user_id_str)

    except Exception as e:
        print(f"Error handling createTabs: {e}")
        return

    redis_key = f"messages:{org_id}:batch_temp"
    total_key = f"messages_total:{org_id}:batch_temp"


    # Debug Redis batch
    try:
        current_length = await redis.llen(redis_key)
        if current_length > 0:
            existing_messages = await redis.lrange(redis_key, 0, -1)
            print(f"Redis batch not empty for org {org_id}: {current_length} messages")
            for msg in existing_messages:
                print(json.loads(msg))
        else:
            print(f"Redis batch empty for org {org_id}")
    except Exception as e:
        print(f"Error checking Redis at start for org {org_id}: {e}")



    if tabs:
        # Log the event with the complete tab data
        asyncio.create_task(log_event(org_id, 'tabs_created', {'tabs': tabs}, timestamp))
   
        # Update Client.last_manualmode_triggered_by
        try:
            async with async_session_scope() as session:
                result = await session.execute(select(Client).where(Client.id == org_id))
                client = result.scalars().first()
                if client:
                    client.last_manualmode_triggered_by = str(user_id)
        except Exception as e:
            print(f"Error updating client last_manualmode_triggered_by: {e}")
            return

        try:
            org_key = f"org:{org_id}:connections"
            sids = await redis.smembers(org_key)
            
            tasks = [sio.emit("createTabs", {"tabs": tabs}, to=s) for s in sids if s != sid]
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for idx, r in enumerate(results):
                    if isinstance(r, Exception):
                        print(f"Emit error to SID {sids[idx]}: {r}")
        except Exception as db_err:
            print(f"Error fetching SIDs for org {org_id}: {db_err}")


        #if org_modes[org] == 'manual':
        if await get_client_mode(org_id) == "manual":

            try:
                await redis.hset(
                    f"client:{org_id}:state",
                    mapping={
                        "last_manualmode_triggered_by": user_id
                    }
                )

            except Exception as e:
                print(f"Error updating user_mode_override with TTL for org {org_id}, user {user_id}: {e}")


            try:
                recent_messages = await get_recent_messages(org_id, minutes=15)
               
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

                messages = await run_cpu_task(build_unique_messages_and_sorted, recent_messages, cpu_pool=thread_pool, cpu_sem=thread_sem)

                org_connections_key = f"org:{org_id}:connections"

                # 1. Get all active socket IDs for this org
                sids = await redis.smembers(org_connections_key)
         
                admin_sid = None

                # 2. Iterate through each connection to find the one for this user
                for sid in sids:
                    connection_key = f"connection:{sid}"
                    connection_data = await redis.hgetall(connection_key)
                    if not connection_data:
                        continue

                    # Compare user_id
                    user_id_str = connection_data.get("user_id")  # key is now string
                    if user_id_str and int(user_id_str) == int(user_id):
                        admin_sid = sid
                        break  # found the matching user, stop here

                if not admin_sid:
                    print(f"No active Redis connection found for user_id={user_id} in org={org_id}")



                if admin_sid:
                    # Clear previous Redis batch
                    try:
                        if await redis.llen(redis_key) > 0:
                            await redis.delete(redis_key, total_key)
                            print(f"Cleared Redis batch keys for org {org_id}")
                    except Exception as e:
                        print(f"Error clearing Redis keys for org {org_id}: {e}")

                    await sio.emit("new_message_FirstUser", {"messages": messages}, to=admin_sid)
                else:
                    print(f"SID not found for admin user_id {user_id} in org {org_id}")

            except Exception as e:
                print(f"Error handling manual mode message batching for org {org_id}: {e}")


@sio.on("admin_response_manual")
async def handle_admin_response_manual(sid, data):
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client
    cpu_pool = fastapi_app.state.cpu_pool
    cpu_sem = fastapi_app.state.cpu_sem

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

        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)

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
        sids = await redis.smembers(org_key)
        
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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client

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

        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)
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
        sids = await redis.smembers(org_key)
        
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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client


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

        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)
    except Exception as e:
        print(f"Error handling clear_input_field (DB lookup): {e}")
        return


   
    
    timestamp = data.get('frontend_time') if data else None

    asyncio.create_task(log_event(org_id, 'clear_input_field', {}, timestamp))
    # Broadcast the event to all connected clients
    #emit('clear_input_field', room=org_id, include_self=False)

    try:
        org_key = f"org:{org_id}:connections"
        sids = await redis.smembers(org_key)
      
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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client


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

        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)
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
        sids = await redis.smembers(org_key)
    
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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client


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

        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)
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
            sids = await redis.smembers(org_key)
           
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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)

    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client


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

        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)

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
        sids = await redis.smembers(org_key)
       
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
    
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    environ = sio.get_environ(sid)
    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client
    cpu_pool = fastapi_app.state.cpu_pool
    cpu_sem = fastapi_app.state.cpu_sem

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

        org_id_str = connection_data.get("org_id")
        if not org_id_str:
            print(f"[Redis] Connection for socket_id {sid} has no org_id")
            return

        org_id = int(org_id_str)
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
        lock_value = str(uuid.uuid4())  # unique lock ID for this worker

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
                sids = await redis.smembers(org_key)
   

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
                    if stored_value and stored_value == lock_value:
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
    environ = sio.get_environ(sid)  # get environ first
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]
                    break

    environ = sio.get_environ(sid)

    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client


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
        org_id_str = connection_data.get("org_id")
        user_id_str = connection_data.get("user_id")

        if not org_id_str or not user_id_str:
            print(f"[Redis][tab_mode_change] Connection for SID {sid} is missing org_id or user_id")
            return

        org_id = int(org_id_str)
        user_id = int(user_id_str)
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
        sids = await redis.smembers(org_key)
  
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

    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
   
    redis = fastapi_app.state.redis_client


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


@sio.on("admin_typing_start")
async def handle_admin_typing_start(sid, data):
    print("Typing??? be??")
    # Get FastAPI app and Redis
    fastapi_app = sio.fastapi_app
    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        print("Redis not ready yet")
        await sio.disconnect(sid)
        return
    
    redis = fastapi_app.state.redis_client

    # Validate session
    environ = sio.get_environ(sid)
    cookies = environ.get("asgi.scope", {}).get("headers", [])
    session_id = None
    for key, value in cookies:
        if key == b'cookie':
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return

    user_id = data["user_id"]
    admin_name = data["admin_name"]
    
    # Get org_id from Redis (existing logic)
    connection_data = await redis.hgetall(f"connection:{sid}")
    org_id_str = connection_data.get("org_id")
    if not org_id_str:
        print(f"No org_id found for sid {sid}")
        return
    org_id = int(org_id_str)

    # Store typing in Redis with TTL (10s)
    await redis.set(f"typing:{org_id}:{user_id}", admin_name, ex=10)

    # Emit to all other SIDs in org
    org_sids = await redis.smembers(f"org:{org_id}:connections")
    for s in org_sids:
        if s != sid:
            await sio.emit("admin_typing_update", {
                "user_id": user_id,
                "admin_name": admin_name,
                "typing": True
            }, to=s)


# Keeps the typing lock alive while the admin is still typing.
@sio.on("admin_typing_ping")
async def handle_admin_typing_ping(sid, data):
    fastapi_app = sio.fastapi_app
    redis = fastapi_app.state.redis_client

    user_id = data["user_id"]
    admin_name = data["admin_name"]

    # Refresh TTL to keep slot alive
    connection_data = await redis.hgetall(f"connection:{sid}")
    org_id = int(connection_data.get("org_id", 0))
    if not org_id: return

    exists = await redis.exists(f"typing:{org_id}:{user_id}")
    if exists:
        await redis.set(f"typing:{org_id}:{user_id}", admin_name, ex=10)


@sio.on("admin_typing_stop")
async def handle_admin_typing_stop(sid, data):
    fastapi_app = sio.fastapi_app
    redis = fastapi_app.state.redis_client

    user_id = data["user_id"]
    admin_name = data["admin_name"]

    # Remove the typing lock
    connection_data = await redis.hgetall(f"connection:{sid}")
    org_id = int(connection_data.get("org_id", 0))
    if not org_id: return

    await redis.delete(f"typing:{org_id}:{user_id}")

    # Emit typing stopped to others
    org_sids = await redis.smembers(f"org:{org_id}:connections")
    for s in org_sids:
        if s != sid:
            await sio.emit("admin_typing_update", {
                "user_id": user_id,
                "admin_name": admin_name,
                "typing": False
            }, to=s)



@sio.on("human_support_request_state_change")
async def human_support_request_state_change(sid, data):

    environ = sio.get_environ(sid)
    cookies = environ.get("asgi.scope", {}).get("headers", [])

    session_id = None
    for key, value in cookies:
        if key == b"cookie":
            cookie_str = value.decode()
            for c in cookie_str.split(";"):
                if c.strip().startswith("session_id="):
                    session_id = c.strip().split("=")[1]

    fastapi_app = sio.fastapi_app

    if not fastapi_app or not getattr(fastapi_app.state, "redis_client", None):
        await sio.disconnect(sid)
        return

    redis = fastapi_app.state.redis_client

    # Validate session
    if not session_id or not await redis.exists(f"session:{session_id}"):
        await sio.emit("force_logout", {"reason": "Session expired"}, to=sid)
        await sio.disconnect(sid)
        return

    # Get org_id from connection
    try:
        connection_key = f"connection:{sid}"
        conn_data = await redis.hgetall(connection_key)

        if not conn_data:
            return

        org_id_str = conn_data.get("org_id")
        org_id = int(org_id_str) if org_id_str else 0

    except Exception as e:
        print(f"Connection lookup error: {e}")
        return

    user_id = data.get("user_id")
    state = data.get("state")

    try:
        key = f"user_mode_override:{org_id}"

        if state:
            await redis.hset(key, user_id, "manual")
            await redis.expire(key, 3600 * 6)
        else:
            await redis.hdel(key, user_id)

    except Exception as e:
        print(f"Error updating user_mode_override for org {org_id}, user {user_id}: {e}")