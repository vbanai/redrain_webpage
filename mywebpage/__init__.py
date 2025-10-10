import os
import secrets
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi_csrf_protect import CsrfProtect
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import OperationalError
import asyncio
import socketio
import redis.asyncio as aioredis  # Async Redis client
from pydantic import BaseModel
from itsdangerous import URLSafeTimedSerializer
from contextlib import asynccontextmanager
from mywebpage import redis_listener, send_admin_heartbeat
from concurrent.futures import ProcessPoolExecutor
from redis.asyncio import from_url as redis_from_url


# --------------------- Redis Async ---------------------

redis_password=os.environ.get("redis_password")
redis_host = "aichatbotredis111.redis.cache.windows.net"
redis_port = 6380

redis_url = f"rediss://default:{redis_password}@{redis_host}:{redis_port}"







# --------------------- Lifespan ---------------------

from contextlib import asynccontextmanager
import asyncio
from concurrent.futures import ProcessPoolExecutor

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup/shutdown lifecycle:
      - create Redis client
      - create ProcessPoolExecutor + Semaphore
      - start background supervisors
      - on shutdown: cancel bg tasks, wait, shutdown pool, close Redis
    """
    # STARTUP
    app.state.redis_client = None
    app.state.cpu_pool = None
    app.state.cpu_sem = None
    app.state.bg_tasks = []

    # Process pool + semaphore
    try:
        app.state.cpu_pool = ProcessPoolExecutor(max_workers=2)
        app.state.cpu_sem = asyncio.Semaphore(2)
        print("Process pool + semaphore created")
    except Exception as e:
        print(f"Failed to create CPU pool/semaphore: {e}")

    # Redis
    try:
        app.state.redis_client = redis_from_url(
            redis_url,
            decode_responses=True,
            socket_keepalive=True,
            retry_on_timeout=True,
        )
        print("Connected to Redis")
    except Exception as e:
        print(f"Redis connection failed at startup: {e}")
        app.state.redis_client = None

    # Supervisor task
    async def supervisor(coro_fn, name: str, restart_delay: float = 5.0):
        print(f"Supervisor {name} starting")
        try:
            while True:
                try:
                    await coro_fn()
                    await asyncio.sleep(0.1)
                except asyncio.CancelledError:
                    print(f"Supervisor {name} cancelled")
                    raise
                except Exception as e:
                    print(f"Supervisor {name} crashed: {e} — restarting in {restart_delay}s")
                    await asyncio.sleep(restart_delay)
        finally:
            print(f"Supervisor {name} exiting")

    # Start background tasks
    if app.state.redis_client:
        try:
            app.state.bg_tasks = [
                asyncio.create_task(supervisor(redis_listener, "redis_listener")),
                asyncio.create_task(supervisor(send_admin_heartbeat, "admin_heartbeat")),
            ]
            print("Background tasks started")
        except Exception as e:
            print(f"Failed to start background tasks: {e}")
            for t in app.state.bg_tasks:
                t.cancel()
    else:
        print("Skipping background tasks (Redis unavailable)")

    # yield to app
    try:
        yield
    finally:
        # SHUTDOWN
        print("Lifespan shutdown initiated")

        # Cancel background tasks
        tasks = getattr(app.state, "bg_tasks", [])

        if tasks:
            for t in tasks:
                t.cancel()
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                print(f"Error while awaiting background tasks: {e}")

        # Shutdown pool
        pool = app.state.pop("cpu_pool", None)
        if pool:
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, pool.shutdown, True)
                print("CPU pool shut down")
            except Exception as e:
                print(f"Error shutting down CPU pool: {e}")
                try:
                    pool.shutdown(wait=False)
                except Exception as e2:
                    print(f"Force shutdown of CPU pool failed: {e2}")

        # Close Redis
        redis_client = app.state.pop("redis_client", None)
        if redis_client:
            try:
                await redis_client.close()
                if hasattr(redis_client, "wait_closed"):
                    await redis_client.wait_closed()
                print("Redis connection closed")
            except Exception as e:
                print(f"Error closing Redis: {e}")

        app.state.pop("cpu_sem", None)

        print("Shutdown complete")

# --------------------- FastAPI app ---------------------


SECRET_KEY = os.environ.get("SECRET_KEY") or secrets.token_hex(16)

fastapi_app = FastAPI(lifespan=lifespan)

fastapi_app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)  # Looks for a session cookie in the request. If none exists, it creates a new session dictionary and sends a cookie back.
# this is the way how it does, as session not see create it: session_id = request.session.get("session_id")
#     if not session_id:
#         session_id = secrets.token_urlsafe(16)
#         request.session['session_id'] = session_id

# CORS It controls which frontend domains can call your backend.
fastapi_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # adjust as needed
    allow_credentials=True,  # Yes, it’s okay to send cookies (like session_id) along with cross-origin requests.”
    allow_methods=["*"],
    allow_headers=["*"]
)


templates = Jinja2Templates(directory="templates")

fastapi_app.mount("/static", StaticFiles(directory="static"), name="static")





# --------------------- Socket.IO --------------------

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    message_queue=redis_url   
  )

app = socketio.ASGIApp(sio, other_asgi_app=fastapi_app)
# when deploying it to azure use app:app

# --------------------- CSRF / Security ---------------------


# ---- CSRF settings ----
class CsrfSettings(BaseModel):
    secret_key: str = SECRET_KEY

#CsrfProtect.load_config registers a global configuration inside the fastapi_csrf_protect library.
# It doesn’t matter where you call Depends(CsrfProtect) later — the library remembers the get_csrf_config function you registered.
# When FastAPI sees Depends(CsrfProtect), it asks the fastapi_csrf_protect library: “Create a CsrfProtect instance for me.”
# fastapi_csrf_protect internally calls the registered get_csrf_config() to get the secret key. Now csrf_protect is a fully configured instance, ready to generate or validate tokens.

@CsrfProtect.load_config
def get_csrf_config():
    return CsrfSettings()


#-------------------------------


#########################################
# POSTGRESQL
#########################################

host = os.environ.get("HOST_POSTGRESQL")
database = 'postgres'
username = os.environ.get("username_POSTGRESQL")
password = os.environ.get("password_POSTGRESQL")
port = "5432"

DATABASE_URI = f"postgresql+asyncpg://{username}:{password}@{host}:{port}/{database}"

# Async SQLAlchemy engine
async_engine = create_async_engine(
    DATABASE_URI,
    pool_size=10,
    max_overflow=5,
    pool_pre_ping=True,
    pool_recycle=1800,
    echo=False
)

# Async session factory
AsyncSessionLocal = sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

Base = declarative_base()

# Async session context manager
@asynccontextmanager
async def async_session_scope():
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception as e:
            await session.rollback()
            print(f"[AsyncSession] Rollback due to error: {e}")
            raise


# Export session, session_scope, and models for use in other modules
# Ez nem fontos, csak akkor ha ezt akarom használni: from mywebpage import *
# ilyenkor automatikusan elérem a neveket egyszerűen más modulokban, de a hagyomás import .. from mywebpage működik mindenhol, ha csak ennyit írok import mywebpage akkor így tudom mywebpage.sio
__all__ = [
    "fastapi_app",  # FastAPI app for endpoints
    "app",          # ASGI app for Socket.IO
    "sio",          # Socket.IO server
    "templates",
    "async_session_scope",
    "async_engine",
    "Base",
    "AsyncSessionLocal",
]







