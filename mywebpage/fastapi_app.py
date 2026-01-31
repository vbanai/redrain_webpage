# from dotenv import load_dotenv
# from pathlib import Path
import os
# dotenv_path = Path(__file__).resolve().parent.parent / ".env"  # adjust depending on folder structure
# load_dotenv(dotenv_path)

# print("Loaded AZURE_STORAGE_CONNECTION_STRING:", os.environ.get("AZURE_STORAGE_CONNECTION_STRING"))

import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
from concurrent.futures import ProcessPoolExecutor
from redis.asyncio import from_url as redis_from_url
from starlette.middleware.sessions import SessionMiddleware
from mywebpage.redis_client import redis_url
from mywebpage.background import redis_listener, send_admin_heartbeat, cleanup_idle_sessions
from mywebpage.models_loader import load_models_bg
from mywebpage.socketio_app import sio
from socketio import ASGIApp
import secrets
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import mywebpage.security 
from pathlib import Path
from fastapi import Request
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi import Request, HTTPException




# --------------------- Lifespan ---------------------

# Everything before yield = executed on startup
# Everything after yield = executed on shutdown



@asynccontextmanager
async def lifespan(app: FastAPI):
    print("LIFESPAN CALLED, app id:", id(app))
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
    #redis_from_url (from redis.asyncio) returns a lazy client — it doesn’t connect immediately.
    #If a request hits before the first Redis command, the dependency may fail, or your background tasks may not work.
    try:
        redis_client = redis_from_url(
            redis_url,
            decode_responses=True,
            socket_keepalive=True,
            retry_on_timeout=True,
        )
        await redis_client.ping()  # A lazy loading miatt we force the connection before any request hit
        app.state.redis_client = redis_client
        print("Connected to Redis")
    except Exception as e:
        print(f"Redis connection failed at startup: {e}")
        app.state.redis_client = None

    # Event to signal that models are ready
    app.state.models_loaded_event = asyncio.Event()
    
    # === Load topic classifier in background ===
   
    asyncio.create_task(load_models_bg(app))

    # Supervisor task
    async def supervisor(coro_fn, name: str, restart_delay: float = 5.0):
        print(f"Supervisor {name} starting")
        try:
            while True:
                try:
                    await coro_fn(app)
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
                asyncio.create_task(supervisor(cleanup_idle_sessions, "idle_logout")),
            ]
            print("Background tasks started")
        except Exception as e:
            print(f"Failed to start background tasks: {e}")
            for t in app.state.bg_tasks:
                t.cancel()
    else:
        print("Skipping background tasks (Redis unavailable)")

 
    # yield to app  Everything before yield runs once, when the FastAPI app starts.
    try:
        yield  # Control goes to FastAPI. FastAPI now handles requests normally.
    # Your routes, middleware, background tasks all work. During this time, your context manager is paused at the yield.
    
    finally:
        # SHUTDOWN Everything after yield runs once, when the FastAPI app stops.
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
        pool = getattr(app.state, "cpu_pool", None)
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
            finally:
                delattr(app.state, "cpu_pool")
        # Close Redis
        redis_client = getattr(app.state, "redis_client", None)
        if redis_client:
            try:
                await redis_client.close()
                if hasattr(redis_client, "wait_closed"):
                    await redis_client.wait_closed()
                print("Redis connection closed")
            except Exception as e:
                print(f"Error closing Redis: {e}")
            finally:
                delattr(app.state, "redis_client")

        # Semaphore
        if hasattr(app.state, "cpu_sem"):
            delattr(app.state, "cpu_sem")

        print("Shutdown complete")

# --------------------- FastAPI app ---------------------


SECRET_KEY = os.environ.get("SECRET_KEY") or secrets.token_hex(16)

# FastAPI’s lifespan parameter takes an async context manager:
fastapi_app = FastAPI(lifespan=lifespan) # with this fastapi_app is defined at the module level, any code in that module can access it:
# This ensures:
# Startup code runs before any request is handled
# Cleanup code runs when the app stops
# Don’t have to manually manage connections or background tasks



# FastAPI catches the HTTPException raised by login_required
# If 401 and request is GET, user is redirected to:
@fastapi_app.exception_handler(HTTPException)
async def auth_exception_handler(request: Request, exc: HTTPException):
    if exc.status_code == 401 and request.method == "GET":
        return RedirectResponse(
            "/logout?reason=expired",
            status_code=302
        )

    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )

sio_app = ASGIApp(sio, other_asgi_app=fastapi_app)
# fastapi_app.include_router(my_router) adds all the routes defined in a router (APIRouter) to the main FastAPI app. Without it  FastAPI doesn’t know about

# Event to signal that models are ready
# fastapi_app.state.models_loaded_event = asyncio.Event()


fastapi_app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)  # Looks for a session cookie in the request. If none exists, it creates a new session dictionary and sends a cookie back.
# This tells FastAPI/Starlette: “Create a session for the user if it doesn’t exist yet, and store it in a cookie called session.”
# server can then do
# session_id = request.session.get("session_id")


# CORS It controls which frontend domains can call your backend.
fastapi_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # adjust as needed
    allow_credentials=True,  # Yes, it’s okay to send cookies (like session_id) along with cross-origin requests.”
    allow_methods=["*"],
    allow_headers=["*"]
)




BASE_DIR = Path(__file__).resolve().parent # C:\Users\vbanai\Documents\Programming\Dezsi porject\ChatFrontEnd\FinalAdminPage_FASTAPI\mywebpage\static


# When someone requests /static/anything,
# serve files from mywebpage/static/:   mount =attach a sub-application
fastapi_app.mount(
    "/static",
    StaticFiles(directory=BASE_DIR / "static"),
    name="static",
)

# {{ url_for("static", filename="style.css") }}
# Means:

# Ask the FastAPI app:
# do you have a route named static?  “Static” means: Files that are sent as-is, without Python logic: css, js, images, fonts   route statc mean holding static files “For every HTTP request whose path starts with /static, serve files from the filesystem folder BASE_DIR/static.”
# If yes, build the correct URL.”
# /static becomes an endpoint, but not like  routes in routes.py.
# It is a mounted ASGI app, not a normal route function.
# endpoints in FastAPI:
# Type A — Function-based routes (what is in routes.py), they are registered in the router
# Type B — Mounted applications (what /static is) This does not create a function, 
# but hands the request over to another ASGI app.
# A request is simply: One message sent by the browser to my server asking for something.
# if I type for example: http://127.0.0.1:8000/ browser sends ONE HTTP request: GET /  Host: 127.0.0.1:8000 this goes to Uvicorn listening on 127.0.0.1:8000   
# when fastapi receives: GET /  it does: check mounted apps: fastapi_app.mount("/static", ...) Does "/" start with "/static"? no , so skip then check routes @router.get("/")  match found

#  REQUEST HANDLING IN DETAIL

# first what is  request:  
# it’s not the raw network request, It’s an object with all headers, URL, query parameters, cookies, plus a reference to the app (request.app).
# That’s why we can do: request.app.state.redis_client
# uvicorn receives it and calls the top ASGI app (Engine.IO wrapper).
# The Engine.IO wrapper decides:
# Is this a Socket.IO handshake? → handle it here
# Is it a normal HTTP request? → forward to other_asgi_app (your FastAPI app)
# If it’s forwarded, then request in FastAPI is a Python object representing that HTTP request.
# ASGI: the standard “language” ,"protocol" that async Python async web servers speak
# UVICORN listens on HTTP, converts the request into an ASGI “event”, and calls your FastAPI app.
# if I write @app.get("/") async def home() fastapi turns this into sg ASGI understand  so uvicorn can call it
# Engine.IO is Low-level WebSocket + long-polling protocol, Used by Socket.IO, which is a higher-level library for realtime events.
# Middleware: sits between the server and your endpoint. pl session middleware It checks for a session cookie, and makes request.session available. vagy CORS middleware → allows cross-domain requests vagy Authentication middleware → checks if a user is logged in
# Templates:  HTML files with placeholders (variables or code) that are filled dynamically. Jinja2 is the template engine FastAPI uses by default. <h1>Hello {{ user_name }}</h1>

# FASTAPI 1.) checks mounted apps (StaticFiles(...)) but skip start handling
#         2.) checks routes: @router.get("/") async def index(...) Match found index() is called 
#          3.) response is sent  : index.html is turned into HTML text, and send it to the browser 
#         4.) browser parses (reads and understanding and discovering the HTML) the HTML <link rel="stylesheet" href="/static/style.css"> etc.
#         5.) for every URL it finds the browser sends a new request   
#            /static/style.css   ->   GET /static/style.css   
#           /static/logo.png -> GET /static/logo.png
#           /login (only if clicked) ->  GET /login
#           /static/style.css is automatic CSS is required to render the page This again goes to Uvicorn → FastAPI
#          6.) FastAPI again checks in order: Step 1 — Mounted apps   
#               Does /static/style.css start with /static?   YES    Forward request to StaticFiles

# Include your routes AFTER mounting static
from mywebpage.routes import router

# here we Attach the router box defined on routes.py to the app
fastapi_app.include_router(router)


