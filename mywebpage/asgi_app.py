# mywebpage/asgi_app.py
from mywebpage.fastapi_app import fastapi_app
from mywebpage.socketio_app import sio
import socketio

# Create the ASGI app that combines FastAPI + Socket.IO
app = socketio.ASGIApp(sio, other_asgi_app=fastapi_app)
