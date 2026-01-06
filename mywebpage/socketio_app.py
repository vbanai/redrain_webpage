# mywebpage/socketio_app.py
import socketio
from mywebpage.redis_client import redis_url

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    message_queue=redis_url
)
