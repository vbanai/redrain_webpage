import os
import secrets
from pydantic import BaseModel
from fastapi_csrf_protect import CsrfProtect


# --------------------- CSRF / Security ---------------------


SECRET_KEY = os.environ.get("SECRET_KEY") or secrets.token_hex(16)

class CsrfSettings(BaseModel):
    secret_key: str = SECRET_KEY

#CsrfProtect.load_config registers a global configuration inside the fastapi_csrf_protect library.
# It doesn’t matter where you call Depends(CsrfProtect) later — the library remembers the get_csrf_config function you registered.
# When FastAPI sees Depends(CsrfProtect), it asks the fastapi_csrf_protect library: “Create a CsrfProtect instance for me.”
# fastapi_csrf_protect internally calls the registered get_csrf_config() to get the secret key. Now csrf_protect is a fully configured instance, ready to generate or validate tokens.


@CsrfProtect.load_config
def get_csrf_config():
    return CsrfSettings()

