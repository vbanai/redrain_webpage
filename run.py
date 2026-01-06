import os
import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))

    uvicorn.run(
        "mywebpage:asgi_app.app",   # Uvicorn, please import the Python module named mywebpage, then look inside it for a variable called app.
        # python looking for mywebpage.py or mywebpage folder with__init__.py which tells it is a package so python does: import mywebpage which means executes mywebpage/__init__.py
        # after importing python asks does mywebpage contain an attribute named app?
        # we can define app in init app=... or import it from .app import app
        host="127.0.0.1", # prodnál ezt használtamhost="0.0.0.0",
        port=port,
        log_level="info",
        reload=True  # reload=True only for local dev
    )


#indítás dev mode: uvicorn mywebpage.asgi_app:app --host 127.0.0.1 --port 8001 --reload