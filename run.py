import os
import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))

    uvicorn.run(
        "mywebpage:app",   # points to the app object inside main.py
        host="0.0.0.0",
        port=port,
        log_level="info",
        reload=False  # reload=True only for local dev
    )