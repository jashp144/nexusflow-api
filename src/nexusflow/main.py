"""ASGI entry point for running NexusFlow microservice via uvicorn."""

from nexusflow.api.app import create_app

app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("nexusflow.main:app", host="0.0.0.0", port=8000, reload=True)
