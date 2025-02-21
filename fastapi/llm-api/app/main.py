from fastapi import FastAPI

from app.api.v1 import chatgpt, api_keys

app = FastAPI(
    title="LLM API",
    description="API для взаимодействия с языковыми моделями",
    version="1.0.0",
    docs_url="/",
    redoc_url=None,
)

app.include_router(chatgpt.router, prefix="/v1")
app.include_router(api_keys.router, prefix="/v1")
