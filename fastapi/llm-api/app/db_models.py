import uuid
from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel, Column, TIMESTAMP, text
from sqlmodel import create_engine

from app.config import config

engine = create_engine(config.database_uri)


class APIKeys(SQLModel, table=True):
    __tablename__ = "api_keys"
    __table_args__ = {"schema": 'llm_api', "comment": "API токены для сервиса"}

    id: Optional[uuid.UUID] = Field(default_factory=uuid.uuid4, primary_key=True)
    token_name: str = Field(nullable=False)
    token_value: bytes = Field(nullable=False)
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)


class ChatGPTLog(SQLModel, table=True):
    __tablename__ = "log_prompts_api"
    __table_args__ = {"schema": "llm_api", "comment": "Логи запросов к ChatGPT через API"}

    date: Optional[datetime] = Field(sa_column=Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        primary_key=True
    ))
    prompt: str = Field(description="Отзыв")
    response: str = Field(description='Ответ ChatGPT')
    input_tokens: int = Field()
    output_tokens: int = Field()
    model: str = Field(description="Модель GPT")
    system_prompt: str = Field(description="Системный промпт")
    api_key: bytes = Field(description="")
