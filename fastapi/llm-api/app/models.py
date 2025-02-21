import uuid
from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel
from sqlmodel import Field


class ReviewRequest(BaseModel):
    reviews: List[str]

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "reviews": [
                        "Есть из чего выбирать, консультант посоветовал кучу моделей телеков, "
                        "взял самый лучший по цене/качеству, одобряю.",
                        "Вообще обнаглели, чтобы поменять стекло на телефоне просят 5000!",
                    ]
                }
            ]
        }
    }


class AttributeResult(BaseModel):
    attribute: str
    tonality: str


class ReviewResult(BaseModel):
    review: str
    attributes: List[AttributeResult]
    classify: str


class ReviewResponse(BaseModel):
    result: List[ReviewResult]


class CreateApiToken(BaseModel):
    token_name: str = Field()

    model_config = {"json_schema_extra": {"examples": [{"token_name": "Тест"}]}}


class ApiToken(BaseModel):
    token_name: str = Field(nullable=False)
    token_id: Optional[uuid.UUID] = Field()
    token_value: bytes = Field(nullable=False)
    created_at: datetime = Field()


class ListApiTokens(BaseModel):
    result: List[ApiToken]


class ApiTokenOutput(BaseModel):
    result: ApiToken
    message: str = Field(nullable=False)


class DeleteApiToken(BaseModel):
    token_id: Optional[uuid.UUID] = Field()
    model_config = {"json_schema_extra": {"examples": [{"token_id": "UUID"}]}}


class PromptsRequest(BaseModel):
    prompts: List[str]


class PromptResult(BaseModel):
    prompt: str
    response: str


class PromptsResponse(BaseModel):
    result: List[PromptResult]
