import datetime
import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select

from app.auth.utils import authorize_admin_ldap
from app.db_models import engine, APIKeys
from app.models import CreateApiToken, ApiTokenOutput, ListApiTokens, DeleteApiToken, ApiToken
from app.support import generate_api_token

router = APIRouter()


@router.post("/api-keys/list", response_model=ListApiTokens, tags=["admin"])
async def get_api_keys(username: str = Depends(authorize_admin_ldap)):
    with Session(engine) as session:
        statement = select(APIKeys)
        results = session.exec(statement).all()
    api_tokens = [
        ApiToken(
            token_id=token.id,
            token_name=token.token_name,
            token_value=token.token_value.decode("utf-8"),
            created_at=token.created_at,
        )
        for token in results
    ]
    return ListApiTokens(result=api_tokens)


@router.post("/api-keys/create", response_model=ApiTokenOutput, tags=["admin"])
async def create_api_key(name: CreateApiToken, username: str = Depends(authorize_admin_ldap)):
    token_value = generate_api_token()
    token_id = uuid.uuid4()
    row = APIKeys(token_name=name.token_name, token_value=token_value, id=token_id)
    with Session(engine) as session:
        session.add(row)
        session.commit()

    return ApiTokenOutput(
        result=ApiToken(
            token_name=name.token_name,
            token_id=token_id,
            token_value=token_value.decode("utf-8"),
            created_at=datetime.datetime.now(),
        ),
        message="Successfully created",
    )


@router.post("/api-keys/delete", response_model=ApiTokenOutput, tags=["admin"])
async def delete_api_key(token: DeleteApiToken, username: str = Depends(authorize_admin_ldap)):
    with Session(engine) as session:
        statement = select(APIKeys).where(APIKeys.id == token.token_id)
        result = session.exec(statement).one_or_none()

        if result is None:
            raise HTTPException(
                status_code=404, detail=f"API key with ID {token.token_id} not found."
            )
        session.delete(result)
        session.commit()

    return ApiTokenOutput(
        result=ApiToken(
            token_name=result.token_name,
            token_id=result.id,
            token_value=result.token_value.decode("utf-8"),
            created_at=result.created_at,
        ),
        message="Successfully deleted",
    )
