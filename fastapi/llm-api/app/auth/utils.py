from fastapi import HTTPException, status, Security, Depends
from fastapi.security import HTTPBasicCredentials, HTTPBasic, api_key
from sqlmodel import Session
from sqlmodel import select

from app.auth import LDAP
from app.config import config
from app.db_models import engine, APIKeys

ldap = LDAP(
    host=config.ldap_host,
    username=config.ldap_username,
    password=config.ldap_password,
    base_dn=config.ldap_base_dn,
    search_filter="(&(objectclass=Person)(|(sAMAccountName={user})(mailNickname={user})))",
    fields=[
        "cn",
        "sn",
        "givenName",
        "memberOf",
        "sAMAccountName",
        "displayName",
        "mail",
        "department",
        "title",
    ],
)
ldap_security = HTTPBasic()
api_key_header_security = api_key.APIKeyHeader(name="X-API-KEY")


def authorize_api_key(api_key_header: str = Security(api_key_header_security)) -> str:
    with Session(engine) as session:
        statement = select(APIKeys)
        results = session.exec(statement).all()
        if api_key_header not in {token.token_value.decode("utf-8") for token in results}:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing or invalid API key",
            )
        session.commit()
        return api_key_header


def authorize_admin_ldap(credentials: HTTPBasicCredentials = Depends(ldap_security)) -> None:
    if not credentials.username or not credentials.password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header missing",
            headers={"WWW-Authenticate": "Basic"},
        )
    if not ldap.bind_user(credentials.username, credentials.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )
    user_info = ldap.get_user_info(credentials.username)
    groups_user = user_info.get("groups", {})
    if not set(config.admin_groups) & set(groups_user):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not enough privileges",
        )
    return user_info
