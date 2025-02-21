from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    debug: bool = False
    database_uri: str

    admin_mails: list[str] = ["Tretyakov.AA@mail.ru"]
    ldap_host: str = "partner.ru"
    ldap_base_dn: str = "OU=DNS Users,DC=partner,DC=ru"
    ldap_username: str
    ldap_password: str
    admin_groups: list[str] = ["IT-группа Инженеры данных"]

    mail_host: str = "mail.ru"
    mail_from: str = "notification.bi@mail.ru"

    openai_api_key: str
    openai_proxy_url: str
    openai_chat_model: str = "gpt-4o-mini"
    openai_max_messages: int = 20
    openai_max_message_length: int = 4000

    model_config = SettingsConfigDict(case_sensitive=False)


config = Settings()
