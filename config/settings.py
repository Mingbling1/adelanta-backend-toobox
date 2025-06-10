from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    MONGO_USERNAME: str
    MONGO_PASSWORD: str | None = None
    MONGO_HOST: str | None = None
    ALGORITHM: str | None = None
    ACCESS_TOKEN_EXPIRE_MINUTES: float | None = None
    SECRET_KEY: str | None = None
    TOKEN_SUNAT: str | None = None
    URL_API_RUC: str | None = None
    URL_API_DNI: str | None = None
    API_KEY_OPEN_AI: str | None = None
    ASSISTANT_ID: str | None = None
    URL_API_META: str
    ID_PHONE_API_META: str
    TOKEN_API_META: str
    DATABASE_MYSQL_URL: str
    DATABASE_MYSQL_URL_ADMINISTRATIVO: str
    DATABASE_MYSQL_URL_CRM: str
    LOG_LEVEL: str
    TOKEN_SUNAT: str
    GOOGLE_CLIENT_ID: str
    GOOGLE_CLIENT_SECRET: str
    GOOGLE_REDIRECT_URI: str
    GOOGLE_REFRESH_TOKEN: str
    COOKIE_NAME: str
    FRONTEND_DOMAIN: str
    RESEND_API_KEY: str
    REDIS_HOST: str
    REDIS_URL: str

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
