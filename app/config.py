from pathlib import Path

from pydantic_settings import BaseSettings
from functools import lru_cache

# Project root: Sistema-Dashboard/
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "postgresql://inei_admin:inei_dashboard_2026@localhost:5432/dashboard_inei"

    # JWT
    JWT_SECRET: str = "change-this-secret-in-production"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRATION_MINUTES: int = 480  # 8 hours

    # App
    APP_NAME: str = "Dashboard INEI"
    DEBUG: bool = True
    API_PREFIX: str = "/api"

    # CORS
    CORS_ORIGINS: list[str] = ["http://localhost:3000", "http://localhost:3001", "http://localhost:3002", "http://localhost:3003", "http://localhost:80"]

    # File storage
    FORMATOS_DIR: Path = _PROJECT_ROOT / "formatos"
    PLANTILLAS_DIR: Path = _PROJECT_ROOT / "formatos" / "plantillas"
    UPLOADS_DIR: Path = _PROJECT_ROOT / "formatos" / "uploads"

    model_config = {"env_file": ".env", "extra": "ignore"}


@lru_cache
def get_settings() -> Settings:
    return Settings()
