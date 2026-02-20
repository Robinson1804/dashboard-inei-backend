"""
Pydantic v2 schemas for the authentication endpoints.

Covers the login request payload, the JWT token response, and the
public user representation returned by ``GET /api/auth/me``.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, EmailStr, Field

from app.utils.constants import ROLES


class LoginRequest(BaseModel):
    """Payload accepted by ``POST /api/auth/login``.

    Attributes:
        username: The user's unique login name.
        password: Plain-text password (transmitted over HTTPS only).
    """

    username: str = Field(
        ...,
        min_length=3,
        max_length=50,
        description="Nombre de usuario único del sistema",
    )
    password: str = Field(
        ...,
        min_length=6,
        max_length=128,
        description="Contraseña en texto plano (solo sobre HTTPS)",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "username": "jperez",
                "password": "secret1234",
            }
        }
    )


class TokenResponse(BaseModel):
    """Response body returned after a successful authentication.

    Attributes:
        access_token: Signed JWT string to be sent in the
                      ``Authorization: Bearer <token>`` header.
        token_type: Always ``"bearer"`` per OAuth2 convention.
    """

    access_token: str = Field(..., description="JWT de acceso firmado con HS256")
    token_type: str = Field(default="bearer", description="Tipo de token OAuth2")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "token_type": "bearer",
            }
        }
    )


class UserResponse(BaseModel):
    """Public representation of an authenticated user.

    Returned by ``GET /api/auth/me`` and embedded in other responses
    that reference user identity.  Sensitive fields (``password_hash``,
    ``fecha_creacion``) are deliberately excluded.

    Attributes:
        id: Database primary key.
        username: Unique login name.
        email: Email address on record.
        nombre_completo: Full display name.
        rol: Role code; one of ``constants.ROLES``.
        ue_id: Organisational unit the user is restricted to, or
               ``None`` if the user has system-wide access.
        activo: Whether the account is currently active.
    """

    id: int
    username: str
    email: str
    nombre_completo: str
    rol: str
    ue_id: int | None
    activo: bool

    # Enable ORM-mode so FastAPI can serialise SQLAlchemy model instances
    # directly without manual dict conversion.
    model_config = ConfigDict(from_attributes=True)
