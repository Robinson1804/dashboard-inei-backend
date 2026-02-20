"""
Authentication router for the Dashboard INEI API.

Mounts under ``/api/auth`` (prefix set in ``main.py``).

Endpoints:
    POST /login   — Authenticate with username + password, receive JWT.
    POST /refresh — Exchange a valid token for a new one (extend session).
    GET  /me      — Return the currently authenticated user's profile.
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.usuario import Usuario
from app.schemas.auth import LoginRequest, TokenResponse, UserResponse
from app.services.auth_service import authenticate_user, get_current_user
from app.utils.security import create_access_token

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Auth"])


# ---------------------------------------------------------------------------
# POST /login
# ---------------------------------------------------------------------------


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Iniciar sesión",
    description=(
        "Autentica al usuario con sus credenciales y retorna un JWT de acceso "
        "válido por el tiempo configurado en ``JWT_EXPIRATION_MINUTES`` (default 8 h)."
    ),
    responses={
        200: {"description": "Autenticación exitosa; se incluye el token JWT."},
        401: {"description": "Credenciales incorrectas o cuenta inactiva."},
        422: {"description": "Cuerpo de la solicitud inválido."},
    },
)
def login(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    db: Annotated[Session, Depends(get_db)],
) -> TokenResponse:
    """Authenticate a user and issue a JWT access token.

    Accepts both the standard OAuth2 ``application/x-www-form-urlencoded``
    form (required by the ``OAuth2PasswordRequestForm`` dependency, which
    enables the Swagger UI "Authorize" button) and exposes a clean JSON
    body schema via the ``LoginRequest`` model documented in the OpenAPI spec.

    Args:
        form_data: Username and password submitted as an OAuth2 form.
        db: Database session injected by ``get_db``.

    Returns:
        A ``TokenResponse`` containing the signed JWT and token type.

    Raises:
        HTTPException 401: If credentials are invalid or the account is inactive.
    """
    user = authenticate_user(db, form_data.username, form_data.password)

    if user is None:
        logger.warning("Failed login attempt for username='%s'", form_data.username)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales incorrectas o cuenta inactiva",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = create_access_token(
        data={
            "sub": str(user.id),
            "username": user.username,
            "rol": user.rol,
            "ue_id": user.ue_id,
        }
    )

    logger.info("Successful login for username='%s' rol='%s'", user.username, user.rol)
    return TokenResponse(access_token=token)


# ---------------------------------------------------------------------------
# POST /refresh
# ---------------------------------------------------------------------------


@router.post(
    "/refresh",
    response_model=TokenResponse,
    summary="Renovar token",
    description=(
        "Emite un nuevo JWT a partir de un token válido (no expirado). "
        "Permite extender la sesión sin re-autenticación."
    ),
    responses={
        200: {"description": "Token renovado exitosamente."},
        401: {"description": "Token inválido o expirado."},
    },
)
def refresh_token(
    current_user: Annotated[Usuario, Depends(get_current_user)],
) -> TokenResponse:
    """Refresh an access token for the currently authenticated user.

    Requires a valid (non-expired) token in the ``Authorization: Bearer``
    header.  Returns a new token with a fresh expiration window.

    Args:
        current_user: Resolved by the ``get_current_user`` dependency;
                      raises HTTP 401 automatically if the token is invalid.

    Returns:
        A new ``TokenResponse`` with a freshly signed JWT.
    """
    new_token = create_access_token(
        data={
            "sub": str(current_user.id),
            "username": current_user.username,
            "rol": current_user.rol,
            "ue_id": current_user.ue_id,
        }
    )
    logger.info("Token refreshed for username='%s'", current_user.username)
    return TokenResponse(access_token=new_token)


# ---------------------------------------------------------------------------
# GET /me
# ---------------------------------------------------------------------------


@router.get(
    "/me",
    response_model=UserResponse,
    summary="Perfil del usuario autenticado",
    description=(
        "Retorna el perfil público del usuario identificado por el JWT "
        "en la cabecera ``Authorization: Bearer <token>``."
    ),
    responses={
        200: {"description": "Perfil del usuario autenticado."},
        401: {"description": "Token ausente, inválido o expirado."},
    },
)
def get_me(
    current_user: Annotated[Usuario, Depends(get_current_user)],
) -> UserResponse:
    """Return the authenticated user's public profile.

    Args:
        current_user: Resolved by the ``get_current_user`` dependency.

    Returns:
        A ``UserResponse`` instance populated from the ORM model.
    """
    return UserResponse.model_validate(current_user)
