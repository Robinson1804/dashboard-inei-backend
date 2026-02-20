"""
Security utilities for the Dashboard INEI authentication system.

Provides JWT token creation/verification and bcrypt password hashing
via python-jose and passlib respectively. All configuration is sourced
from the application settings singleton so that secrets are never
hard-coded in source files.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import bcrypt
from jose import JWTError, jwt

from app.config import get_settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Password helpers (bcrypt direct — passlib has compatibility issues with
# bcrypt 4.x on Python 3.13)
# ---------------------------------------------------------------------------


def hash_password(password: str) -> str:
    pwd_bytes = password.encode("utf-8")
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(pwd_bytes, salt).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------


def create_access_token(data: dict[str, Any]) -> str:
    """Create a signed JWT access token.

    The token payload is a copy of *data* augmented with an ``exp``
    (expiration) claim computed from ``JWT_EXPIRATION_MINUTES`` in
    settings.  The ``sub`` (subject) claim should be set by the caller
    (typically ``str(user.id)`` or ``user.username``).

    Args:
        data: Arbitrary claims to embed in the token payload.
              Must not contain ``exp`` — that claim is set here.

    Returns:
        A compact, URL-safe JWT string signed with HS256.

    Example::

        token = create_access_token({"sub": str(user.id), "rol": user.rol})
    """
    settings = get_settings()
    payload = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(
        minutes=settings.JWT_EXPIRATION_MINUTES
    )
    payload["exp"] = expire
    payload["iat"] = datetime.now(timezone.utc)

    encoded = jwt.encode(
        payload,
        settings.JWT_SECRET,
        algorithm=settings.JWT_ALGORITHM,
    )
    return encoded


def verify_token(token: str) -> dict[str, Any]:
    """Decode and verify a JWT access token.

    Validates signature, expiration, and structural integrity.

    Args:
        token: A compact JWT string obtained from ``create_access_token``.

    Returns:
        The decoded payload dictionary on success.

    Raises:
        ValueError: If the token is invalid, expired, or cannot be decoded.
                    Callers (e.g. FastAPI dependencies) should map this to
                    an HTTP 401 response.
    """
    settings = get_settings()
    try:
        payload: dict[str, Any] = jwt.decode(
            token,
            settings.JWT_SECRET,
            algorithms=[settings.JWT_ALGORITHM],
        )
        return payload
    except JWTError as exc:
        logger.debug("JWT verification failed: %s", exc)
        raise ValueError("Token inválido o expirado") from exc
