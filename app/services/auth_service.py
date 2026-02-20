"""
Authentication business logic for the Dashboard INEI system.

Provides:
- ``authenticate_user`` — credential verification against the DB.
- ``get_current_user`` — FastAPI dependency that extracts and validates
  the Bearer JWT from the ``Authorization`` header.
- ``require_role`` — dependency factory that enforces role-based access
  control on top of ``get_current_user``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.usuario import Usuario
from app.utils.security import verify_password, verify_token

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OAuth2 scheme — tells FastAPI/Swagger where to find the Bearer token.
# The ``tokenUrl`` must match the login endpoint path (relative to root).
# ---------------------------------------------------------------------------

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


# ---------------------------------------------------------------------------
# Core authentication function
# ---------------------------------------------------------------------------


def authenticate_user(db: Session, username: str, password: str) -> Usuario | None:
    """Verify username/password credentials against the database.

    Looks up the user by ``username``, confirms the account is active,
    and validates the supplied password against the stored bcrypt hash.
    Returns ``None`` (instead of raising) so that callers can control the
    HTTP error response.

    Args:
        db: An active SQLAlchemy session (injected via ``get_db``).
        username: The login name submitted by the client.
        password: The plain-text password submitted by the client.

    Returns:
        The ``Usuario`` ORM instance on success, or ``None`` on failure
        (unknown user, inactive account, or wrong password).
    """
    user: Usuario | None = (
        db.query(Usuario)
        .filter(Usuario.username == username, Usuario.activo.is_(True))
        .first()
    )

    if user is None:
        # Use the same code path for unknown users and wrong passwords to
        # prevent username enumeration via timing side-channels.
        logger.debug("authenticate_user: unknown or inactive user '%s'", username)
        return None

    if not verify_password(password, user.password_hash):
        logger.debug("authenticate_user: wrong password for user '%s'", username)
        return None

    # Update last-access timestamp — best-effort, do not rollback on failure.
    try:
        user.ultimo_acceso = datetime.now(timezone.utc)
        db.commit()
    except Exception:  # pragma: no cover
        db.rollback()
        logger.warning("Could not update ultimo_acceso for user '%s'", username)

    return user


# ---------------------------------------------------------------------------
# FastAPI dependency — current authenticated user
# ---------------------------------------------------------------------------


def get_current_user(
    token: Annotated[str, Depends(oauth2_scheme)],
    db: Annotated[Session, Depends(get_db)],
) -> Usuario:
    """FastAPI dependency that resolves the caller's identity from a JWT.

    Extracts the ``Authorization: Bearer <token>`` header via the
    ``oauth2_scheme`` dependency, verifies the token signature and
    expiration, then loads the corresponding ``Usuario`` row.

    Args:
        token: Raw JWT string supplied by ``oauth2_scheme``.
        db: SQLAlchemy session supplied by ``get_db``.

    Returns:
        The authenticated ``Usuario`` ORM instance.

    Raises:
        HTTPException 401: If the token is missing, invalid, or expired,
                           or if the referenced user no longer exists or
                           has been deactivated.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudo validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = verify_token(token)
    except ValueError:
        raise credentials_exception

    # The ``sub`` claim stores the user's primary key as a string.
    user_id_str: str | None = payload.get("sub")
    if user_id_str is None:
        raise credentials_exception

    try:
        user_id = int(user_id_str)
    except (TypeError, ValueError):
        raise credentials_exception

    user: Usuario | None = (
        db.query(Usuario)
        .filter(Usuario.id == user_id, Usuario.activo.is_(True))
        .first()
    )

    if user is None:
        raise credentials_exception

    return user


# ---------------------------------------------------------------------------
# Role enforcement dependency factory
# ---------------------------------------------------------------------------


def require_role(*roles: str):
    """Return a FastAPI dependency that restricts access to the given roles.

    Designed to be used in endpoint signatures via ``Depends``:

    .. code-block:: python

        @router.get("/admin-only")
        def admin_endpoint(
            current_user: Usuario = Depends(require_role("ADMIN")),
        ):
            ...

    Args:
        *roles: One or more role codes from ``constants.ROLES`` that are
                permitted to access the decorated endpoint.

    Returns:
        A callable FastAPI dependency that resolves to the authenticated
        ``Usuario`` if their role is in *roles*, or raises HTTP 403.

    Raises:
        HTTPException 403: If the authenticated user's role is not in
                           the allowed *roles* set.
    """
    allowed = frozenset(roles)

    def _check_role(
        current_user: Annotated[Usuario, Depends(get_current_user)],
    ) -> Usuario:
        if current_user.rol not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"Acceso denegado. Se requiere uno de los roles: "
                    f"{sorted(allowed)}"
                ),
            )
        return current_user

    return _check_role
