"""
Pydantic v2 schemas for user management (CRUD) endpoints.

Separates write schemas (``UsuarioCreate``, ``UsuarioUpdate``) from the
read schema (``UsuarioResponse``), following the single-responsibility
principle and avoiding accidental password exposure in API responses.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, EmailStr, Field

from app.utils.constants import ROLES

# Re-export the canonical read schema so callers can import from one place.
from app.schemas.auth import UserResponse as UsuarioResponse  # noqa: F401


class UsuarioCreate(BaseModel):
    """Payload for creating a new user account (``POST /api/usuarios``).

    Only accessible by users with the ``ADMIN`` role.

    Attributes:
        username: Unique login identifier (3–50 chars, alphanumeric + _).
        email: Valid email address; must be unique in the database.
        password: Plain-text password that will be hashed before storage.
        nombre_completo: Full display name for UI and audit logs.
        rol: Role code from ``constants.ROLES``.
        ue_id: Optional FK to ``unidad_ejecutora``; omit for system-wide access.
    """

    username: str = Field(
        ...,
        min_length=3,
        max_length=50,
        pattern=r"^[a-zA-Z0-9_]+$",
        description="Identificador único de inicio de sesión (alfanumérico y _)",
    )
    email: EmailStr = Field(
        ...,
        description="Correo electrónico institucional válido",
    )
    password: str = Field(
        ...,
        min_length=8,
        max_length=128,
        description="Contraseña en texto plano; se almacenará hasheada con bcrypt",
    )
    nombre_completo: str = Field(
        ...,
        min_length=3,
        max_length=150,
        description="Nombre completo para identificación en la UI y auditoría",
    )
    rol: str = Field(
        ...,
        description=f"Código de rol. Valores permitidos: {ROLES}",
    )
    ue_id: int | None = Field(
        default=None,
        description="ID de la Unidad Ejecutora asignada; None para acceso global",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "username": "mflores",
                "email": "m.flores@inei.gob.pe",
                "password": "IneiSecure2026!",
                "nombre_completo": "María Flores Quispe",
                "rol": "PRESUPUESTO",
                "ue_id": 3,
            }
        }
    )


class UsuarioUpdate(BaseModel):
    """Payload for partial updates to an existing user (``PUT /api/usuarios/{id}``).

    All fields are optional — only supplied fields are modified.
    Omitting ``password`` leaves the stored hash unchanged.

    Attributes:
        email: New email address (must remain unique).
        password: If provided, replaces the current password hash.
        nombre_completo: Updated display name.
        rol: New role assignment.
        ue_id: New organisational unit; ``None`` grants system-wide access.
        activo: Pass ``False`` to suspend the account (soft-delete).
    """

    email: EmailStr | None = Field(default=None, description="Nuevo correo electrónico")
    password: str | None = Field(
        default=None,
        min_length=8,
        max_length=128,
        description="Nueva contraseña en texto plano; omitir para mantener la actual",
    )
    nombre_completo: str | None = Field(
        default=None,
        min_length=3,
        max_length=150,
        description="Nombre completo actualizado",
    )
    rol: str | None = Field(
        default=None,
        description=f"Nuevo rol. Valores permitidos: {ROLES}",
    )
    ue_id: int | None = Field(
        default=None,
        description="Nueva Unidad Ejecutora asignada",
    )
    activo: bool | None = Field(
        default=None,
        description="False para suspender la cuenta",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "rol": "LOGISTICA",
                "activo": False,
            }
        }
    )
