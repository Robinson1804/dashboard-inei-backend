"""Usuario model — application user with role-based access control."""

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class Usuario(Base):
    """System user with a role that controls dashboard access levels.

    Roles:
        - ADMIN: Full system access including configuration.
        - GERENCIA: Read-only executive view of all units.
        - PRESUPUESTO: Budget module full access.
        - LOGISTICA: Acquisitions and minor contracts full access.
        - CONSULTA: Read-only access to all modules.

    Attributes:
        id: Primary key.
        username: Unique login username.
        email: Unique email address.
        password_hash: Bcrypt-hashed password (never store plain text).
        nombre_completo: Full display name.
        rol: Role identifier controlling permissions.
        ue_id: Optional FK to UnidadEjecutora (restricts data scope).
        activo: Whether the account is active.
        ultimo_acceso: Timestamp of the last successful login.
        created_at: Record creation timestamp.
        updated_at: Last modification timestamp.
    """

    __tablename__ = "usuario"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(100), unique=True, nullable=False)
    email = Column(String(200), unique=True, nullable=False)
    password_hash = Column(String(200), nullable=False)
    nombre_completo = Column(String(300), nullable=True)
    rol = Column(String(50), nullable=True)
    # "ADMIN", "GERENCIA", "PRESUPUESTO", "LOGISTICA", "CONSULTA"
    ue_id = Column(Integer, ForeignKey("unidad_ejecutora.id"), nullable=True)
    activo = Column(Boolean, default=True, nullable=False)
    ultimo_acceso = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    unidad_ejecutora = relationship(
        "UnidadEjecutora", back_populates="usuarios", lazy="select"
    )
