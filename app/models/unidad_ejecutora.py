"""UnidadEjecutora model — executing unit (headquarters or regional office)."""

from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class UnidadEjecutora(Base):
    """Organizational unit that executes the budget.

    Can be the central INEI office ("CENTRAL") or one of the
    regional statistical offices ("ODEI").

    Attributes:
        id: Primary key.
        codigo: Short internal code, e.g. "001".
        nombre: Full legal name.
        sigla: Abbreviation, e.g. "INEI-LIMA".
        tipo: Unit classification — "CENTRAL" or "ODEI".
        activo: Soft-delete / active flag.
        created_at: Row creation timestamp (UTC).
        updated_at: Last modification timestamp (UTC).
    """

    __tablename__ = "unidad_ejecutora"

    id = Column(Integer, primary_key=True, autoincrement=True)
    codigo = Column(String(10), unique=True, nullable=False)
    nombre = Column(String(200), nullable=False)
    sigla = Column(String(20), nullable=False)
    tipo = Column(String(50), nullable=True)  # "CENTRAL" or "ODEI"
    activo = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    metas_presupuestales = relationship(
        "MetaPresupuestal", back_populates="unidad_ejecutora", lazy="select"
    )
    actividades_operativas = relationship(
        "ActividadOperativa", back_populates="unidad_ejecutora", lazy="select"
    )
    programaciones_presupuestales = relationship(
        "ProgramacionPresupuestal", back_populates="unidad_ejecutora", lazy="select"
    )
    modificaciones_presupuestales = relationship(
        "ModificacionPresupuestal", back_populates="unidad_ejecutora", lazy="select"
    )
    adquisiciones = relationship(
        "Adquisicion", back_populates="unidad_ejecutora", lazy="select"
    )
    contratos_menores = relationship(
        "ContratoMenor", back_populates="unidad_ejecutora", lazy="select"
    )
    alertas = relationship(
        "Alerta", back_populates="unidad_ejecutora", lazy="select"
    )
    usuarios = relationship(
        "Usuario", back_populates="unidad_ejecutora", lazy="select"
    )
