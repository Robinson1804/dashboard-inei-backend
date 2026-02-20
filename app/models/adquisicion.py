"""Adquisicion model — complex procurement process (>8 UIT, 22 milestones)."""

from sqlalchemy import Column, DateTime, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class Adquisicion(Base):
    """Procurement process for acquisitions exceeding 8 UIT (>S/44,000).

    These follow the full 22-step OSCE procurement workflow divided into
    three phases: Actos Preparatorios, Selección, and Ejecución Contractual.

    Attributes:
        id: Primary key.
        codigo: Unique process code, e.g. "ADQ-2026-001".
        anio: Fiscal year.
        ue_id: FK to UnidadEjecutora.
        meta_id: FK to MetaPresupuestal.
        descripcion: Full description of the object being procured.
        tipo_objeto: Type — "BIEN", "SERVICIO", "OBRA", or "CONSULTORÍA".
        tipo_procedimiento: OSCE procedure type — LP, CP, SIE, etc.
        estado: Current process state.
        fase_actual: Current phase of the process.
        monto_referencial: Reference value before adjudication.
        monto_adjudicado: Awarded contract value.
        proveedor_id: FK to Proveedor (nullable until adjudicated).
        created_at: Record creation timestamp.
        updated_at: Last modification timestamp.
    """

    __tablename__ = "adquisicion"

    id = Column(Integer, primary_key=True, autoincrement=True)
    codigo = Column(String(20), unique=True, nullable=False)
    anio = Column(Integer, nullable=True)
    ue_id = Column(Integer, ForeignKey("unidad_ejecutora.id"), nullable=True)
    meta_id = Column(Integer, ForeignKey("meta_presupuestal.id"), nullable=True)
    descripcion = Column(String(1000), nullable=False)
    tipo_objeto = Column(String(20), nullable=True)  # "BIEN", "SERVICIO", "OBRA", "CONSULTORÍA"
    tipo_procedimiento = Column(String(100), nullable=True)  # LP, CP, SIE, etc.
    estado = Column(String(50), nullable=True)
    # "EN_ACTOS_PREPARATORIOS", "EN_SELECCION", "EN_EJECUCION",
    # "ADJUDICADO", "CULMINADO", "DESIERTO", "NULO"
    fase_actual = Column(String(50), nullable=True)
    # "ACTUACIONES_PREPARATORIAS", "SELECCION", "EJECUCION_CONTRACTUAL"
    monto_referencial = Column(Numeric(15, 2), nullable=True)
    monto_adjudicado = Column(Numeric(15, 2), nullable=True)
    proveedor_id = Column(Integer, ForeignKey("proveedor.id"), nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    unidad_ejecutora = relationship(
        "UnidadEjecutora", back_populates="adquisiciones", lazy="select"
    )
    meta_presupuestal = relationship(
        "MetaPresupuestal", back_populates="adquisiciones", lazy="select"
    )
    proveedor = relationship(
        "Proveedor", back_populates="adquisiciones", lazy="select"
    )
    detalle = relationship(
        "AdquisicionDetalle",
        back_populates="adquisicion",
        uselist=False,
        lazy="select",
        cascade="all, delete-orphan",
    )
    procesos = relationship(
        "AdquisicionProceso",
        back_populates="adquisicion",
        order_by="AdquisicionProceso.orden",
        lazy="select",
        cascade="all, delete-orphan",
    )
