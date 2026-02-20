"""ContratoMenor model — minor contract / direct award (≤8 UIT, 9 milestones)."""

from sqlalchemy import Column, DateTime, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class ContratoMenor(Base):
    """Minor contracting process for acquisitions at or below 8 UIT (≤S/44,000).

    Follows a simplified 9-step stepper workflow. The system automatically
    detects "fraccionamiento" (deliberate splitting to stay below the threshold)
    by grouping contracts with the same categoria.

    Attributes:
        id: Primary key.
        codigo: Unique process code, e.g. "CM-2026-001".
        anio: Fiscal year.
        ue_id: FK to UnidadEjecutora.
        meta_id: FK to MetaPresupuestal.
        descripcion: Description of goods or services being contracted.
        tipo_objeto: "BIEN" or "SERVICIO".
        categoria: Grouping category used for fraccionamiento detection.
        estado: Current process state.
        monto_estimado: Estimated amount before quotation.
        monto_ejecutado: Final executed amount.
        proveedor_id: FK to Proveedor (nullable until awarded).
        n_orden: Order/purchase order number.
        n_cotizaciones: Number of quotations obtained.
        created_at: Record creation timestamp.
        updated_at: Last modification timestamp.
    """

    __tablename__ = "contrato_menor"

    id = Column(Integer, primary_key=True, autoincrement=True)
    codigo = Column(String(20), unique=True, nullable=True)
    anio = Column(Integer, nullable=True)
    ue_id = Column(Integer, ForeignKey("unidad_ejecutora.id"), nullable=True)
    meta_id = Column(Integer, ForeignKey("meta_presupuestal.id"), nullable=True)
    descripcion = Column(String(1000), nullable=True)
    tipo_objeto = Column(String(20), nullable=True)  # "BIEN", "SERVICIO"
    categoria = Column(String(100), nullable=True)
    estado = Column(String(50), nullable=True)
    # "PENDIENTE", "EN_PROCESO", "ORDEN_EMITIDA", "EJECUTADO", "PAGADO"
    monto_estimado = Column(Numeric(15, 2), nullable=True)
    monto_ejecutado = Column(Numeric(15, 2), nullable=True)
    proveedor_id = Column(Integer, ForeignKey("proveedor.id"), nullable=True)
    n_orden = Column(String(50), nullable=True)
    n_cotizaciones = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    unidad_ejecutora = relationship(
        "UnidadEjecutora", back_populates="contratos_menores", lazy="select"
    )
    meta_presupuestal = relationship(
        "MetaPresupuestal", back_populates="contratos_menores", lazy="select"
    )
    proveedor = relationship(
        "Proveedor", back_populates="contratos_menores", lazy="select"
    )
    procesos = relationship(
        "ContratoMenorProceso",
        back_populates="contrato_menor",
        order_by="ContratoMenorProceso.orden",
        lazy="select",
        cascade="all, delete-orphan",
    )
