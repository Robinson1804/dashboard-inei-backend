"""ActividadOperativa model — operational activity linked to strategic objectives."""

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from app.database import Base


class ActividadOperativa(Base):
    """Operational activity (AO) that contributes to institutional strategic goals.

    Each AO is identified by a CEPLAN code and is linked to both an
    Institutional Strategic Objective (OEI) and an Institutional Strategic
    Action (AEI).

    Attributes:
        id: Primary key.
        codigo_ceplan: CEPLAN code, e.g. "AOI00000500001".
        nombre: Full activity name.
        oei: Institutional Strategic Objective description.
        aei: Institutional Strategic Action description.
        meta_id: Foreign key to MetaPresupuestal.
        ue_id: Foreign key to UnidadEjecutora.
        anio: Fiscal year.
        activo: Soft-delete flag.
    """

    __tablename__ = "actividad_operativa"

    id = Column(Integer, primary_key=True, autoincrement=True)
    codigo_ceplan = Column(String(20), nullable=False)
    nombre = Column(String(500), nullable=False)
    oei = Column(String(200), nullable=True)
    aei = Column(String(200), nullable=True)
    meta_id = Column(Integer, ForeignKey("meta_presupuestal.id"), nullable=True)
    ue_id = Column(Integer, ForeignKey("unidad_ejecutora.id"), nullable=True)
    anio = Column(Integer, nullable=True)
    activo = Column(Boolean, default=True, nullable=False)

    # Relationships
    meta_presupuestal = relationship(
        "MetaPresupuestal", back_populates="actividades_operativas", lazy="select"
    )
    unidad_ejecutora = relationship(
        "UnidadEjecutora", back_populates="actividades_operativas", lazy="select"
    )
