"""MetaPresupuestal model — budget target/meta linked to an executing unit."""

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from app.database import Base


class MetaPresupuestal(Base):
    """Budget meta that groups expenditure classifications under a unit.

    Attributes:
        id: Primary key.
        codigo: Meta code, e.g. "0001".
        descripcion: Full description of the meta.
        sec_funcional: Functional sequence code.
        ue_id: Foreign key to UnidadEjecutora.
        anio: Fiscal year this meta belongs to.
        activo: Soft-delete flag.
    """

    __tablename__ = "meta_presupuestal"

    id = Column(Integer, primary_key=True, autoincrement=True)
    codigo = Column(String(10), nullable=False)
    descripcion = Column(String(500), nullable=True)
    sec_funcional = Column(String(20), nullable=True)
    ue_id = Column(Integer, ForeignKey("unidad_ejecutora.id"), nullable=False)
    anio = Column(Integer, nullable=False)
    activo = Column(Boolean, default=True, nullable=False)

    # Relationships
    unidad_ejecutora = relationship(
        "UnidadEjecutora", back_populates="metas_presupuestales", lazy="select"
    )
    actividades_operativas = relationship(
        "ActividadOperativa", back_populates="meta_presupuestal", lazy="select"
    )
    programaciones_presupuestales = relationship(
        "ProgramacionPresupuestal", back_populates="meta_presupuestal", lazy="select"
    )
    adquisiciones = relationship(
        "Adquisicion", back_populates="meta_presupuestal", lazy="select"
    )
    contratos_menores = relationship(
        "ContratoMenor", back_populates="meta_presupuestal", lazy="select"
    )
