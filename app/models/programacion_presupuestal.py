"""ProgramacionPresupuestal model — annual budget programming record."""

from sqlalchemy import Column, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import relationship

from app.database import Base


class ProgramacionPresupuestal(Base):
    """Annual budget programming entry combining unit, meta, and classifier.

    Tracks the full budget execution chain: PIA → PIM → Certificado →
    Compromiso → Devengado → Girado, plus computed saldo.

    Attributes:
        id: Primary key.
        anio: Fiscal year.
        ue_id: Foreign key to UnidadEjecutora.
        meta_id: Foreign key to MetaPresupuestal.
        clasificador_id: Foreign key to ClasificadorGasto.
        pia: Initial Institutional Budget (PIA).
        pim: Modified Institutional Budget (PIM).
        certificado: Certified amount.
        compromiso_anual: Annual commitment.
        devengado: Accrued expenditure.
        girado: Payment issued.
        saldo: Available balance (PIM - devengado).
        fuente_financiamiento: Funding source description.
    """

    __tablename__ = "programacion_presupuestal"

    id = Column(Integer, primary_key=True, autoincrement=True)
    anio = Column(Integer, nullable=False)
    ue_id = Column(Integer, ForeignKey("unidad_ejecutora.id"), nullable=False)
    meta_id = Column(Integer, ForeignKey("meta_presupuestal.id"), nullable=False)
    clasificador_id = Column(Integer, ForeignKey("clasificador_gasto.id"), nullable=False)
    pia = Column(Numeric(15, 2), default=0, nullable=False)
    pim = Column(Numeric(15, 2), default=0, nullable=False)
    certificado = Column(Numeric(15, 2), default=0, nullable=False)
    compromiso_anual = Column(Numeric(15, 2), default=0, nullable=False)
    devengado = Column(Numeric(15, 2), default=0, nullable=False)
    girado = Column(Numeric(15, 2), default=0, nullable=False)
    saldo = Column(Numeric(15, 2), default=0, nullable=False)
    fuente_financiamiento = Column(String(100), nullable=True)

    # Relationships
    unidad_ejecutora = relationship(
        "UnidadEjecutora", back_populates="programaciones_presupuestales", lazy="select"
    )
    meta_presupuestal = relationship(
        "MetaPresupuestal", back_populates="programaciones_presupuestales", lazy="select"
    )
    clasificador_gasto = relationship(
        "ClasificadorGasto", back_populates="programaciones_presupuestales", lazy="select"
    )
    programaciones_mensuales = relationship(
        "ProgramacionMensual",
        back_populates="programacion_presupuestal",
        lazy="select",
        cascade="all, delete-orphan",
    )
