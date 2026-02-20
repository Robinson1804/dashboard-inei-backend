"""AdquisicionDetalle model — extended details for a procurement process (1:1)."""

from sqlalchemy import Column, Date, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from app.database import Base


class AdquisicionDetalle(Base):
    """Extended procurement details stored separately from the main Adquisicion.

    Has a strict 1:1 relationship with Adquisicion; the unique constraint on
    adquisicion_id enforces this at the database level.

    Attributes:
        id: Primary key.
        adquisicion_id: Unique FK to Adquisicion (enforces 1:1).
        n_expediente: Internal expedient number.
        n_proceso_seace: SEACE (national procurement portal) process number.
        n_proceso_pladicop: PLADICOP process number.
        bases_url: URL to the publicly published procurement bases.
        resolucion_aprobacion: Resolution number approving the expedient.
        fecha_aprobacion_expediente: Date the expedient was approved.
        observaciones: Free-text observations.
    """

    __tablename__ = "adquisicion_detalle"

    id = Column(Integer, primary_key=True, autoincrement=True)
    adquisicion_id = Column(
        Integer,
        ForeignKey("adquisicion.id"),
        unique=True,
        nullable=False,
    )
    n_expediente = Column(String(50), nullable=True)
    n_proceso_seace = Column(String(50), nullable=True)
    n_proceso_pladicop = Column(String(50), nullable=True)
    bases_url = Column(String(500), nullable=True)
    resolucion_aprobacion = Column(String(100), nullable=True)
    fecha_aprobacion_expediente = Column(Date, nullable=True)
    observaciones = Column(Text, nullable=True)

    # Relationships
    adquisicion = relationship(
        "Adquisicion", back_populates="detalle", lazy="select"
    )
