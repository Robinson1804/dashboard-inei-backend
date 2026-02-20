"""AdquisicionProceso model — individual milestone in a 22-step procurement workflow."""

from sqlalchemy import Column, Date, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from app.database import Base


class AdquisicionProceso(Base):
    """One milestone (hito) within the 22-step procurement workflow.

    Milestones are distributed across three phases and multiple responsible
    areas (OTIN, DEC, OTA, OTPP, PROVEEDOR, COMITÉ). Planned vs actual
    dates drive the Gantt chart on the dashboard.

    Attributes:
        id: Primary key.
        adquisicion_id: FK to Adquisicion.
        orden: Sequential position 1–22.
        hito: Name of the milestone.
        fase: Phase this milestone belongs to.
        area_responsable: Area responsible for completing this milestone.
        dias_planificados: Planned duration in working days.
        fecha_inicio: Planned start date.
        fecha_fin: Planned end date.
        fecha_real_inicio: Actual start date (nullable until started).
        fecha_real_fin: Actual end date (nullable until completed).
        estado: Current milestone status.
        observacion: Free-text observations or blocking notes.
    """

    __tablename__ = "adquisicion_proceso"

    id = Column(Integer, primary_key=True, autoincrement=True)
    adquisicion_id = Column(Integer, ForeignKey("adquisicion.id"), nullable=False)
    orden = Column(Integer, nullable=False)  # 1–22
    hito = Column(String(200), nullable=False)
    fase = Column(String(50), nullable=True)
    # "ACTUACIONES_PREPARATORIAS", "SELECCION", "EJECUCION_CONTRACTUAL"
    area_responsable = Column(String(50), nullable=True)
    # "OTIN", "DEC", "OTA", "OTPP", "PROVEEDOR", "COMITÉ"
    dias_planificados = Column(Integer, nullable=True)
    fecha_inicio = Column(Date, nullable=True)
    fecha_fin = Column(Date, nullable=True)
    fecha_real_inicio = Column(Date, nullable=True)
    fecha_real_fin = Column(Date, nullable=True)
    estado = Column(String(20), nullable=True)
    # "COMPLETADO", "EN_CURSO", "PENDIENTE", "OBSERVADO"
    observacion = Column(Text, nullable=True)

    # Relationships
    adquisicion = relationship(
        "Adquisicion", back_populates="procesos", lazy="select"
    )
