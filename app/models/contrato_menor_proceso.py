"""ContratoMenorProceso model — milestone in the 9-step minor contracting workflow."""

from sqlalchemy import Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from app.database import Base


class ContratoMenorProceso(Base):
    """One milestone (hito) within the 9-step minor contracting workflow.

    Milestones are rendered as a linear stepper in the frontend dashboard.
    Each step has a responsible area and planned/actual dates.

    Attributes:
        id: Primary key.
        contrato_menor_id: FK to ContratoMenor.
        orden: Sequential position 1–9.
        hito: Name of the milestone.
        area_responsable: Area responsible for this step.
        dias_planificados: Planned duration in working days.
        fecha_inicio: Planned start date.
        fecha_fin: Planned end date.
        estado: Current milestone status.
    """

    __tablename__ = "contrato_menor_proceso"

    id = Column(Integer, primary_key=True, autoincrement=True)
    contrato_menor_id = Column(Integer, ForeignKey("contrato_menor.id"), nullable=False)
    orden = Column(Integer, nullable=False)  # 1–9
    hito = Column(String(200), nullable=False)
    area_responsable = Column(String(50), nullable=True)
    dias_planificados = Column(Integer, nullable=True)
    fecha_inicio = Column(Date, nullable=True)
    fecha_fin = Column(Date, nullable=True)
    estado = Column(String(20), nullable=True)  # "COMPLETADO", "EN_CURSO", "PENDIENTE"

    # Relationships
    contrato_menor = relationship(
        "ContratoMenor", back_populates="procesos", lazy="select"
    )
