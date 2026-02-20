"""ProgramacionMensual model — monthly breakdown of budget programming."""

from sqlalchemy import Column, ForeignKey, Integer, Numeric
from sqlalchemy.orm import relationship

from app.database import Base


class ProgramacionMensual(Base):
    """Monthly budget programming and execution record.

    Each row represents one month (1–12) within a ProgramacionPresupuestal,
    recording how much was planned (programado) vs actually spent (ejecutado).

    Attributes:
        id: Primary key.
        programacion_presupuestal_id: FK to ProgramacionPresupuestal.
        mes: Month number (1 = January, 12 = December).
        programado: Planned expenditure for the month.
        ejecutado: Actual expenditure for the month.
        saldo: Difference (programado - ejecutado).
    """

    __tablename__ = "programacion_mensual"

    id = Column(Integer, primary_key=True, autoincrement=True)
    programacion_presupuestal_id = Column(
        Integer,
        ForeignKey("programacion_presupuestal.id"),
        nullable=False,
    )
    mes = Column(Integer, nullable=False)  # 1–12
    programado = Column(Numeric(15, 2), default=0, nullable=False)
    ejecutado = Column(Numeric(15, 2), default=0, nullable=False)
    saldo = Column(Numeric(15, 2), default=0, nullable=False)

    # Relationships
    programacion_presupuestal = relationship(
        "ProgramacionPresupuestal",
        back_populates="programaciones_mensuales",
        lazy="select",
    )
