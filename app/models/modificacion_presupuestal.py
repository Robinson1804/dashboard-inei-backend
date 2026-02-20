"""ModificacionPresupuestal model — budget modification records (credits/debits)."""

from sqlalchemy import Column, Date, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import relationship

from app.database import Base


class ModificacionPresupuestal(Base):
    """Budget modification record (habilitación or habilitada).

    Tracks budget transfers between expenditure classifiers within or
    across executing units, producing an updated PIM (pim_resultante).

    Attributes:
        id: Primary key.
        anio: Fiscal year of the modification.
        ue_id: FK to UnidadEjecutora.
        clasificador_id: FK to ClasificadorGasto.
        tipo: "HABILITACION" (receiving funds) or "HABILITADA" (giving funds).
        monto: Amount transferred (positive).
        nota_modificacion: Official modification note number, e.g. "NM-001".
        fecha: Date the modification was issued.
        pim_resultante: Resulting PIM after the modification is applied.
    """

    __tablename__ = "modificacion_presupuestal"

    id = Column(Integer, primary_key=True, autoincrement=True)
    anio = Column(Integer, nullable=True)
    ue_id = Column(Integer, ForeignKey("unidad_ejecutora.id"), nullable=True)
    clasificador_id = Column(Integer, ForeignKey("clasificador_gasto.id"), nullable=True)
    tipo = Column(String(50), nullable=True)  # "HABILITACION" or "HABILITADA"
    monto = Column(Numeric(15, 2), nullable=True)
    nota_modificacion = Column(String(50), nullable=True)
    fecha = Column(Date, nullable=True)
    pim_resultante = Column(Numeric(15, 2), nullable=True)

    # Relationships
    unidad_ejecutora = relationship(
        "UnidadEjecutora", back_populates="modificaciones_presupuestales", lazy="select"
    )
    clasificador_gasto = relationship(
        "ClasificadorGasto", back_populates="modificaciones_presupuestales", lazy="select"
    )
