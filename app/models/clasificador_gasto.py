"""ClasificadorGasto model — expenditure classifier codes (SIAF standard)."""

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

from app.database import Base


class ClasificadorGasto(Base):
    """Standard expenditure classifier from the SIAF (financial system).

    These codes follow the Peruvian public budget classification scheme,
    e.g. "2.3.1.5.1.2" for specific goods/services.

    Attributes:
        id: Primary key.
        codigo: Unique classifier code, e.g. "2.3.1.5.1.2".
        descripcion: Full name of the expenditure type.
        tipo_generico: Top-level group — "2.1", "2.3", "2.5", or "2.6".
    """

    __tablename__ = "clasificador_gasto"

    id = Column(Integer, primary_key=True, autoincrement=True)
    codigo = Column(String(20), unique=True, nullable=False)
    descripcion = Column(String(500), nullable=False)
    tipo_generico = Column(String(10), nullable=True)  # "2.1", "2.3", "2.5", "2.6"

    # Relationships
    programaciones_presupuestales = relationship(
        "ProgramacionPresupuestal", back_populates="clasificador_gasto", lazy="select"
    )
    modificaciones_presupuestales = relationship(
        "ModificacionPresupuestal", back_populates="clasificador_gasto", lazy="select"
    )
