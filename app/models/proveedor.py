"""Proveedor model — supplier/vendor registry."""

from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.orm import relationship

from app.database import Base


class Proveedor(Base):
    """Supplier or vendor registered in the system.

    Attributes:
        id: Primary key.
        ruc: Unique tax identification number (11 digits).
        razon_social: Legal company name.
        nombre_comercial: Trade name.
        estado_rnp: Status in the National Providers Registry.
        direccion: Physical address.
        telefono: Contact phone number.
        email: Contact email address.
        activo: Soft-delete flag.
    """

    __tablename__ = "proveedor"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ruc = Column(String(11), unique=True, nullable=False)
    razon_social = Column(String(300), nullable=False)
    nombre_comercial = Column(String(300), nullable=True)
    estado_rnp = Column(String(50), nullable=True)  # "HABIDO", "NO_HABIDO", "SUSPENDIDO"
    direccion = Column(String(500), nullable=True)
    telefono = Column(String(50), nullable=True)
    email = Column(String(200), nullable=True)
    activo = Column(Boolean, default=True, nullable=False)

    # Relationships — string references avoid circular imports at module load time
    adquisiciones = relationship("Adquisicion", back_populates="proveedor", lazy="select")
    contratos_menores = relationship("ContratoMenor", back_populates="proveedor", lazy="select")
