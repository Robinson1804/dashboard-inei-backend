"""RegistroImportacion model — audit log of every file imported into the system."""

from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.sql import func

from app.database import Base


class RegistroImportacion(Base):
    """Persistent audit record created after each file import attempt.

    One row is written per upload regardless of success or failure so that
    administrators can review what was imported, by whom, and when.

    Attributes:
        id: Primary key.
        formato: Short label of the detected/declared file format,
            e.g. ``"FORMATO_1"``, ``"SIAF"``, ``"SIGA"``.
        archivo_nombre: Original filename submitted by the client.
        fecha: UTC timestamp when the import was processed.
        usuario_id: ID of the Usuario who performed the upload.
        usuario_username: Snapshot of the username at import time (denormalised
            to survive user renames without breaking the history display).
        ue_sigla: Sigla of the UnidadEjecutora detected in the file,
            or ``None`` for system-wide imports.
        registros_ok: Count of rows successfully persisted.
        registros_error: Count of rows rejected during validation.
        estado: Final import status — ``"EXITOSO"``, ``"PARCIAL"``,
            or ``"FALLIDO"``.
        errors_json: JSON-serialised list of error messages (stored as text
            to avoid a separate join table for the common case of < 100 errors).
        warnings_json: JSON-serialised list of warning messages.
    """

    __tablename__ = "registro_importacion"

    id = Column(Integer, primary_key=True, autoincrement=True)
    formato = Column(String(50), nullable=False)
    archivo_nombre = Column(String(500), nullable=False)
    fecha = Column(DateTime, default=func.now(), nullable=False)
    usuario_id = Column(Integer, nullable=False)
    usuario_username = Column(String(100), nullable=False)
    ue_sigla = Column(String(20), nullable=True)
    registros_ok = Column(Integer, default=0, nullable=False)
    registros_error = Column(Integer, default=0, nullable=False)
    estado = Column(String(20), nullable=False)  # EXITOSO | PARCIAL | FALLIDO
    errors_json = Column(Text, nullable=True)
    warnings_json = Column(Text, nullable=True)
