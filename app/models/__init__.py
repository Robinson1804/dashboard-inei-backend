"""SQLAlchemy models package for Dashboard INEI.

Importing all models here ensures that SQLAlchemy's mapper registry is
populated before ``Base.metadata.create_all()`` or Alembic migrations run.
The import order follows the foreign-key dependency graph so that parent
tables are always registered before their children.

Usage from other modules:
    from app.models import Adquisicion, UnidadEjecutora
"""

# Leaf tables (no FK dependencies on other domain models)
from app.models.proveedor import Proveedor  # noqa: F401
from app.models.clasificador_gasto import ClasificadorGasto  # noqa: F401

# Core organisational hierarchy
from app.models.unidad_ejecutora import UnidadEjecutora  # noqa: F401
from app.models.meta_presupuestal import MetaPresupuestal  # noqa: F401
from app.models.actividad_operativa import ActividadOperativa  # noqa: F401

# Budget execution chain
from app.models.programacion_presupuestal import ProgramacionPresupuestal  # noqa: F401
from app.models.programacion_mensual import ProgramacionMensual  # noqa: F401
from app.models.modificacion_presupuestal import ModificacionPresupuestal  # noqa: F401

# Procurement — complex process (>8 UIT, 22 milestones)
from app.models.adquisicion import Adquisicion  # noqa: F401
from app.models.adquisicion_detalle import AdquisicionDetalle  # noqa: F401
from app.models.adquisicion_proceso import AdquisicionProceso  # noqa: F401

# Procurement — minor contracts (≤8 UIT, 9 milestones)
from app.models.contrato_menor import ContratoMenor  # noqa: F401
from app.models.contrato_menor_proceso import ContratoMenorProceso  # noqa: F401

# Cross-cutting concerns
from app.models.alerta import Alerta  # noqa: F401
from app.models.usuario import Usuario  # noqa: F401

# Import audit log
from app.models.registro_importacion import RegistroImportacion  # noqa: F401

__all__ = [
    "Proveedor",
    "ClasificadorGasto",
    "UnidadEjecutora",
    "MetaPresupuestal",
    "ActividadOperativa",
    "ProgramacionPresupuestal",
    "ProgramacionMensual",
    "ModificacionPresupuestal",
    "Adquisicion",
    "AdquisicionDetalle",
    "AdquisicionProceso",
    "ContratoMenor",
    "ContratoMenorProceso",
    "Alerta",
    "Usuario",
    "RegistroImportacion",
]
