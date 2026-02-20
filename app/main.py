import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: generate plantillas if they don't exist
    try:
        from app.services.template_service import generate_all_templates
        plantillas_dir = settings.PLANTILLAS_DIR
        # Only generate if directory is empty (no .xlsx files)
        existing = list(plantillas_dir.glob("*.xlsx"))
        if not existing:
            logger.info("Generating Excel plantillas on startup...")
            generate_all_templates(plantillas_dir)
            logger.info("Plantillas generated successfully.")
        else:
            logger.info("Plantillas already exist (%d files), skipping generation.", len(existing))
    except Exception as exc:
        logger.warning("Could not generate plantillas on startup: %s", exc)
    yield


app = FastAPI(
    title=settings.APP_NAME,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health_check():
    return {"status": "ok", "app": settings.APP_NAME}


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

from app.routers import auth  # noqa: E402

app.include_router(auth.router, prefix="/api/auth", tags=["Auth"])

# Budget Dashboard
from app.routers import presupuesto  # noqa: E402

app.include_router(
    presupuesto.router,
    prefix="/api/presupuesto",
    tags=["Presupuesto"],
)

# Import module
from app.routers import importacion  # noqa: E402

app.include_router(
    importacion.router,
    prefix="/api/importacion",
    tags=["Importación"],
)

# Master data / dropdown sources
from app.routers import datos_maestros  # noqa: E402

app.include_router(
    datos_maestros.router,
    prefix="/api/datos-maestros",
    tags=["Datos Maestros"],
)

# Contratos Menores ≤8 UIT
from app.routers import contratos_menores  # noqa: E402

app.include_router(
    contratos_menores.router,
    prefix="/api/contratos-menores",
    tags=["Contratos Menores"],
)

# Adquisiciones >8 UIT
from app.routers import adquisiciones  # noqa: E402

app.include_router(
    adquisiciones.router,
    prefix="/api/adquisiciones",
    tags=["Adquisiciones"],
)

# Actividades Operativas
from app.routers import actividades_operativas  # noqa: E402

app.include_router(
    actividades_operativas.router,
    prefix="/api/actividades-operativas",
    tags=["Actividades Operativas"],
)

# Alertas
from app.routers import alertas  # noqa: E402

app.include_router(
    alertas.router,
    prefix="/api/alertas",
    tags=["Alertas"],
)

# Exportación (Excel + PDF)
from app.routers import exportacion  # noqa: E402

app.include_router(
    exportacion.router,
    prefix="/api/exportar",
    tags=["Exportación"],
)
# v2: auto-create master data + fixed context positions
