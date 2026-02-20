import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


def _seed_admin_user() -> None:
    """Create default admin user if no users exist in the database."""
    try:
        from app.database import SessionLocal
        from app.models.usuario import Usuario
        from app.utils.security import hash_password

        db = SessionLocal()
        try:
            count = db.query(Usuario).count()
            print(f"[SEED] Usuarios en BD: {count}", flush=True)
            # Always ensure admin exists with correct password
            admin = db.query(Usuario).filter(Usuario.username == "admin").first()
            if admin is None:
                admin = Usuario(
                    username="admin",
                    email="admin@inei.gob.pe",
                    password_hash=hash_password("Admin123!"),
                    nombre_completo="Administrador INEI",
                    rol="ADMIN",
                    activo=True,
                )
                db.add(admin)
                db.commit()
                print("[SEED] ✅ Admin creado: admin / Admin123!", flush=True)
            else:
                # Reset password to ensure it's correct
                admin.password_hash = hash_password("Admin123!")
                admin.activo = True
                db.commit()
                print("[SEED] ✅ Admin password actualizado: admin / Admin123!", flush=True)
        finally:
            db.close()
    except Exception as exc:
        print(f"[SEED] ERROR: {exc}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: seed admin user if DB is empty
    _seed_admin_user()

    # Startup: generate plantillas if they don't exist
    try:
        from app.services.template_service import generate_all_templates
        plantillas_dir = settings.PLANTILLAS_DIR
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
