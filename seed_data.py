"""Seed data script for the Dashboard INEI PostgreSQL database.

Populates the database with realistic demo data for development and testing.
The script is idempotent: it checks for existing records before inserting.

Usage (from the backend/ directory):
    py seed_data.py
"""

from __future__ import annotations

import sys
import os
from datetime import date, datetime, timedelta
from decimal import Decimal

# Ensure the backend package is importable when running from backend/
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database import SessionLocal, engine  # noqa: E402
from app.models import (  # noqa: E402
    Adquisicion,
    AdquisicionDetalle,
    AdquisicionProceso,
    Alerta,
    ActividadOperativa,
    ClasificadorGasto,
    ContratoMenor,
    ContratoMenorProceso,
    MetaPresupuestal,
    ModificacionPresupuestal,
    ProgramacionMensual,
    ProgramacionPresupuestal,
    Proveedor,
    UnidadEjecutora,
    Usuario,
)
from app.utils.security import hash_password  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ANIO = 2026


def _d(year: int, month: int, day: int) -> date:
    """Shorthand date constructor."""
    return date(year, month, day)


def _dec(value: float) -> Decimal:
    """Convert float to Decimal for Numeric columns."""
    return Decimal(str(round(value, 2)))


# ---------------------------------------------------------------------------
# Seed functions
# ---------------------------------------------------------------------------


def seed_unidades_ejecutoras(session) -> list[UnidadEjecutora]:
    """Insert 8 Unidades Ejecutoras if they do not already exist."""
    if session.query(UnidadEjecutora).count() > 0:
        print("  [SKIP] UnidadEjecutora — table already has data.")
        return session.query(UnidadEjecutora).all()

    registros = [
        UnidadEjecutora(
            codigo="001",
            nombre="Oficina Técnica de Informática",
            sigla="OTIN",
            tipo="CENTRAL",
            activo=True,
        ),
        UnidadEjecutora(
            codigo="002",
            nombre="Dirección Ejecutiva de Censos",
            sigla="DEC",
            tipo="CENTRAL",
            activo=True,
        ),
        UnidadEjecutora(
            codigo="003",
            nombre="Oficina Técnica de Administración",
            sigla="OTA",
            tipo="CENTRAL",
            activo=True,
        ),
        UnidadEjecutora(
            codigo="004",
            nombre="Oficina Técnica de Planificación y Presupuesto",
            sigla="OTPP",
            tipo="CENTRAL",
            activo=True,
        ),
        UnidadEjecutora(
            codigo="005",
            nombre="Dirección Nacional de Cuentas Económicas",
            sigla="DNCE",
            tipo="CENTRAL",
            activo=True,
        ),
        UnidadEjecutora(
            codigo="006",
            nombre="Dirección Nacional de Censos y Padrones Poblacionales",
            sigla="DNCPP",
            tipo="CENTRAL",
            activo=True,
        ),
        UnidadEjecutora(
            codigo="007",
            nombre="Dirección Nacional de Estadísticas Laborales",
            sigla="DNEL",
            tipo="CENTRAL",
            activo=True,
        ),
        UnidadEjecutora(
            codigo="008",
            nombre="Dirección de Tecnologías de Información",
            sigla="DTI",
            tipo="CENTRAL",
            activo=True,
        ),
    ]
    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] UnidadEjecutora — {len(registros)} registros insertados.")
    return session.query(UnidadEjecutora).all()


def seed_usuarios(session, ues: list[UnidadEjecutora]) -> None:
    """Insert 3 users if they do not already exist."""
    if session.query(Usuario).count() > 0:
        print("  [SKIP] Usuario — table already has data.")
        return

    ue_map = {ue.sigla: ue.id for ue in ues}

    registros = [
        Usuario(
            username="admin",
            email="admin@inei.gob.pe",
            password_hash=hash_password("Admin123!"),
            nombre_completo="Robinson Céspedes",
            rol="ADMIN",
            ue_id=ue_map.get("OTIN"),
            activo=True,
        ),
        Usuario(
            username="especialista",
            email="especialista@inei.gob.pe",
            password_hash=hash_password("esp123"),
            nombre_completo="María López",
            rol="PRESUPUESTO",
            ue_id=ue_map.get("DEC"),
            activo=True,
        ),
        Usuario(
            username="consultor",
            email="consultor@inei.gob.pe",
            password_hash=hash_password("cons123"),
            nombre_completo="Carlos Ruiz",
            rol="CONSULTA",
            ue_id=ue_map.get("OTA"),
            activo=True,
        ),
    ]
    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] Usuario — {len(registros)} registros insertados.")


def seed_metas_presupuestales(session, ues: list[UnidadEjecutora]) -> list[MetaPresupuestal]:
    """Insert 5 Metas Presupuestales if they do not already exist."""
    if session.query(MetaPresupuestal).count() > 0:
        print("  [SKIP] MetaPresupuestal — table already has data.")
        return session.query(MetaPresupuestal).all()

    ue_map = {ue.sigla: ue.id for ue in ues}

    registros = [
        MetaPresupuestal(
            codigo="0001",
            descripcion="Conducción y Orientación Superior del INEI",
            sec_funcional="00.001.0001",
            ue_id=ue_map["OTIN"],
            anio=ANIO,
            activo=True,
        ),
        MetaPresupuestal(
            codigo="0002",
            descripcion="Producción de Estadísticas Económicas Nacionales",
            sec_funcional="10.003.0002",
            ue_id=ue_map["DNCE"],
            anio=ANIO,
            activo=True,
        ),
        MetaPresupuestal(
            codigo="0003",
            descripcion="Ejecución del Censo Nacional de Población y Vivienda",
            sec_funcional="10.005.0003",
            ue_id=ue_map["DEC"],
            anio=ANIO,
            activo=True,
        ),
        MetaPresupuestal(
            codigo="0004",
            descripcion="Administración de Sistemas de Información Estadística",
            sec_funcional="05.002.0004",
            ue_id=ue_map["DTI"],
            anio=ANIO,
            activo=True,
        ),
        MetaPresupuestal(
            codigo="0005",
            descripcion="Fortalecimiento de Capacidades Estadísticas Regionales",
            sec_funcional="10.010.0005",
            ue_id=ue_map["OTPP"],
            anio=ANIO,
            activo=True,
        ),
    ]
    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] MetaPresupuestal — {len(registros)} registros insertados.")
    return session.query(MetaPresupuestal).all()


def seed_clasificadores_gasto(session) -> list[ClasificadorGasto]:
    """Insert 15 Clasificadores de Gasto if they do not already exist."""
    if session.query(ClasificadorGasto).count() > 0:
        print("  [SKIP] ClasificadorGasto — table already has data.")
        return session.query(ClasificadorGasto).all()

    registros = [
        # Personal activo — 2.1
        ClasificadorGasto(
            codigo="2.1.1.1.1.1",
            descripcion="Retribuciones y Complementos en Efectivo — Personal Permanente",
            tipo_generico="2.1",
        ),
        ClasificadorGasto(
            codigo="2.1.2.1.1.1",
            descripcion="Contribuciones a EsSalud — Personal Permanente",
            tipo_generico="2.1",
        ),
        # Bienes y servicios — 2.3
        ClasificadorGasto(
            codigo="2.3.1.5.1.2",
            descripcion="Papelería en General, Útiles y Materiales de Oficina",
            tipo_generico="2.3",
        ),
        ClasificadorGasto(
            codigo="2.3.1.5.1.3",
            descripcion="Aseo, Limpieza y Tocador",
            tipo_generico="2.3",
        ),
        ClasificadorGasto(
            codigo="2.3.1.8.1.1",
            descripcion="Combustibles, Carburantes, Lubricantes y Afines",
            tipo_generico="2.3",
        ),
        ClasificadorGasto(
            codigo="2.3.2.2.1.1",
            descripcion="Servicio de Telefonía e Internet",
            tipo_generico="2.3",
        ),
        ClasificadorGasto(
            codigo="2.3.2.2.2.1",
            descripcion="Servicio de Energía Eléctrica",
            tipo_generico="2.3",
        ),
        ClasificadorGasto(
            codigo="2.3.2.4.1.1",
            descripcion="Contrato Administrativo de Servicios (CAS)",
            tipo_generico="2.3",
        ),
        ClasificadorGasto(
            codigo="2.3.2.7.11.99",
            descripcion="Servicios de Consultoría y Asesoría Especializada",
            tipo_generico="2.3",
        ),
        ClasificadorGasto(
            codigo="2.3.2.9.1.1",
            descripcion="Servicio de Mantenimiento y Reparación de Equipos Informáticos",
            tipo_generico="2.3",
        ),
        ClasificadorGasto(
            codigo="2.3.2.12.1.1",
            descripcion="Licencias de Software y Actualizaciones",
            tipo_generico="2.3",
        ),
        # Donaciones y transferencias — 2.5
        ClasificadorGasto(
            codigo="2.5.3.1.1.1",
            descripcion="Transferencias Corrientes a Entidades Públicas",
            tipo_generico="2.5",
        ),
        # Adquisición de activos no financieros — 2.6
        ClasificadorGasto(
            codigo="2.6.3.2.1.1",
            descripcion="Adquisición de Equipos de Procesamiento de Datos",
            tipo_generico="2.6",
        ),
        ClasificadorGasto(
            codigo="2.6.3.2.3.1",
            descripcion="Adquisición de Vehículos de Transporte Terrestre",
            tipo_generico="2.6",
        ),
        ClasificadorGasto(
            codigo="2.6.6.1.3.1",
            descripcion="Adquisición de Software y Sistemas de Información",
            tipo_generico="2.6",
        ),
    ]
    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] ClasificadorGasto — {len(registros)} registros insertados.")
    return session.query(ClasificadorGasto).all()


def seed_proveedores(session) -> list[Proveedor]:
    """Insert 8 realistic Peruvian suppliers if they do not already exist."""
    if session.query(Proveedor).count() > 0:
        print("  [SKIP] Proveedor — table already has data.")
        return session.query(Proveedor).all()

    registros = [
        Proveedor(
            ruc="20100070970",
            razon_social="IBM DEL PERU S.A.C.",
            nombre_comercial="IBM Perú",
            estado_rnp="HABIDO",
            direccion="Av. Javier Prado Oeste 1650, San Isidro, Lima",
            telefono="(01) 611-9000",
            email="licitaciones@ibm.com.pe",
            activo=True,
        ),
        Proveedor(
            ruc="20112273922",
            razon_social="MICROSOFT PERU S.R.L.",
            nombre_comercial="Microsoft Perú",
            estado_rnp="HABIDO",
            direccion="Av. El Derby 254 Of.1201, Santiago de Surco, Lima",
            telefono="(01) 215-8000",
            email="ventas@microsoft.com.pe",
            activo=True,
        ),
        Proveedor(
            ruc="20501503893",
            razon_social="SUMINISTROS PAPELEROS LA UNION S.A.C.",
            nombre_comercial="Papelería La Unión",
            estado_rnp="HABIDO",
            direccion="Jr. Ucayali 491, Cercado de Lima",
            telefono="(01) 427-3456",
            email="ventas@launionpapelera.com.pe",
            activo=True,
        ),
        Proveedor(
            ruc="20602913771",
            razon_social="SOLUCIONES TECNOLOGICAS ANDINAS S.A.C.",
            nombre_comercial="STA Soluciones TI",
            estado_rnp="HABIDO",
            direccion="Calle Los Pinos 346, Miraflores, Lima",
            telefono="(01) 445-7890",
            email="proyectos@sta.com.pe",
            activo=True,
        ),
        Proveedor(
            ruc="20536987412",
            razon_social="CONSULTORA ESTADISTICA PERU S.A.C.",
            nombre_comercial="CEP Consultores",
            estado_rnp="HABIDO",
            direccion="Av. Arequipa 2560 Of.801, Lince, Lima",
            telefono="(01) 422-1234",
            email="info@cepконсult.com.pe",
            activo=True,
        ),
        Proveedor(
            ruc="20600347851",
            razon_social="ELECTRO SERVICIOS GENERALES J&M E.I.R.L.",
            nombre_comercial="Electro Servicios J&M",
            estado_rnp="HABIDO",
            direccion="Jr. Lampa 1010, Cercado de Lima",
            telefono="(01) 332-6540",
            email="electrojym@gmail.com",
            activo=True,
        ),
        Proveedor(
            ruc="20479346801",
            razon_social="TRANSPORTES Y LOGISTICA SANTA ROSA S.A.",
            nombre_comercial="Logística Santa Rosa",
            estado_rnp="HABIDO",
            direccion="Av. Tomás Valle 1820, Los Olivos, Lima",
            telefono="(01) 521-8900",
            email="logistica@santarosa.pe",
            activo=True,
        ),
        Proveedor(
            ruc="20555124963",
            razon_social="GRUPO EDITORIAL NACIONAL S.A.C.",
            nombre_comercial="Editorial Nacional",
            estado_rnp="HABIDO",
            direccion="Av. República de Panamá 3650, San Isidro, Lima",
            telefono="(01) 224-5000",
            email="contratos@editorialnacional.pe",
            activo=True,
        ),
    ]
    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] Proveedor — {len(registros)} registros insertados.")
    return session.query(Proveedor).all()


def seed_actividades_operativas(
    session,
    ues: list[UnidadEjecutora],
    metas: list[MetaPresupuestal],
) -> None:
    """Insert 15 Actividades Operativas with realistic semáforo distribution."""
    if session.query(ActividadOperativa).count() > 0:
        print("  [SKIP] ActividadOperativa — table already has data.")
        return

    ue_map = {ue.sigla: ue.id for ue in ues}
    meta_map = {m.codigo: m.id for m in metas}

    # Each entry: (codigo_ceplan, nombre, oei, aei, sigla_ue, codigo_meta)
    datos = [
        # VERDE (>=90%) — 6 activities
        (
            "AOI00000500001",
            "Conducción y Orientación Superior",
            "OEI.01 Gestión institucional eficiente y transparente",
            "AEI.01.01 Fortalecer la gobernanza institucional del INEI",
            "OTIN", "0001",
        ),
        (
            "AOI00000500002",
            "Gestión Presupuestaria y Financiera",
            "OEI.01 Gestión institucional eficiente y transparente",
            "AEI.01.02 Optimizar la ejecución presupuestal",
            "OTPP", "0001",
        ),
        (
            "AOI00000500003",
            "Producción de Estadísticas Económicas",
            "OEI.02 Información estadística oportuna y confiable",
            "AEI.02.01 Generar estadísticas económicas de calidad",
            "DNCE", "0002",
        ),
        (
            "AOI00000500004",
            "Diseño y Cartografía Censal",
            "OEI.03 Censos nacionales ejecutados con calidad",
            "AEI.03.01 Implementar el sistema de cartografía digital",
            "DEC", "0003",
        ),
        (
            "AOI00000500005",
            "Mantenimiento de Infraestructura Tecnológica",
            "OEI.04 Infraestructura TI moderna y segura",
            "AEI.04.01 Actualizar equipamiento informático crítico",
            "DTI", "0004",
        ),
        (
            "AOI00000500006",
            "Capacitación al Personal Estadístico",
            "OEI.01 Gestión institucional eficiente y transparente",
            "AEI.01.03 Desarrollar competencias del capital humano",
            "OTPP", "0005",
        ),
        # AMARILLO (70-89%) — 5 activities
        (
            "AOI00000500007",
            "Producción del Censo Nacional de Población",
            "OEI.03 Censos nacionales ejecutados con calidad",
            "AEI.03.02 Ejecutar operativos censales a nivel nacional",
            "DEC", "0003",
        ),
        (
            "AOI00000500008",
            "Encuesta Nacional de Hogares — ENAHO",
            "OEI.02 Información estadística oportuna y confiable",
            "AEI.02.02 Producir encuestas de hogares representativas",
            "DNEL", "0002",
        ),
        (
            "AOI00000500009",
            "Registro Nacional de Estadísticas Agropecuarias",
            "OEI.02 Información estadística oportuna y confiable",
            "AEI.02.03 Actualizar registros estadísticos sectoriales",
            "DNCE", "0002",
        ),
        (
            "AOI00000500010",
            "Desarrollo de Sistemas de Información Geoespacial",
            "OEI.04 Infraestructura TI moderna y segura",
            "AEI.04.02 Implementar plataformas de datos georreferenciados",
            "DTI", "0004",
        ),
        (
            "AOI00000500011",
            "Difusión y Publicación de Estadísticas Oficiales",
            "OEI.02 Información estadística oportuna y confiable",
            "AEI.02.04 Diseminar información estadística a la ciudadanía",
            "DNCPP", "0002",
        ),
        # ROJO (<70%) — 4 activities
        (
            "AOI00000500012",
            "Modernización del Sistema de Archivo Estadístico",
            "OEI.01 Gestión institucional eficiente y transparente",
            "AEI.01.04 Digitalizar y preservar el acervo estadístico histórico",
            "OTA", "0001",
        ),
        (
            "AOI00000500013",
            "Implementación del Módulo de Estadísticas Laborales",
            "OEI.02 Información estadística oportuna y confiable",
            "AEI.02.05 Ampliar cobertura de estadísticas de empleo",
            "DNEL", "0002",
        ),
        (
            "AOI00000500014",
            "Fortalecimiento de Oficinas Regionales de Estadística",
            "OEI.05 Descentralización estadística consolidada",
            "AEI.05.01 Dotar de recursos y equipamiento a OREIs",
            "OTPP", "0005",
        ),
        (
            "AOI00000500015",
            "Investigación y Desarrollo Metodológico Estadístico",
            "OEI.02 Información estadística oportuna y confiable",
            "AEI.02.06 Innovar métodos de recolección y procesamiento",
            "DNCPP", "0002",
        ),
    ]

    registros = []
    for codigo, nombre, oei, aei, sigla_ue, codigo_meta in datos:
        registros.append(
            ActividadOperativa(
                codigo_ceplan=codigo,
                nombre=nombre,
                oei=oei,
                aei=aei,
                ue_id=ue_map.get(sigla_ue),
                meta_id=meta_map.get(codigo_meta),
                anio=ANIO,
                activo=True,
            )
        )

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] ActividadOperativa — {len(registros)} registros insertados.")


def seed_programaciones_presupuestales(
    session,
    ues: list[UnidadEjecutora],
    metas: list[MetaPresupuestal],
    clasificadores: list[ClasificadorGasto],
) -> list[ProgramacionPresupuestal]:
    """Insert 20 ProgramacionPresupuestal records."""
    if session.query(ProgramacionPresupuestal).count() > 0:
        print("  [SKIP] ProgramacionPresupuestal — table already has data.")
        return session.query(ProgramacionPresupuestal).all()

    ue_map = {ue.sigla: ue.id for ue in ues}
    meta_map = {m.codigo: m.id for m in metas}
    clas_map = {c.codigo: c.id for c in clasificadores}

    # (sigla_ue, cod_meta, cod_clasificador, pia, pim, certificado, compromiso, devengado, girado, fuente)
    datos = [
        ("OTIN", "0001", "2.3.1.5.1.2",  45000,  52000,  48500,  48500,  44200,  44200,  "RO"),
        ("OTIN", "0001", "2.3.2.2.1.1",  38000,  38000,  35000,  35000,  33800,  33800,  "RO"),
        ("OTIN", "0001", "2.6.3.2.1.1", 180000, 220000, 210000, 210000, 198000, 195000,  "RO"),
        ("DEC",  "0003", "2.3.2.4.1.1", 650000, 720000, 700000, 700000, 680000, 675000,  "RO"),
        ("DEC",  "0003", "2.3.1.5.1.2",  28000,  35000,  30000,  30000,  25000,  25000,  "RO"),
        ("DEC",  "0003", "2.3.2.2.2.1",  15000,  18000,  14000,  14000,  11000,  11000,  "RREC"),
        ("DNCE", "0002", "2.3.2.7.11.99",120000, 145000, 130000, 130000, 115000, 112000, "RO"),
        ("DNCE", "0002", "2.3.2.9.1.1",  22000,  25000,  18000,  18000,  14000,  14000,  "RO"),
        ("DTI",  "0004", "2.6.3.2.1.1", 350000, 400000, 380000, 380000, 340000, 335000,  "RO"),
        ("DTI",  "0004", "2.3.2.12.1.1", 55000,  68000,  62000,  62000,  58000,  57000,  "RO"),
        ("OTPP", "0001", "2.3.2.2.1.1",  12000,  12000,  10500,  10500,   9800,   9800,  "RO"),
        ("OTPP", "0005", "2.3.2.4.1.1", 280000, 310000, 295000, 295000, 270000, 265000,  "RO"),
        ("OTA",  "0001", "2.3.1.5.1.3",   8000,   9500,   6500,   5000,   3200,   3200,  "RO"),
        ("OTA",  "0001", "2.3.1.8.1.1",  18000,  20000,  12000,   8000,   5600,   5600,  "RO"),
        ("DNEL", "0002", "2.3.2.4.1.1", 420000, 450000, 390000, 390000, 320000, 310000,  "RO"),
        ("DNEL", "0002", "2.3.2.7.11.99", 75000,  90000,  60000,  60000,  42000,  40000, "RREC"),
        ("DNCPP","0003", "2.3.2.4.1.1", 380000, 395000, 370000, 370000, 355000, 350000,  "RO"),
        ("DNCPP","0002", "2.6.6.1.3.1", 110000, 130000, 115000, 115000, 108000, 105000,  "RO"),
        ("DTI",  "0004", "2.6.3.2.3.1", 250000, 250000, 200000, 200000, 150000, 145000,  "RO"),
        ("OTPP", "0005", "2.5.3.1.1.1",  90000,  95000,  80000,  80000,  66000,  64000,  "RREC"),
    ]

    registros = []
    for sigla, cod_meta, cod_clas, pia, pim, cert, comp, dev, gir, fuente in datos:
        saldo = pim - dev
        registros.append(
            ProgramacionPresupuestal(
                anio=ANIO,
                ue_id=ue_map[sigla],
                meta_id=meta_map[cod_meta],
                clasificador_id=clas_map[cod_clas],
                pia=_dec(pia),
                pim=_dec(pim),
                certificado=_dec(cert),
                compromiso_anual=_dec(comp),
                devengado=_dec(dev),
                girado=_dec(gir),
                saldo=_dec(saldo),
                fuente_financiamiento=fuente,
            )
        )

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] ProgramacionPresupuestal — {len(registros)} registros insertados.")
    return session.query(ProgramacionPresupuestal).all()


def seed_programaciones_mensuales(
    session,
    programaciones: list[ProgramacionPresupuestal],
) -> None:
    """Insert monthly breakdown for the first 3 programaciones (Jan-Dec)."""
    if session.query(ProgramacionMensual).count() > 0:
        print("  [SKIP] ProgramacionMensual — table already has data.")
        return

    # Monthly distribution pattern: % of annual budget by month (roughly bell-shaped)
    # Sum should be 1.0
    distribucion = [0.06, 0.07, 0.08, 0.09, 0.09, 0.10, 0.10, 0.10, 0.09, 0.09, 0.08, 0.05]

    # Execution ratios by month (cumulative progress pattern; Feb 2026 = month 2)
    # Months 1-2 have actual data; months 3-12 are future (ejecutado = 0)
    ejecucion_ratio = [0.95, 0.88, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

    registros: list[ProgramacionMensual] = []

    # Seed monthly data for the first 5 programaciones
    for prog in programaciones[:5]:
        pim_anual = float(prog.pim)
        devengado_anual = float(prog.devengado)

        for mes_idx in range(12):
            mes = mes_idx + 1
            programado = pim_anual * distribucion[mes_idx]

            if ejecucion_ratio[mes_idx] > 0:
                ejecutado = programado * ejecucion_ratio[mes_idx]
            else:
                ejecutado = 0.0

            saldo = programado - ejecutado

            registros.append(
                ProgramacionMensual(
                    programacion_presupuestal_id=prog.id,
                    mes=mes,
                    programado=_dec(programado),
                    ejecutado=_dec(ejecutado),
                    saldo=_dec(saldo),
                )
            )

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] ProgramacionMensual — {len(registros)} registros insertados.")


def seed_adquisiciones(
    session,
    ues: list[UnidadEjecutora],
    metas: list[MetaPresupuestal],
    proveedores: list[Proveedor],
) -> list[Adquisicion]:
    """Insert 10 Adquisiciones >8 UIT with mixed states."""
    if session.query(Adquisicion).count() > 0:
        print("  [SKIP] Adquisicion — table already has data.")
        return session.query(Adquisicion).all()

    ue_map = {ue.sigla: ue.id for ue in ues}
    meta_map = {m.codigo: m.id for m in metas}
    prov_map = {p.ruc: p.id for p in proveedores}

    datos = [
        # code, sigla_ue, cod_meta, descripcion, tipo_objeto, tipo_proc, estado, fase, monto_ref, monto_adj, ruc_prov
        (
            "ADQ-2026-001", "DTI", "0004",
            "Adquisición de Servidores de Alto Rendimiento para el Centro de Datos Nacional del INEI",
            "BIEN", "LICITACION_PUBLICA",
            "EN_EJECUCION", "EJECUCION_CONTRACTUAL",
            350000.00, 338500.00, "20602913771",
        ),
        (
            "ADQ-2026-002", "DEC", "0003",
            "Servicio de Impresión y Distribución de Formularios Censales para el Censo Nacional 2026",
            "SERVICIO", "CONCURSO_PUBLICO",
            "ADJUDICADO", "EJECUCION_CONTRACTUAL",
            280000.00, 271500.00, "20555124963",
        ),
        (
            "ADQ-2026-003", "OTIN", "0001",
            "Adquisición de Licencias Corporativas de Microsoft 365 para Sedes Centrales",
            "BIEN", "CATALOGO_ELECTRONICO",
            "CULMINADO", "EJECUCION_CONTRACTUAL",
            145000.00, 138700.00, "20112273922",
        ),
        (
            "ADQ-2026-004", "DNCE", "0002",
            "Servicio de Consultoría para el Diseño e Implementación del Sistema de Cuentas Nacionales",
            "CONSULTORIA", "CONCURSO_PUBLICO",
            "EN_SELECCION", "SELECCION",
            320000.00, None, None,
        ),
        (
            "ADQ-2026-005", "DTI", "0004",
            "Adquisición de Equipos de Comunicaciones y Redes para Oficinas Regionales",
            "BIEN", "LICITACION_PUBLICA",
            "EN_ACTOS_PREPARATORIOS", "ACTUACIONES_PREPARATORIAS",
            210000.00, None, None,
        ),
        (
            "ADQ-2026-006", "DNEL", "0002",
            "Servicio de Encuestadores para la Encuesta Nacional de Hogares ENAHO 2026",
            "SERVICIO", "CONCURSO_PUBLICO",
            "EN_EJECUCION", "EJECUCION_CONTRACTUAL",
            480000.00, 465000.00, "20536987412",
        ),
        (
            "ADQ-2026-007", "DNCPP", "0003",
            "Adquisición de Tablets y Dispositivos Móviles para Empadronadores Censales",
            "BIEN", "LICITACION_PUBLICA",
            "DESIERTO", "SELECCION",
            520000.00, None, None,
        ),
        (
            "ADQ-2026-008", "OTA", "0001",
            "Servicio de Mantenimiento Preventivo y Correctivo de Vehículos Oficiales",
            "SERVICIO", "COMPARACION_PRECIOS",
            "EN_SELECCION", "SELECCION",
            58000.00, None, None,
        ),
        (
            "ADQ-2026-009", "OTPP", "0005",
            "Adquisición de Equipos de Videoconferencia para Capacitaciones Estadísticas Regionales",
            "BIEN", "SUBASTA_INVERSA",
            "CULMINADO", "EJECUCION_CONTRACTUAL",
            95000.00, 89200.00, "20602913771",
        ),
        (
            "ADQ-2026-010", "DTI", "0004",
            "Servicio de Desarrollo e Implementación del Portal Web de Datos Abiertos del INEI",
            "SERVICIO", "CONCURSO_PUBLICO",
            "EN_ACTOS_PREPARATORIOS", "ACTUACIONES_PREPARATORIAS",
            175000.00, None, None,
        ),
    ]

    registros = []
    for (
        codigo, sigla, cod_meta, desc, tipo_obj, tipo_proc,
        estado, fase, monto_ref, monto_adj, ruc_prov,
    ) in datos:
        registros.append(
            Adquisicion(
                codigo=codigo,
                anio=ANIO,
                ue_id=ue_map[sigla],
                meta_id=meta_map[cod_meta],
                descripcion=desc,
                tipo_objeto=tipo_obj,
                tipo_procedimiento=tipo_proc,
                estado=estado,
                fase_actual=fase,
                monto_referencial=_dec(monto_ref) if monto_ref else None,
                monto_adjudicado=_dec(monto_adj) if monto_adj else None,
                proveedor_id=prov_map.get(ruc_prov) if ruc_prov else None,
            )
        )

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] Adquisicion — {len(registros)} registros insertados.")
    return session.query(Adquisicion).order_by(Adquisicion.codigo).all()


def seed_adquisicion_detalles(
    session,
    adquisiciones: list[Adquisicion],
) -> None:
    """Insert AdquisicionDetalle (1:1) for each adquisicion."""
    if session.query(AdquisicionDetalle).count() > 0:
        print("  [SKIP] AdquisicionDetalle — table already has data.")
        return

    registros = []
    for idx, adq in enumerate(adquisiciones, start=1):
        has_seace = adq.estado not in ("EN_ACTOS_PREPARATORIOS",)
        registros.append(
            AdquisicionDetalle(
                adquisicion_id=adq.id,
                n_expediente=f"EXP-OTIN-{ANIO}-{idx:03d}",
                n_proceso_seace=f"N°{ANIO}-1-{idx:04d}-OEI-INEI-1" if has_seace else None,
                n_proceso_pladicop=f"PLA-{ANIO}-{idx:04d}" if has_seace else None,
                bases_url=(
                    f"https://seace.gob.pe/procurement/{ANIO}/{adq.codigo.lower()}/bases"
                    if has_seace
                    else None
                ),
                resolucion_aprobacion=(
                    f"RD-N°{idx:03d}-{ANIO}-INEI/OTA"
                    if adq.estado not in ("EN_ACTOS_PREPARATORIOS",)
                    else None
                ),
                fecha_aprobacion_expediente=(
                    _d(ANIO, 1, 10 + idx) if adq.estado not in ("EN_ACTOS_PREPARATORIOS",) else None
                ),
                observaciones=(
                    "Proceso sin observaciones." if adq.estado == "CULMINADO" else None
                ),
            )
        )

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] AdquisicionDetalle — {len(registros)} registros insertados.")


def seed_adquisicion_procesos(
    session,
    adquisiciones: list[Adquisicion],
) -> None:
    """Insert AdquisicionProceso hitos for each adquisicion following 3-phase structure."""
    if session.query(AdquisicionProceso).count() > 0:
        print("  [SKIP] AdquisicionProceso — table already has data.")
        return

    # 22-step template: (orden, hito, fase, area_responsable, dias_planificados)
    plantilla_hitos = [
        # FASE 1: ACTUACIONES_PREPARATORIAS (steps 1-8)
        (1,  "Elaboración del Requerimiento Técnico",         "ACTUACIONES_PREPARATORIAS", "OTIN",      5),
        (2,  "Conformidad del Área Usuaria",                  "ACTUACIONES_PREPARATORIAS", "OTIN",      3),
        (3,  "Elaboración del Estudio de Mercado",            "ACTUACIONES_PREPARATORIAS", "DEC",       7),
        (4,  "Determinación del Valor Referencial",           "ACTUACIONES_PREPARATORIAS", "DEC",       3),
        (5,  "Elaboración del Expediente de Contratación",    "ACTUACIONES_PREPARATORIAS", "OTA",       5),
        (6,  "Aprobación del Expediente de Contratación",     "ACTUACIONES_PREPARATORIAS", "OTPP",      3),
        (7,  "Designación del Comité de Selección",           "ACTUACIONES_PREPARATORIAS", "OTPP",      2),
        (8,  "Elaboración y Aprobación de Bases",             "ACTUACIONES_PREPARATORIAS", "COMITÉ",    7),
        # FASE 2: SELECCION (steps 9-16)
        (9,  "Convocatoria y Publicación en SEACE",           "SELECCION",                 "COMITÉ",    1),
        (10, "Registro de Participantes",                     "SELECCION",                 "COMITÉ",    5),
        (11, "Formulación de Consultas y Observaciones",      "SELECCION",                 "COMITÉ",    5),
        (12, "Absolución de Consultas y Observaciones",       "SELECCION",                 "COMITÉ",    7),
        (13, "Integración de Bases",                          "SELECCION",                 "COMITÉ",    3),
        (14, "Presentación de Ofertas",                       "SELECCION",                 "PROVEEDOR", 5),
        (15, "Evaluación y Calificación de Ofertas",          "SELECCION",                 "COMITÉ",    5),
        (16, "Otorgamiento de la Buena Pro",                  "SELECCION",                 "COMITÉ",    1),
        # FASE 3: EJECUCION_CONTRACTUAL (steps 17-22)
        (17, "Consentimiento de la Buena Pro",                "EJECUCION_CONTRACTUAL",     "COMITÉ",    5),
        (18, "Suscripción del Contrato",                      "EJECUCION_CONTRACTUAL",     "OTA",       5),
        (19, "Entrega de Adelanto (si aplica)",               "EJECUCION_CONTRACTUAL",     "OTA",       3),
        (20, "Ejecución de la Prestación",                    "EJECUCION_CONTRACTUAL",     "PROVEEDOR", 30),
        (21, "Conformidad de la Prestación",                  "EJECUCION_CONTRACTUAL",     "OTIN",      5),
        (22, "Pago al Proveedor",                             "EJECUCION_CONTRACTUAL",     "OTA",       5),
    ]

    # Progress map: how many steps are COMPLETADO per estado
    progreso_por_estado = {
        "EN_ACTOS_PREPARATORIOS": 4,   # Steps 1-4 done, 5+ pending
        "EN_SELECCION": 10,             # Steps 1-10 done, 11+ pending
        "EN_EJECUCION": 19,             # Steps 1-19 done, 20+ pending
        "ADJUDICADO": 17,               # Through adjudication
        "CULMINADO": 22,               # All 22 done
        "DESIERTO": 14,                # Got to evaluation, then declared void
    }

    registros: list[AdquisicionProceso] = []
    fecha_base = _d(ANIO, 1, 15)  # Project start date

    for adq in adquisiciones:
        pasos_completados = progreso_por_estado.get(adq.estado, 0)
        fecha_corriente = fecha_base

        for orden, hito, fase, area, dias in plantilla_hitos:
            fecha_inicio = fecha_corriente
            fecha_fin = fecha_corriente + timedelta(days=dias)

            if orden <= pasos_completados:
                estado_hito = "COMPLETADO"
                fecha_real_inicio = fecha_inicio
                fecha_real_fin = fecha_fin
            elif orden == pasos_completados + 1 and adq.estado not in ("DESIERTO", "CULMINADO"):
                estado_hito = "EN_CURSO"
                fecha_real_inicio = fecha_inicio
                fecha_real_fin = None
            else:
                estado_hito = "PENDIENTE"
                fecha_real_inicio = None
                fecha_real_fin = None

            # For DESIERTO at step 14: mark that step as OBSERVADO
            if adq.estado == "DESIERTO" and orden == 15:
                estado_hito = "OBSERVADO"

            registros.append(
                AdquisicionProceso(
                    adquisicion_id=adq.id,
                    orden=orden,
                    hito=hito,
                    fase=fase,
                    area_responsable=area,
                    dias_planificados=dias,
                    fecha_inicio=fecha_inicio,
                    fecha_fin=fecha_fin,
                    fecha_real_inicio=fecha_real_inicio,
                    fecha_real_fin=fecha_real_fin,
                    estado=estado_hito,
                    observacion=(
                        "Proceso declarado desierto por falta de postores calificados."
                        if adq.estado == "DESIERTO" and orden == 15
                        else None
                    ),
                )
            )

            # Advance calendar
            fecha_corriente = fecha_fin + timedelta(days=1)

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] AdquisicionProceso — {len(registros)} registros insertados.")


def seed_contratos_menores(
    session,
    ues: list[UnidadEjecutora],
    metas: list[MetaPresupuestal],
    proveedores: list[Proveedor],
) -> list[ContratoMenor]:
    """Insert 10 ContratoMenor <=8 UIT with mixed states."""
    if session.query(ContratoMenor).count() > 0:
        print("  [SKIP] ContratoMenor — table already has data.")
        return session.query(ContratoMenor).all()

    ue_map = {ue.sigla: ue.id for ue in ues}
    meta_map = {m.codigo: m.id for m in metas}
    prov_map = {p.ruc: p.id for p in proveedores}

    datos = [
        # codigo, sigla, cod_meta, desc, tipo, categoria, estado, monto_est, monto_ej, ruc_prov, n_orden, n_cot
        (
            "CM-2026-001", "OTIN", "0001",
            "Adquisición de Papel Bond A4 para Impresoras — 200 Millar",
            "BIEN", "MATERIALES_OFICINA", "PAGADO",
            4200.00, 4080.00, "20501503893", "OC-2026-001", 3,
        ),
        (
            "CM-2026-002", "OTA", "0001",
            "Servicio de Limpieza y Mantenimiento de Oficinas — Enero 2026",
            "SERVICIO", "LIMPIEZA", "PAGADO",
            3800.00, 3800.00, "20600347851", "OS-2026-001", 3,
        ),
        (
            "CM-2026-003", "DTI", "0004",
            "Adquisición de Cartuchos de Tinta y Tóner para Impresoras",
            "BIEN", "SUMINISTROS_TI", "EJECUTADO",
            5500.00, 5350.00, "20602913771", "OC-2026-003", 3,
        ),
        (
            "CM-2026-004", "DEC", "0003",
            "Servicio de Fotocopiado e Impresión de Materiales Censales",
            "SERVICIO", "MATERIALES_CENSALES", "ORDEN_EMITIDA",
            8900.00, None, "20555124963", "OS-2026-004", 3,
        ),
        (
            "CM-2026-005", "DNEL", "0002",
            "Adquisición de Combustible para Vehículos del Operativo ENAHO",
            "BIEN", "COMBUSTIBLES", "EN_PROCESO",
            12000.00, None, None, None, 2,
        ),
        (
            "CM-2026-006", "DNCE", "0002",
            "Servicio de Mensajería y Courier Nacional para Envío de Documentos Estadísticos",
            "SERVICIO", "MENSAJERIA", "PAGADO",
            2800.00, 2750.00, "20479346801", "OS-2026-006", 3,
        ),
        (
            "CM-2026-007", "OTIN", "0001",
            "Adquisición de Materiales de Limpieza para Áreas de Trabajo",
            "BIEN", "MATERIALES_LIMPIEZA", "PAGADO",
            1850.00, 1820.00, "20600347851", "OC-2026-007", 3,
        ),
        (
            "CM-2026-008", "OTPP", "0005",
            "Servicio de Diseño e Impresión de Brochures y Afiches para Difusión Estadística",
            "SERVICIO", "MATERIALES_DIFUSION", "EN_PROCESO",
            7200.00, None, "20555124963", "OS-2026-008", 2,
        ),
        (
            "CM-2026-009", "DEC", "0003",
            "Adquisición de Útiles de Escritorio y Artículos de Oficina",
            "BIEN", "MATERIALES_OFICINA", "PENDIENTE",
            3100.00, None, None, None, 0,
        ),
        (
            "CM-2026-010", "OTA", "0001",
            "Servicio de Mantenimiento y Reparación de Mobiliario de Oficinas",
            "SERVICIO", "MANTENIMIENTO", "ORDEN_EMITIDA",
            6500.00, None, "20600347851", "OS-2026-010", 3,
        ),
    ]

    registros = []
    for (
        codigo, sigla, cod_meta, desc, tipo, cat,
        estado, monto_est, monto_ej, ruc_prov, n_orden, n_cot,
    ) in datos:
        registros.append(
            ContratoMenor(
                codigo=codigo,
                anio=ANIO,
                ue_id=ue_map[sigla],
                meta_id=meta_map[cod_meta],
                descripcion=desc,
                tipo_objeto=tipo,
                categoria=cat,
                estado=estado,
                monto_estimado=_dec(monto_est) if monto_est else None,
                monto_ejecutado=_dec(monto_ej) if monto_ej else None,
                proveedor_id=prov_map.get(ruc_prov) if ruc_prov else None,
                n_orden=n_orden,
                n_cotizaciones=n_cot,
            )
        )

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] ContratoMenor — {len(registros)} registros insertados.")
    return session.query(ContratoMenor).order_by(ContratoMenor.codigo).all()


def seed_contrato_menor_procesos(
    session,
    contratos: list[ContratoMenor],
) -> None:
    """Insert 9-step workflow hitos for each ContratoMenor."""
    if session.query(ContratoMenorProceso).count() > 0:
        print("  [SKIP] ContratoMenorProceso — table already has data.")
        return

    # 9-step template: (orden, hito, area_responsable, dias_planificados)
    plantilla = [
        (1, "Elaboración del Requerimiento de Cotización",    "OTIN",      2),
        (2, "Solicitud de Cotizaciones al Mercado",           "OTA",       3),
        (3, "Recepción y Evaluación de Cotizaciones",         "OTA",       2),
        (4, "Selección del Proveedor y Cuadro Comparativo",   "OTA",       1),
        (5, "Aprobación del Cuadro Comparativo",              "OTPP",      1),
        (6, "Emisión de la Orden de Compra/Servicio",         "OTA",       1),
        (7, "Ejecución y Entrega por el Proveedor",           "PROVEEDOR", 7),
        (8, "Conformidad del Área Usuaria",                   "OTIN",      2),
        (9, "Tramitación del Pago",                           "OTA",       3),
    ]

    # Progress map: steps completed per estado
    progreso_por_estado = {
        "PENDIENTE": 0,
        "EN_PROCESO": 3,
        "ORDEN_EMITIDA": 6,
        "EJECUTADO": 8,
        "PAGADO": 9,
    }

    registros: list[ContratoMenorProceso] = []
    fecha_base = _d(ANIO, 1, 20)

    for contrato in contratos:
        pasos_completados = progreso_por_estado.get(contrato.estado, 0)
        fecha_corriente = fecha_base

        for orden, hito, area, dias in plantilla:
            fecha_inicio = fecha_corriente
            fecha_fin = fecha_corriente + timedelta(days=dias)

            if orden <= pasos_completados:
                estado_hito = "COMPLETADO"
            elif orden == pasos_completados + 1 and contrato.estado not in ("PENDIENTE",):
                estado_hito = "EN_CURSO"
            else:
                estado_hito = "PENDIENTE"

            registros.append(
                ContratoMenorProceso(
                    contrato_menor_id=contrato.id,
                    orden=orden,
                    hito=hito,
                    area_responsable=area,
                    dias_planificados=dias,
                    fecha_inicio=fecha_inicio,
                    fecha_fin=fecha_fin,
                    estado=estado_hito,
                )
            )

            fecha_corriente = fecha_fin + timedelta(days=1)

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] ContratoMenorProceso — {len(registros)} registros insertados.")


def seed_alertas(
    session,
    ues: list[UnidadEjecutora],
) -> None:
    """Insert 8 system alerts with mixed tipos and niveles."""
    if session.query(Alerta).count() > 0:
        print("  [SKIP] Alerta — table already has data.")
        return

    ue_map = {ue.sigla: ue.id for ue in ues}
    ahora = datetime(ANIO, 2, 17, 9, 0, 0)

    registros = [
        Alerta(
            tipo="EJECUCION_BAJA",
            nivel="ROJO",
            titulo="Ejecución Presupuestal Crítica — OTA",
            descripcion=(
                "La Oficina Técnica de Administración registra una ejecución del 55.8% "
                "en el clasificador 2.3.1.8.1.1 (Combustibles), muy por debajo del umbral "
                "mínimo del 70% establecido para el periodo. Se recomienda verificar los "
                "procesos de adquisición pendientes e iniciar acciones correctivas inmediatas."
            ),
            ue_id=ue_map["OTA"],
            modulo="PRESUPUESTO",
            entidad_id=None,
            entidad_tipo="programacion",
            leida=False,
            resuelta=False,
            fecha_generacion=ahora - timedelta(hours=2),
        ),
        Alerta(
            tipo="EJECUCION_BAJA",
            nivel="AMARILLO",
            titulo="Ejecución por Debajo de Meta — DNEL",
            descripcion=(
                "La Dirección Nacional de Estadísticas Laborales muestra ejecución del 71.1% "
                "en servicios de consultoría (clasificador 2.3.2.7.11.99). Se encuentra en zona "
                "amarilla. Revisar avance de contratos vigentes para asegurar el cumplimiento "
                "de metas al cierre del primer trimestre."
            ),
            ue_id=ue_map["DNEL"],
            modulo="PRESUPUESTO",
            entidad_id=None,
            entidad_tipo="programacion",
            leida=True,
            resuelta=False,
            fecha_generacion=ahora - timedelta(hours=5),
            fecha_lectura=ahora - timedelta(hours=4),
        ),
        Alerta(
            tipo="PROCESO_PARALIZADO",
            nivel="ROJO",
            titulo="Adquisición ADQ-2026-005 Sin Avance por 15 Días",
            descripcion=(
                "El proceso de adquisición ADQ-2026-005 'Equipos de Comunicaciones y Redes' "
                "lleva 15 días sin registro de avance en la fase de Actuaciones Preparatorias. "
                "El comité designado debe reportar el estado actual y gestionar la aprobación "
                "del expediente de contratación antes del 28 de febrero para no comprometer "
                "el cronograma del primer trimestre."
            ),
            ue_id=ue_map["DTI"],
            modulo="ADQUISICIONES",
            entidad_id=5,
            entidad_tipo="adquisicion",
            leida=False,
            resuelta=False,
            fecha_generacion=ahora - timedelta(hours=1),
        ),
        Alerta(
            tipo="PROCESO_DESIERTO",
            nivel="AMARILLO",
            titulo="Proceso ADQ-2026-007 Declarado Desierto",
            descripcion=(
                "El proceso de Licitación Pública ADQ-2026-007 para 'Tablets y Dispositivos "
                "Móviles para Empadronadores Censales' fue declarado desierto por falta de "
                "postores calificados. La Dirección Nacional de Censos debe iniciar un nuevo "
                "proceso a la brevedad para no comprometer el operativo censal programado "
                "para el segundo trimestre 2026."
            ),
            ue_id=ue_map["DNCPP"],
            modulo="ADQUISICIONES",
            entidad_id=7,
            entidad_tipo="adquisicion",
            leida=False,
            resuelta=False,
            fecha_generacion=ahora - timedelta(days=2),
        ),
        Alerta(
            tipo="FRACCIONAMIENTO_DETECTADO",
            nivel="ROJO",
            titulo="Posible Fraccionamiento — Materiales de Oficina OTIN",
            descripcion=(
                "El sistema detectó 3 contratos menores en la categoría 'MATERIALES_OFICINA' "
                "para la Oficina Técnica de Informática (CM-2026-001, CM-2026-007 y uno pendiente) "
                "dentro del mismo trimestre. El monto acumulado supera las 8 UIT (S/44,000). "
                "Este patrón configura posible fraccionamiento según el artículo 27 de la Ley "
                "de Contrataciones del Estado. Se requiere revisión inmediata por el área legal."
            ),
            ue_id=ue_map["OTIN"],
            modulo="CONTRATOS_MENORES",
            entidad_id=None,
            entidad_tipo="contrato_menor",
            leida=False,
            resuelta=False,
            fecha_generacion=ahora - timedelta(hours=3),
        ),
        Alerta(
            tipo="PLAZO_VENCIMIENTO",
            nivel="AMARILLO",
            titulo="Contrato ADQ-2026-006 Próximo al Vencimiento",
            descripcion=(
                "El contrato de servicio ADQ-2026-006 'Encuestadores ENAHO 2026' vence en "
                "15 días calendario. Si el servicio aún está en ejecución, el área de "
                "Logística debe gestionar la adenda de ampliación de plazo o iniciar el "
                "nuevo proceso de contratación para evitar una brecha de servicio."
            ),
            ue_id=ue_map["DNEL"],
            modulo="ADQUISICIONES",
            entidad_id=6,
            entidad_tipo="adquisicion",
            leida=True,
            resuelta=False,
            fecha_generacion=ahora - timedelta(days=1),
            fecha_lectura=ahora - timedelta(hours=12),
        ),
        Alerta(
            tipo="META_CUMPLIDA",
            nivel="VERDE",
            titulo="Meta de Ejecución Cumplida — ADQ-2026-009",
            descripcion=(
                "El proceso ADQ-2026-009 'Equipos de Videoconferencia para Capacitaciones "
                "Estadísticas' ha sido culminado exitosamente. El proveedor entregó todos "
                "los equipos conformes y se realizó el pago final de S/89,200. La ejecución "
                "total representa el 93.9% del monto referencial, dentro del rango óptimo."
            ),
            ue_id=ue_map["OTPP"],
            modulo="ADQUISICIONES",
            entidad_id=9,
            entidad_tipo="adquisicion",
            leida=True,
            resuelta=True,
            fecha_generacion=ahora - timedelta(days=5),
            fecha_lectura=ahora - timedelta(days=4),
            fecha_resolucion=ahora - timedelta(days=4),
        ),
        Alerta(
            tipo="SALDO_INSUFICIENTE",
            nivel="AMARILLO",
            titulo="Saldo Presupuestal Bajo — DTI Vehículos",
            descripcion=(
                "El clasificador 2.6.3.2.3.1 (Adquisición de Vehículos) de la Dirección de "
                "Tecnologías de Información registra una ejecución del 60% con una demanda "
                "pendiente estimada de S/55,000. El saldo disponible es de S/100,000 lo que "
                "podría resultar insuficiente si se aprueban todas las solicitudes pendientes. "
                "Se recomienda revisar la programación del segundo semestre."
            ),
            ue_id=ue_map["DTI"],
            modulo="PRESUPUESTO",
            entidad_id=None,
            entidad_tipo="programacion",
            leida=False,
            resuelta=False,
            fecha_generacion=ahora - timedelta(hours=6),
        ),
    ]

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] Alerta — {len(registros)} registros insertados.")


def seed_modificaciones_presupuestales(
    session,
    ues: list[UnidadEjecutora],
    clasificadores: list[ClasificadorGasto],
) -> None:
    """Insert 5 Modificaciones Presupuestales (habilitaciones/habilitadas)."""
    if session.query(ModificacionPresupuestal).count() > 0:
        print("  [SKIP] ModificacionPresupuestal — table already has data.")
        return

    ue_map = {ue.sigla: ue.id for ue in ues}
    clas_map = {c.codigo: c.id for c in clasificadores}

    datos = [
        # sigla_ue, cod_clas, tipo, monto, nota, fecha, pim_resultante
        ("DTI",  "2.6.3.2.1.1", "HABILITACION", 50000.00, "NM-2026-001", _d(ANIO, 1, 25), 450000.00),
        ("OTA",  "2.3.1.8.1.1", "HABILITADA",    8000.00, "NM-2026-002", _d(ANIO, 2,  5),  12000.00),
        ("DNEL", "2.3.2.4.1.1", "HABILITACION", 30000.00, "NM-2026-003", _d(ANIO, 2, 10), 480000.00),
        ("OTPP", "2.3.2.7.11.99","HABILITADA",  15000.00, "NM-2026-004", _d(ANIO, 2, 12),  75000.00),
        ("DEC",  "2.3.2.2.2.1", "HABILITACION", 5000.00,  "NM-2026-005", _d(ANIO, 2, 15),  23000.00),
    ]

    registros = []
    for sigla, cod_clas, tipo, monto, nota, fecha, pim_res in datos:
        registros.append(
            ModificacionPresupuestal(
                anio=ANIO,
                ue_id=ue_map[sigla],
                clasificador_id=clas_map[cod_clas],
                tipo=tipo,
                monto=_dec(monto),
                nota_modificacion=nota,
                fecha=fecha,
                pim_resultante=_dec(pim_res),
            )
        )

    session.bulk_save_objects(registros)
    session.flush()
    print(f"  [OK] ModificacionPresupuestal — {len(registros)} registros insertados.")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the complete seed process within a single database transaction."""
    print("=" * 60)
    print("  Dashboard INEI — Seed Data Script")
    print(f"  Año fiscal: {ANIO}")
    print("=" * 60)

    session = SessionLocal()
    try:
        print("\n[1/10] Unidades Ejecutoras...")
        ues = seed_unidades_ejecutoras(session)

        print("\n[2/10] Usuarios...")
        seed_usuarios(session, ues)

        print("\n[3/10] Metas Presupuestales...")
        metas = seed_metas_presupuestales(session, ues)

        print("\n[4/10] Clasificadores de Gasto...")
        clasificadores = seed_clasificadores_gasto(session)

        print("\n[5/10] Proveedores...")
        proveedores = seed_proveedores(session)

        print("\n[6/10] Actividades Operativas...")
        seed_actividades_operativas(session, ues, metas)

        print("\n[7/10] Programacion Presupuestal...")
        programaciones = seed_programaciones_presupuestales(session, ues, metas, clasificadores)

        print("\n[8/10] Programacion Mensual...")
        seed_programaciones_mensuales(session, programaciones)

        print("\n[9/10] Adquisiciones >8 UIT + Detalles + Procesos...")
        adquisiciones = seed_adquisiciones(session, ues, metas, proveedores)
        seed_adquisicion_detalles(session, adquisiciones)
        seed_adquisicion_procesos(session, adquisiciones)

        print("\n[9b/10] Contratos Menores ≤8 UIT + Procesos...")
        contratos = seed_contratos_menores(session, ues, metas, proveedores)
        seed_contrato_menor_procesos(session, contratos)

        print("\n[10/10] Alertas + Modificaciones Presupuestales...")
        seed_alertas(session, ues)
        seed_modificaciones_presupuestales(session, ues, clasificadores)

        session.commit()
        print("\n" + "=" * 60)
        print("  Seed completado exitosamente.")
        print("=" * 60)

    except Exception as exc:
        session.rollback()
        print(f"\n[ERROR] Seed fallido — se hizo rollback.")
        print(f"  Detalle: {exc}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
