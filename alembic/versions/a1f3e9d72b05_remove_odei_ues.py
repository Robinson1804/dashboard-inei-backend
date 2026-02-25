"""remove_odei_ues

Elimina todas las UEs ODEI (sigla LIKE 'ODEI-%') y sus datos relacionados.
Las 8 UEs centrales (OTIN, DEC, OTA, OTPP, DNCE, DNCPP, DNEL, DTI) no se tocan.

Revision ID: a1f3e9d72b05
Revises: bc0da26fecd4
Create Date: 2026-02-25 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'a1f3e9d72b05'
down_revision: Union[str, None] = 'bc0da26fecd4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()

    # Verificar cuantas UEs ODEI existen
    result = conn.execute(sa.text(
        "SELECT COUNT(*) FROM unidad_ejecutora WHERE sigla LIKE 'ODEI-%'"
    ))
    count = result.scalar()

    if count == 0:
        print("[MIGRATION] No hay UEs ODEI que eliminar. Saltando.")
        return

    print(f"[MIGRATION] Eliminando {count} UEs ODEI y sus datos relacionados...")

    # Paso 1: adquisicion_proceso
    conn.execute(sa.text("""
        DELETE FROM adquisicion_proceso
        WHERE adquisicion_id IN (
            SELECT id FROM adquisicion
            WHERE ue_id IN (SELECT id FROM unidad_ejecutora WHERE sigla LIKE 'ODEI-%')
        )
    """))

    # Paso 2: adquisicion_detalle
    conn.execute(sa.text("""
        DELETE FROM adquisicion_detalle
        WHERE adquisicion_id IN (
            SELECT id FROM adquisicion
            WHERE ue_id IN (SELECT id FROM unidad_ejecutora WHERE sigla LIKE 'ODEI-%')
        )
    """))

    # Paso 3: adquisicion
    conn.execute(sa.text("""
        DELETE FROM adquisicion
        WHERE ue_id IN (SELECT id FROM unidad_ejecutora WHERE sigla LIKE 'ODEI-%')
    """))

    # Paso 4: contrato_menor_proceso
    conn.execute(sa.text("""
        DELETE FROM contrato_menor_proceso
        WHERE contrato_menor_id IN (
            SELECT id FROM contrato_menor
            WHERE ue_id IN (SELECT id FROM unidad_ejecutora WHERE sigla LIKE 'ODEI-%')
        )
    """))

    # Paso 5: contrato_menor
    conn.execute(sa.text("""
        DELETE FROM contrato_menor
        WHERE ue_id IN (SELECT id FROM unidad_ejecutora WHERE sigla LIKE 'ODEI-%')
    """))

    # Paso 6: actividad_operativa
    conn.execute(sa.text("""
        DELETE FROM actividad_operativa
        WHERE ue_id IN (SELECT id FROM unidad_ejecutora WHERE sigla LIKE 'ODEI-%')
    """))

    # Paso 7: alerta
    conn.execute(sa.text("""
        DELETE FROM alerta
        WHERE ue_id IN (SELECT id FROM unidad_ejecutora WHERE sigla LIKE 'ODEI-%')
    """))

    # Paso 8: programacion_mensual
    conn.execute(sa.text("""
        DELETE FROM programacion_mensual
        WHERE programacion_presupuestal_id IN (
            SELECT pp.id FROM programacion_presupuestal pp
            JOIN meta_presupuestal mp ON pp.meta_presupuestal_id = mp.id
            WHERE mp.ue_id IN (SELECT id FROM unidad_ejecutora WHERE sigla LIKE 'ODEI-%')
        )
    """))

    # Paso 9: programacion_presupuestal
    conn.execute(sa.text("""
        DELETE FROM programacion_presupuestal
        WHERE meta_presupuestal_id IN (
            SELECT id FROM meta_presupuestal
            WHERE ue_id IN (SELECT id FROM unidad_ejecutora WHERE sigla LIKE 'ODEI-%')
        )
    """))

    # Paso 10: meta_presupuestal
    conn.execute(sa.text("""
        DELETE FROM meta_presupuestal
        WHERE ue_id IN (SELECT id FROM unidad_ejecutora WHERE sigla LIKE 'ODEI-%')
    """))

    # Paso 11: usuario (si tiene FK a ue_id)
    conn.execute(sa.text("""
        DELETE FROM usuario
        WHERE ue_id IN (SELECT id FROM unidad_ejecutora WHERE sigla LIKE 'ODEI-%')
    """))

    # Paso 12: unidad_ejecutora ODEI
    conn.execute(sa.text("""
        DELETE FROM unidad_ejecutora WHERE sigla LIKE 'ODEI-%'
    """))

    # Verificar resultado
    result = conn.execute(sa.text("SELECT COUNT(*) FROM unidad_ejecutora"))
    remaining = result.scalar()
    print(f"[MIGRATION] Completado. UEs restantes: {remaining}")


def downgrade() -> None:
    # No reversible — los datos ODEI no deben restaurarse
    pass
