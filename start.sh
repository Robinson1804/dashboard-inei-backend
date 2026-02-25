#!/bin/bash
set -e

echo "=== Running Alembic migrations ==="
alembic upgrade head

echo "=== Running seed data ==="
python seed_data.py || echo "[WARN] seed_data.py failed or already seeded"

echo "=== Running demo transactions seed ==="
python seed_demo_transactions.py || echo "[WARN] seed_demo_transactions.py failed or already seeded"

echo "=== Running 2025 budget seed (33 UEs + 2112 monthly records) ==="
python seed_2025.py || echo "[WARN] seed_2025.py failed or already seeded"

echo "=== Starting Uvicorn on port $PORT ==="
exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
