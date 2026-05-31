"""
Script d'orchestration avec Dagster.
Enchaîne séquentiellement les étapes du pipeline :
  1. Ingestion  (ingest.py)
  2. Validation (validate.py)
  3. Transformation dbt (dbt run)
  4. Tests dbt (dbt test)
"""

import os
import sys
import subprocess
from dagster import op, job, In, Nothing

# Ajouter le dossier parent au path pour les imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "pipeline"))

from ingest import ingest as run_ingest
from validate import validate as run_validate

# Localiser l'exécutable dbt dans le même environnement que Python
SCRIPTS_DIR = os.path.dirname(sys.executable)
DBT = os.path.join(SCRIPTS_DIR, "dbt")


@op
def ingest_op(context):
    """Étape 1 : Ingestion des données CSV vers DuckDB."""
    context.log.info(">>> Lancement de l'ingestion...")
    run_ingest()
    context.log.info(">>> Ingestion terminee avec succes.")


@op(ins={"start": In(Nothing)})
def validate_op(context):
    """Étape 2 : Validation des données dans DuckDB."""
    context.log.info(">>> Lancement de la validation...")
    run_validate()
    context.log.info(">>> Validation terminee avec succes.")


@op(ins={"start": In(Nothing)})
def transform(context):
    """Étape 3 : Transformation des données avec dbt (dbt run)."""
    context.log.info(">>> Lancement de dbt run...")
    dbt_dir = os.path.join(BASE_DIR, "dbt_pipeline")
    result = subprocess.run(
        [DBT, "run", "--project-dir", dbt_dir, "--profiles-dir", dbt_dir],
        capture_output=True, text=True
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception(f"dbt run a echoue (code {result.returncode}).")
    context.log.info(">>> dbt run termine avec succes.")


@op(ins={"start": In(Nothing)})
def test_data(context):
    """Étape 4 : Tests de qualité des données avec dbt (dbt test)."""
    context.log.info(">>> Lancement de dbt test...")
    dbt_dir = os.path.join(BASE_DIR, "dbt_pipeline")
    result = subprocess.run(
        [DBT, "test", "--project-dir", dbt_dir, "--profiles-dir", dbt_dir],
        capture_output=True, text=True
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception(f"dbt test a echoue (code {result.returncode}).")
    context.log.info(">>> dbt test termine avec succes.")


@job
def pipeline_job():
    """
    Job Dagster : exécute les 4 étapes du pipeline dans l'ordre.
    ingest -> validate -> transform -> test_data
    """
    test_data(start=transform(start=validate_op(start=ingest_op())))


if __name__ == "__main__":
    # Exécution du job en mode in-process
    result = pipeline_job.execute_in_process()
    if result.success:
        print("\n[OK] Pipeline execute avec succes !")
    else:
        print("\n[ERREUR] Le pipeline a echoue.")
