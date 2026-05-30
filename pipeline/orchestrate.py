"""
Script d'orchestration avec Dagster.
Enchaîne séquentiellement les étapes du pipeline :
  1. Ingestion  (ingest.py)
  2. Validation (validate.py)
  3. Transformation dbt (dbt run)
  4. Tests dbt (dbt test)
"""

import os
from dagster import op, job


# Répertoire racine du projet
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@op
def ingest(context):
    """Étape 1 : Ingestion des données CSV vers DuckDB."""
    context.log.info(">>> Lancement de l'ingestion...")
    script = os.path.join(BASE_DIR, "pipeline", "ingest.py")
    exit_code = os.system(f"python \"{script}\"")
    if exit_code != 0:
        raise Exception("L'ingestion a échoué.")
    context.log.info(">>> Ingestion terminée avec succès.")


@op
def validate(context):
    """Étape 2 : Validation des données dans DuckDB."""
    context.log.info(">>> Lancement de la validation...")
    script = os.path.join(BASE_DIR, "pipeline", "validate.py")
    exit_code = os.system(f"python \"{script}\"")
    if exit_code != 0:
        raise Exception("La validation a échoué.")
    context.log.info(">>> Validation terminée avec succès.")


@op
def transform(context):
    """Étape 3 : Transformation des données avec dbt (dbt run)."""
    context.log.info(">>> Lancement de dbt run...")
    dbt_dir = os.path.join(BASE_DIR, "dbt_pipeline")
    exit_code = os.system(f"dbt run --project-dir \"{dbt_dir}\" --profiles-dir \"{dbt_dir}\"")
    if exit_code != 0:
        raise Exception("dbt run a échoué.")
    context.log.info(">>> dbt run terminé avec succès.")


@op
def test_data(context):
    """Étape 4 : Tests de qualité des données avec dbt (dbt test)."""
    context.log.info(">>> Lancement de dbt test...")
    dbt_dir = os.path.join(BASE_DIR, "dbt_pipeline")
    exit_code = os.system(f"dbt test --project-dir \"{dbt_dir}\" --profiles-dir \"{dbt_dir}\"")
    if exit_code != 0:
        raise Exception("dbt test a échoué.")
    context.log.info(">>> dbt test terminé avec succès.")


@job
def pipeline_job():
    """
    Job Dagster : exécute les 4 étapes du pipeline dans l'ordre.
    ingest -> validate -> transform -> test_data
    """
    result_ingest = ingest()
    result_validate = validate(result_ingest)
    result_transform = transform(result_validate)
    test_data(result_transform)


if __name__ == "__main__":
    # Exécution du job en mode in-process
    result = pipeline_job.execute_in_process()
    if result.success:
        print("\n✅ Pipeline exécuté avec succès !")
    else:
        print("\n❌ Le pipeline a échoué.")
