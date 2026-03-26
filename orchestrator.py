import os
import sys
import subprocess
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Rutas del proyecto
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PROJECT_ROOT, "chicago_crimes.duckdb")
DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "02.transformation", "chicago_crimes")


def run_ingestion(start_date: str = None, reset_state: bool = False):
    """Paso 1: Ejecutar pipeline de ingesta con DLT."""
    logger.info("=" * 60)
    logger.info("PASO 1: Pipeline de Ingesta (DLT → DuckDB)")
    logger.info("=" * 60)

    sys.path.insert(0, os.path.join(PROJECT_ROOT, "01.ingestion"))
    from chicago_crimes_pipeline import run_pipeline

    load_info = run_pipeline(db_path=DB_PATH, start_date=start_date, reset_state=reset_state)
    logger.info(f"Ingesta completada: {load_info}")
    return load_info


def run_transformation():
    """Paso 2: Ejecutar pipeline de transformación con DBT (target blue)."""
    logger.info("=" * 60)
    logger.info("PASO 2: Pipeline de Transformación (DBT → target blue)")
    logger.info("=" * 60)

    # Configurar la ruta de DuckDB para DBT (usado en profiles.yml)
    os.environ["CHICAGO_CRIMES_DB_PATH"] = DB_PATH

    # Instalar paquetes de DBT
    logger.info("Instalando paquetes DBT...")
    subprocess.run(
        [
            "dbt", "deps",
            "--project-dir", DBT_PROJECT_DIR,
            "--profiles-dir", DBT_PROJECT_DIR,
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )

    # Ejecutar modelos DBT en target blue (Write)
    logger.info("Ejecutando modelos DBT (target blue - Write)...")
    result = subprocess.run(
        [
            "dbt", "run",
            "--target", "blue",
            "--project-dir", DBT_PROJECT_DIR,
            "--profiles-dir", DBT_PROJECT_DIR,
        ],
        check=True,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)

    # Ejecutar tests DBT en target blue (Audit)
    logger.info("Ejecutando tests DBT (target blue - Audit)...")
    test_result = subprocess.run(
        [
            "dbt", "test",
            "--target", "blue",
            "--project-dir", DBT_PROJECT_DIR,
            "--profiles-dir", DBT_PROJECT_DIR,
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    logger.info(test_result.stdout)
    if test_result.returncode != 0:
        logger.warning(f"Algunos tests fallaron:\n{test_result.stderr}")

    return result


def run_publish():
    """Paso 3: Publicar datos de blue -> green (WAP Publish)."""
    logger.info("=" * 60)
    logger.info("PASO 3: Publicación WAP (blue → green)")
    logger.info("=" * 60)

    os.environ["CHICAGO_CRIMES_DB_PATH"] = DB_PATH

    result = subprocess.run(
        [
            "dbt", "run-operation", "publish",
            "--target", "green",
            "--project-dir", DBT_PROJECT_DIR,
            "--profiles-dir", DBT_PROJECT_DIR,
        ],
        check=True,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)

    logger.info("Publicación WAP completada: blue -> green")
    return result


def main():
    """Punto de entrada principal del orquestador."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Orquestador ETL Chicago Crimes",
        formatter_class=argparse.RawDescriptionHelpFormatter, 
    )
    parser.add_argument(
        "--start-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Fecha de inicio para la ingesta (default: 1 de enero del año actual). "
             "Solo aplica en la primera ejecución o con --reset-state.",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Elimina el estado incremental guardado y recarga desde --start-date.",
    )
    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="Saltar la fase de ingesta (usar datos existentes)",
    )
    parser.add_argument(
        "--skip-transformation",
        action="store_true",
        help="Saltar la fase de transformación",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Publicar datos de blue → green (WAP Publish). "
             "Copia las tablas del datamart de blue a green.",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Iniciando Pipeline ETL - Chicago Crimes")
    logger.info(f"Directorio del proyecto: {PROJECT_ROOT}")
    logger.info(f"Base de datos: {DB_PATH}")
    logger.info("=" * 60)

    try:
        # Paso 1: Ingesta
        if not args.skip_ingestion:
            run_ingestion(
                start_date=args.start_date,
                reset_state=args.reset_state,
            )
        else:
            logger.info("Saltando fase de ingesta...")

        # Paso 2: Transformación
        if not args.skip_transformation:
            run_transformation()
        else:
            logger.info("Saltando fase de transformación...")

        # Paso 3: Publicación WAP (blue → green)
        if args.publish:
            run_publish()
        else:
            logger.info("Sin publicación WAP. Usa --publish para promover blue → green.")

        # Resumen final
        logger.info("=" * 60)
        logger.info("Pipeline ETL completado exitosamente!")
        logger.info(f"Base de datos: {DB_PATH}")
        logger.info("Para iniciar el dashboard ejecuta:")
        logger.info(f"  cd '{PROJECT_ROOT}' && streamlit run 03.dashboard/app.py")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Pipeline fallido: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
