import os
import logging
from datetime import datetime
import dlt
from dlt.sources.helpers import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
DEFAULT_LIMIT = 1000


def _default_start_date() -> str:
    """Devuelve el primer día del año actual como string ISO."""
    return f"{datetime.now().year}-01-01T00:00:00"


@dlt.resource(
    table_name="crimes",
    write_disposition="merge",
    primary_key="id",
)
def chicago_crimes_resource(
    date_cursor=dlt.sources.incremental("date", initial_value=_default_start_date()),
    page_size=DEFAULT_LIMIT,
): 
    offset = 0

    # Formatear el valor del cursor para la query de la API
    cursor_value = date_cursor.last_value
    if hasattr(cursor_value, "isoformat"):
        cursor_str = cursor_value.isoformat()
    else:
        cursor_str = str(cursor_value)

    logger.info(f"Iniciando ingesta incremental desde: {cursor_str}")

    while True:
        params = {
            "$limit": page_size,
            "$offset": offset,
            "$order": "id",
            "$where": f"date > '{cursor_str}'",
        }

        logger.info(f"Solicitando página: offset={offset}, limit={page_size}")
        response = requests.get(BASE_URL, params=params)
        data = response.json()

        if not data:
            logger.info("No hay más datos disponibles.")
            break

        yield data

        offset += page_size


def run_pipeline(
    db_path: str = "chicago_crimes.duckdb",
    start_date: str = None,
    reset_state: bool = False,
): 
    # Normalizar start_date: si no se provee, usar inicio del año actual
    if start_date is None:
        start_date = _default_start_date()
    elif "T" not in start_date:
        start_date = f"{start_date}T00:00:00"

    logger.info(f"Parámetro start_date: {start_date}")
 
    os.environ["DESTINATION__DUCKDB__CREDENTIALS__DATABASE"] = db_path

    logger.info(f"Configurando pipeline DLT → DuckDB: {db_path}")

    pipeline = dlt.pipeline(
        pipeline_name="chicago_crimes_ingestion",
        destination="duckdb",
        dataset_name="raw_crimes",
    )

    # Resetear estado incremental si se solicita, para forzar recarga desde start_date
    if reset_state:
        logger.info("Reseteando estado incremental del pipeline...")
        pipeline.drop()
        logger.info("Estado reseteado. La próxima ejecución cargará desde start_date.")

    load_info = pipeline.run(
        chicago_crimes_resource(
            date_cursor=dlt.sources.incremental("date", initial_value=start_date),
        ),
    )

    logger.info(f"Pipeline completado: {load_info}")
    return load_info


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Chicago Crimes DLT Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter, 
    )
    parser.add_argument(
        "--db-path",
        default="chicago_crimes.duckdb", 
    )
    parser.add_argument(
        "--start-date",
        default=None,
        metavar="YYYY-MM-DD", 
    )
    parser.add_argument(
        "--reset-state",
        action="store_true", 
    )
    args = parser.parse_args()

    run_pipeline(
        db_path=os.path.abspath(args.db_path),
        start_date=args.start_date,
        reset_state=args.reset_state,
    )
