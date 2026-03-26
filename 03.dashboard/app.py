import os
import streamlit as st
import duckdb
import pandas as pd

DB_PATH = os.environ.get(
    "CHICAGO_CRIMES_DB_PATH",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "chicago_crimes.duckdb"),
)


@st.cache_resource
def get_connection():
    """Obtener conexión read-only a DuckDB."""
    return duckdb.connect(DB_PATH, read_only=True)


def query(sql: str) -> pd.DataFrame:
    """Ejecutar una query SQL y devolver un DataFrame."""
    conn = get_connection()
    return conn.sql(sql).df()


def main():
    st.set_page_config(
        page_title="Chicago Crimes Dashboard",
        page_icon="🔍",
        layout="wide",
    )

    st.title("🔍 Chicago Crimes - ETL Dashboard")
    st.markdown("Datos procesados mediante pipeline **DLT -> DBT -> DuckDB**")
 
    try:
        total_df = query("SELECT COUNT(*) AS cnt FROM green.fact_chicago_crimes")
        total_crimes = int(total_df["cnt"].iloc[0])
    except Exception as e:
        st.error(
            f"No se encontraron datos. Ejecuta primero el pipeline ETL con publicación.\n\n"
            f"```\npython orchestrator.py --publish\n```\n\nError: {e}"
        )
        return

    # KPIs
    st.header("📊 Métricas Principales")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Crímenes", f"{total_crimes:,}")

    with col2:
        arrests_df = query(
            "SELECT COUNT(*) AS cnt FROM green.fact_chicago_crimes WHERE arrest = true"
        )
        st.metric("Total Arrestos", f"{int(arrests_df['cnt'].iloc[0]):,}")

    with col3:
        rate_df = query(
            "SELECT ROUND(AVG(CASE WHEN arrest THEN 1.0 ELSE 0.0 END) * 100, 1) AS rate "
            "FROM green.fact_chicago_crimes"
        )
        st.metric("Tasa de Arresto", f"{rate_df['rate'].iloc[0]}%")

    with col4:
        types_df = query(
            "SELECT COUNT(DISTINCT primary_type) AS cnt FROM green.fact_chicago_crimes"
        )
        st.metric("Tipos de Crimen", f"{int(types_df['cnt'].iloc[0])}")

    st.divider()
 
    # Crímenes por Hora & Día de la Semana
    col_left2, col_right2 = st.columns(2)

    with col_left2:
        st.subheader("🕐 Crímenes por Hora del Día")
        df_hourly = query("""
            SELECT crime_hour AS hora, COUNT(*) AS total
            FROM green.fact_chicago_crimes
            GROUP BY crime_hour
            ORDER BY crime_hour
        """)
        st.bar_chart(df_hourly, x="hora", y="total")

    with col_right2:
        st.subheader("📅 Crímenes por Día de la Semana")
        df_daily = query("""
            SELECT
                CASE crime_day_of_week
                    WHEN 0 THEN 'Domingo'
                    WHEN 1 THEN 'Lunes'
                    WHEN 2 THEN 'Martes'
                    WHEN 3 THEN 'Miércoles'
                    WHEN 4 THEN 'Jueves'
                    WHEN 5 THEN 'Viernes'
                    WHEN 6 THEN 'Sábado'
                END AS dia,
                crime_day_of_week,
                COUNT(*) AS total
            FROM green.fact_chicago_crimes
            GROUP BY crime_day_of_week, dia
            ORDER BY crime_day_of_week
        """)
        st.bar_chart(df_daily, x="dia", y="total")

    st.divider()

    # Arresto vs No Arresto
    st.subheader("🚔 Arrestos vs No Arrestos")
    col_a, col_b = st.columns([1, 2])

    df_arrest = query("""
        SELECT
            CASE WHEN arrest THEN 'Arresto' ELSE 'Sin Arresto' END AS estado,
            COUNT(*) AS total
        FROM green.fact_chicago_crimes
        GROUP BY arrest
    """)

    with col_a:
        st.dataframe(df_arrest, use_container_width=True, hide_index=True)

    with col_b:
        st.bar_chart(df_arrest, x="estado", y="total")

    st.divider()

    # Período del Día
    st.subheader("🌅 Crímenes por Período del Día")
    df_period = query("""
        SELECT time_of_day AS periodo, COUNT(*) AS total
        FROM green.fact_chicago_crimes
        GROUP BY time_of_day
        ORDER BY total DESC
    """)
    st.bar_chart(df_period, x="periodo", y="total")

    st.divider()
 
    # Top 10 Ubicaciones
    st.subheader("📍 Top 10 Ubicaciones con más Crímenes")
    df_locations = query("""
        SELECT location_description AS ubicacion, COUNT(*) AS total
        FROM green.fact_chicago_crimes
        WHERE location_description IS NOT NULL
        GROUP BY location_description
        ORDER BY total DESC
        LIMIT 10
    """)
    st.dataframe(df_locations, use_container_width=True, hide_index=True)

    # Footer 
    st.divider()
    st.caption(
        "Fuente: City of Chicago Open Data Portal | "
        "Pipeline: DLT (ingesta) -> DBT (transformación) -> DuckDB (almacenamiento) -> Streamlit (visualización)"
    )


if __name__ == "__main__":
    main()
