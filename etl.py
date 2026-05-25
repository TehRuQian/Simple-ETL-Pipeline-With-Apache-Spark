from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from itertools import chain
import psycopg2

# --------------------------------------------------
# Test psycopg2
# --------------------------------------------------
try:
    import psycopg2
    print("psycopg2 imported OK")
except ImportError as e:
    print(f"psycopg2 import FAILED: {e}")

# --------------------------------------------------
# Spark Session
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("ETL - Censo Escolar")
    .config(
        "spark.jars",
        r"C:\spark\jars\postgresql-42.7.3.jar"   # <-- CHANGE THIS
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# --------------------------------------------------
# PostgreSQL config
# --------------------------------------------------
PG_HOST = "postgres"
PG_PORT = 5432
PG_DB = "censo_escolar"
PG_USER = "censo"
PG_PASSWORD = "123"

PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

POSTGRES_CONFIG = {
    "url": PG_URL,
    "properties": {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
}

# --------------------------------------------------
# PostgreSQL helper
# --------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

# --------------------------------------------------
# Load parquet
# --------------------------------------------------
print("Loading parquet...")

data = spark.read.parquet(
    "/app/data/censo_escolar.parquet"   # <-- CHANGE THIS
)

print(f"Total rows: {data.count()}")

# --------------------------------------------------
# Dimension tables
# --------------------------------------------------
INTEGER_DIMENSIONS = [
    "TP_DEPENDENCIA",
    "TP_LOCALIZACAO",
    "IN_AGUA_POTAVEL",
    "IN_ENERGIA_INEXISTENTE",
    "IN_ESGOTO_INEXISTENTE",
    "IN_BANHEIRO",
    "IN_BIBLIOTECA",
    "IN_REFEITORIO",
    "IN_COMPUTADOR",
    "IN_INTERNET",
    "IN_EQUIP_NENHUM"
]

DIMENSION_TABLES_CONFIG = {
    "DIM_LOCAL": {
        "fields": [
            {"field": "NO_UF", "type": "string"},
            {"field": "SG_UF", "type": "string"},
            {"field": "CO_UF", "type": "string"},
            {"field": "NO_MUNICIPIO", "type": "string"},
            {"field": "CO_MUNICIPIO", "type": "string"}
        ]
    }
}

DIMENSION_TABLES_CONFIG.update(
    {
        "DIM_" + dimension.upper(): {
            "fields": [
                {"field": dimension, "type": "integer"}
            ]
        }
        for dimension in INTEGER_DIMENSIONS
    }
)

# --------------------------------------------------
# Write dimensions
# --------------------------------------------------
for table_name, table_config in DIMENSION_TABLES_CONFIG.items():

    print(f"[{datetime.now()}] Writing {table_name}")

    dimension_df = (
        data
        .select(
            [
                F.col(field["field"])
                .cast(field["type"])
                .alias(field["field"])
                for field in table_config["fields"]
            ]
        )
        .distinct()
        .withColumn("id", F.monotonically_increasing_id())
    )

    dimension_df.write.jdbc(
        url=POSTGRES_CONFIG["url"],
        table=table_name,
        mode="overwrite",
        properties=POSTGRES_CONFIG["properties"]
    )

    print(f"[{datetime.now()}] Wrote {table_name}")

    # Add primary key
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            f"ALTER TABLE {table_name} ADD PRIMARY KEY (id);"
        )
        conn.commit()

    except Exception as e:
        print(f"PK already exists or error: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

    print(f"[{datetime.now()}] Done")

# --------------------------------------------------
# Facts table
# --------------------------------------------------
print("Creating facts table...")

FACT_TABLE_NAME = "FACT_CENSO_ESCOLAR"

FACT_COLUMNS = [
    "QT_DOC_BAS",
    "QT_DOC_INF",
    "QT_DOC_FUND",
    "QT_DOC_MED",
    "QT_MAT_BAS",
    "QT_MAT_INF",
    "QT_MAT_FUND",
    "QT_MAT_MED",
    "QT_MAT_BAS_ND",
    "QT_MAT_BAS_BRANCA",
    "QT_MAT_BAS_PRETA",
    "QT_MAT_BAS_PARDA",
    "QT_MAT_BAS_AMARELA",
    "QT_MAT_BAS_INDIGENA",
    "NU_ANO_CENSO"
]

FACT_CONFIG = {
    fact: {
        "fields": [
            {"field": fact, "type": "integer"}
        ]
    }
    for fact in FACT_COLUMNS
}

DIMENSION_ID_CONFIG = {
    table_name: [
        field["field"]
        for field in table_fields["fields"]
    ]
    for table_name, table_fields in DIMENSION_TABLES_CONFIG.items()
}

FACT_TABLE_ALL_COLUMNS_ORDERED = (
    FACT_COLUMNS
    + list(map(lambda col: "ID_" + col, DIMENSION_ID_CONFIG.keys()))
)

# --------------------------------------------------
# Create facts table
# --------------------------------------------------
comma_break_line = ",\n"

conn = get_connection()
cursor = conn.cursor()

try:

    # ----------------------------------------------
    # Create FACT table
    # ----------------------------------------------
    facts_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {FACT_TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        {
            comma_break_line.join(
                [f"{field} INTEGER" for field in FACT_COLUMNS]
                +
                [
                    f"ID_{dim_table} BIGINT"
                    for dim_table in DIMENSION_ID_CONFIG.keys()
                ]
            )
        }
    );
    """

    cursor.execute(facts_table_sql)
    conn.commit()

    print(f"[{datetime.now()}] Created facts table")

    # ----------------------------------------------
    # Add foreign keys
    # ----------------------------------------------
    for dim_table in DIMENSION_ID_CONFIG.keys():

        constraint_name = f"fk_{FACT_TABLE_NAME}_{dim_table}"

        fk_sql = f"""
        ALTER TABLE {FACT_TABLE_NAME}
        ADD CONSTRAINT {constraint_name}
        FOREIGN KEY (ID_{dim_table})
        REFERENCES {dim_table}(id);
        """

        try:

            cursor.execute(fk_sql)
            conn.commit()

            print(
                f"[{datetime.now()}] Added FK: "
                f"{FACT_TABLE_NAME} -> {dim_table}"
            )

        except Exception as fk_error:

            print(
                f"[{datetime.now()}] FK already exists "
                f"or error: {fk_error}"
            )

            conn.rollback()

except Exception as e:

    print(f"ERROR creating fact table: {e}")
    conn.rollback()

finally:

    cursor.close()
    conn.close()

# --------------------------------------------------
# Prepare facts data
# --------------------------------------------------
facts_data = data.select(
    [
        *chain(
            *DIMENSION_ID_CONFIG.values(),
            FACT_CONFIG.keys()
        )
    ]
)

# --------------------------------------------------
# Join dimensions
# --------------------------------------------------
for table_name, table_fields in DIMENSION_ID_CONFIG.items():

    dim_table = (
        spark.read.jdbc(
            url=POSTGRES_CONFIG["url"],
            table=table_name,
            properties=POSTGRES_CONFIG["properties"]
        )
        .withColumnRenamed("id", f"ID_{table_name}")
    )

    facts_data = (
        facts_data
        .join(
            dim_table,
            on=table_fields,
            how="left"
        )
        .drop(*table_fields)
    )

# --------------------------------------------------
# Write facts table
# --------------------------------------------------
try:

    (
        facts_data
        .select(*FACT_TABLE_ALL_COLUMNS_ORDERED)
        .write
        .jdbc(
            url=POSTGRES_CONFIG["url"],
            table=FACT_TABLE_NAME,
            mode="append",
            properties=POSTGRES_CONFIG["properties"]
        )
    )

    print("Facts table written successfully")

except Exception as e:
    print(f"ERROR in facts table: {e}")

print("ETL complete!")

spark.stop()