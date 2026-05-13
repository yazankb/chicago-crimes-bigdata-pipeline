"""Load Stage III/IV data into PostgreSQL for Superset dashboard."""
import psycopg2

DB_CONFIG = {
    "host": "hadoop-04.uni.innopolis.ru",
    "dbname": "team2_projectdb",
    "user": "team2",
    "password": "V2P1hy6zjPqWoXMm",
}


def execute(conn, sql, fetch=False):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        if fetch and cur.description:
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            conn.commit()
            cur.close()
            return cols, rows
        conn.commit()
        print(f"  OK: {sql[:80]}... ({cur.rowcount} rows)")
        cur.close()
        return None
    except Exception as e:
        conn.rollback()
        print(f"  ERROR: {e}")
        cur.close()
        return None


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    print("Connected to PostgreSQL")

    # 1. Model evaluation table
    execute(conn, "DROP TABLE IF EXISTS ml_evaluation CASCADE")
    execute(conn, """
        CREATE TABLE ml_evaluation (
            model VARCHAR(100),
            auc_roc DOUBLE PRECISION,
            auc_pr DOUBLE PRECISION,
            accuracy DOUBLE PRECISION,
            f1 DOUBLE PRECISION
        )
    """)
    execute(conn, """
        INSERT INTO ml_evaluation VALUES
        ('Random Forest', 0.8738, 0.7968, 0.8492, 0.8256),
        ('Gradient Boosted Trees', 0.8844, 0.8176, 0.8809, 0.8710)
    """)

    # 2. Dataset info table (data description)
    execute(conn, "DROP TABLE IF EXISTS dataset_info CASCADE")
    execute(conn, """
        CREATE TABLE dataset_info (
            table_name VARCHAR(100),
            record_count BIGINT,
            column_count INT,
            description TEXT
        )
    """)
    execute(conn, """
        INSERT INTO dataset_info VALUES
        ('crimes', 8549662, 22, 'Raw Chicago crimes 2001-Present'),
        ('crimes_optimized', 8549662, 22, 'Partitioned by year, bucketed by district'),
        ('ml_evaluation', 2, 5, 'Stage III model comparison'),
        ('ml_predictions_rf', 199973, 2, 'RF predictions on test set'),
        ('ml_predictions_gbt', 199973, 2, 'GBT predictions on test set')
    """)

    # 3. Schema info (column types description)
    execute(conn, "DROP TABLE IF EXISTS schema_info CASCADE")
    execute(conn, """
        CREATE TABLE schema_info AS
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'crimes'
    """)

    # 4. Feature importance summary
    execute(conn, "DROP TABLE IF EXISTS ml_features CASCADE")
    execute(conn, """
        CREATE TABLE ml_features (
            feature_group VARCHAR(50),
            feature_count INT,
            description TEXT
        )
    """)
    execute(conn, """
        INSERT INTO ml_features VALUES
        ('Categorical (OHE)', 4, 'primary_type, iucr, location_description, beat'),
        ('Numerical', 5, 'district, community_area, x_coordinate, y_coordinate, domestic'),
        ('Cyclical sin/cos', 6, 'hour_sin, hour_cos, day_sin, day_cos, month_sin, month_cos'),
        ('Geospatial ECEF', 3, 'ecef_x, ecef_y, ecef_z')
    """)

    # 5. Grid search params
    execute(conn, "DROP TABLE IF EXISTS ml_gridsearch CASCADE")
    execute(conn, """
        CREATE TABLE ml_gridsearch (
            model VARCHAR(50),
            param VARCHAR(50),
            values_tested VARCHAR(100)
        )
    """)
    execute(conn, """
        INSERT INTO ml_gridsearch VALUES
        ('RF', 'numTrees', '20, 50'),
        ('RF', 'maxDepth', '5, 10'),
        ('GBT', 'maxDepth', '3, 6'),
        ('GBT', 'stepSize', '0.05, 0.1')
    """)

    # Verify
    print("\nVerification:")
    for tbl in ["ml_evaluation", "dataset_info", "schema_info", "ml_features", "ml_gridsearch"]:
        cols, rows = execute(conn, f"SELECT * FROM {tbl}", fetch=True)
        if cols:
            print(f"  {tbl}: {len(rows)} rows, columns={cols}")
            for row in rows[:3]:
                print(f"    {row}")

    conn.close()
    print("\nPostgreSQL loading complete!")


if __name__ == "__main__":
    main()
