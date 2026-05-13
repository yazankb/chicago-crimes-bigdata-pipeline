"""Stage II - EDA: 7 Insights via PySpark, results to output/q*.csv + PostgreSQL"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os, csv, io

spark = SparkSession.builder.appName("Crimes_EDA_Stage2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read from Sqoop Parquet path
df = spark.read.parquet("/user/team2/project/warehouse/crimes")
df.createOrReplaceTempView("crimes")
total = df.count()
print(f"Total records: {total}")

OUTPUT = "output"
os.makedirs(OUTPUT, exist_ok=True)

def save_query(spark_df, label):
    cnt = spark_df.count()
    pandas_df = spark_df.toPandas()
    out_path = f"{OUTPUT}/{label}.csv"
    pandas_df.to_csv(out_path, index=False)
    print(f"  {label}: {cnt} rows -> {out_path}")
    # Also write to PostgreSQL
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="hadoop-04", dbname="team2_projectdb",
            user="team2", password="V2P1hy6zjPqWoXMm", connect_timeout=5
        )
        cur = conn.cursor()
        table_name = f"{label}_results"
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        col_defs = ", ".join([f'"{c}" TEXT' for c in pandas_df.columns])
        cur.execute(f"CREATE TABLE {table_name} ({col_defs})")
        csv_buf = io.StringIO()
        pandas_df.to_csv(csv_buf, index=False, header=False)
        csv_buf.seek(0)
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", csv_buf)
        conn.commit()
        cur.close()
        conn.close()
        print(f"  {label}: stored in PostgreSQL as {table_name}")
    except Exception as e:
        print(f"  {label}: PostgreSQL store skipped ({e})")

# q1: Arrest rate by police district
print("\n=== q1: Arrest Rate by District ===")
q1 = spark.sql("""
  SELECT district, COUNT(*) AS total_crimes,
         SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) AS arrests,
         ROUND(100.0 * SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate
  FROM crimes WHERE district IS NOT NULL
  GROUP BY district ORDER BY district
""")
q1.show(25)
save_query(q1, "q1")

# q2: Top 15 crime types
print("\n=== q2: Top 15 Crime Types ===")
q2 = spark.sql("""
  SELECT primary_type, COUNT(*) AS crime_count
  FROM crimes
  GROUP BY primary_type ORDER BY crime_count DESC LIMIT 15
""")
q2.show(truncate=False)
save_query(q2, "q2")

# q3: Crimes by hour of day
print("\n=== q3: Crimes by Hour of Day ===")
q3 = spark.sql("""
  SELECT HOUR(from_unixtime(`date`)) AS hour_of_day, COUNT(*) AS crime_count
  FROM crimes WHERE `date` IS NOT NULL
  GROUP BY hour_of_day ORDER BY hour_of_day
""")
q3.show(24)
save_query(q3, "q3")

# q4: Arrest rate trend by year
print("\n=== q4: Arrest Rate Trend by Year ===")
q4 = spark.sql("""
  SELECT year, COUNT(*) AS total_crimes,
         SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) AS arrests,
         ROUND(100.0 * SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate
  FROM crimes WHERE year >= 2001
  GROUP BY year ORDER BY year
""")
q4.show(30)
save_query(q4, "q4")

# q5: Crimes by month of year
print("\n=== q5: Crimes by Month ===")
q5 = spark.sql("""
  SELECT MONTH(from_unixtime(`date`)) AS month_num,
         COUNT(*) AS crime_count
  FROM crimes WHERE `date` IS NOT NULL
  GROUP BY month_num ORDER BY month_num
""")
q5.show(12)
save_query(q5, "q5")

# q6: Arrest rate by community area
print("\n=== q6: Arrest Rate by Community Area ===")
q6 = spark.sql("""
  SELECT community_area, COUNT(*) AS total_crimes,
         SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) AS arrests,
         ROUND(100.0 * SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate
  FROM crimes WHERE community_area IS NOT NULL
  GROUP BY community_area ORDER BY community_area
""")
q6.show(20)
save_query(q6, "q6")

# q7: Domestic vs non-domestic arrest rates
print("\n=== q7: Domestic vs Non-Domestic ===")
q7 = spark.sql("""
  SELECT domestic, COUNT(*) AS total_crimes,
         SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) AS arrests,
         ROUND(100.0 * SUM(CASE WHEN arrest = true THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate
  FROM crimes WHERE domestic IS NOT NULL
  GROUP BY domestic ORDER BY domestic
""")
q7.show()
save_query(q7, "q7")

print("\n=== EDA Complete ===")
spark.stop()
