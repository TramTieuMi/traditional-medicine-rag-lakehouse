# superset_config/init_duckdb.py
# Chạy script này để tạo DuckDB database file từ MinIO Parquet

import duckdb
import os

MINIO_ENDPOINT   = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
DB_PATH          = "/app/superset_home/yhct.duckdb"

def init_duckdb():
    print("Đang khởi tạo DuckDB từ MinIO Parquet...")
    
    con = duckdb.connect(DB_PATH)
    
    # Cấu hình S3/MinIO
    con.execute(f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    
    # Tạo views từ Parquet files trên MinIO
    con.execute("""
        CREATE OR REPLACE VIEW bronze_pdf_pages AS
        SELECT * FROM read_parquet(
            's3://yhct-bronze/bronze/pdf/bronze_pdf_pages.parquet'
        );
    """)
    print("✅ bronze_pdf_pages")
    
    con.execute("""
        CREATE OR REPLACE VIEW silver_filtered_pages AS
        SELECT * FROM read_parquet(
            's3://yhct-silver/silver/pdf/silver_filtered_pages.parquet'
        );
    """)
    print("✅ silver_filtered_pages")
    
    con.execute("""
        CREATE OR REPLACE VIEW gold_chunks AS
        SELECT * FROM read_parquet(
            's3://yhct-gold/gold/chunks/gold_yhct_chunks.parquet'
        );
    """)
    print("✅ gold_chunks")
    
    con.execute("""
        CREATE OR REPLACE VIEW gold_herb_mentions AS
        SELECT * FROM read_parquet(
            's3://yhct-gold/gold/herbs/gold_herb_mentions.parquet'
        );
    """)
    print("✅ gold_herb_mentions")
    
    con.execute("""
        CREATE OR REPLACE VIEW gold_tang_phu_mentions AS
        SELECT * FROM read_parquet(
            's3://yhct-gold/gold/tang_phu/gold_tang_phu_mentions.parquet'
        );
    """)
    print("✅ gold_tang_phu_mentions")
    
    # Tạo các bảng analytics tổng hợp
    con.execute("""
        CREATE OR REPLACE TABLE herb_summary AS
        SELECT
            herb_name,
            COUNT(DISTINCT doc_id)     AS doc_count,
            COUNT(DISTINCT chunk_id)   AS chunk_count,
            SUM(count_in_chunk)        AS total_mentions
        FROM gold_herb_mentions
        GROUP BY herb_name
        ORDER BY total_mentions DESC;
    """)
    print("✅ herb_summary")
    
    con.execute("""
        CREATE OR REPLACE TABLE tang_phu_summary AS
        SELECT
            tang_phu,
            COUNT(DISTINCT doc_id)   AS doc_count,
            COUNT(DISTINCT chunk_id) AS chunk_count,
            COUNT(*)                 AS total_mentions
        FROM gold_tang_phu_mentions
        GROUP BY tang_phu
        ORDER BY total_mentions DESC;
    """)
    print("✅ tang_phu_summary")
    
    con.execute("""
        CREATE OR REPLACE TABLE source_summary AS
        SELECT
            source_file,
            COUNT(DISTINCT doc_id) AS doc_count,
            COUNT(*)               AS total_pages,
            SUM(word_count)        AS total_words,
            AVG(word_count)        AS avg_words_per_page
        FROM bronze_pdf_pages
        GROUP BY source_file
        ORDER BY total_pages DESC;
    """)
    print("✅ source_summary")
    
    con.execute("""
        CREATE OR REPLACE TABLE chunk_stats AS
        SELECT
            source_file,
            COUNT(*)            AS total_chunks,
            AVG(word_count)     AS avg_words,
            MIN(word_count)     AS min_words,
            MAX(word_count)     AS max_words
        FROM gold_chunks
        GROUP BY source_file
        ORDER BY total_chunks DESC;
    """)
    print("✅ chunk_stats")
    
    con.close()
    print(f"\n✅ DuckDB đã sẵn sàng tại: {DB_PATH}")

if __name__ == "__main__":
    init_duckdb()