# etl_pipeline/etl_pipeline/assets/gold_chunks.py

from dagster import asset, Output, MetadataValue, AssetIn
import polars as pl
from datetime import datetime
import os

CHUNK_SIZE    = 250   # số từ mỗi chunk
CHUNK_OVERLAP = 50    # số từ overlap


def split_chunks(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """Sliding window chunking"""
    words  = text.split()
    chunks = []
    i = 0
    while i < len(words):
        chunk = " ".join(words[i:i+size])
        chunks.append(chunk)
        i += size - overlap
        if i + overlap >= len(words):
            break
    return chunks if chunks else [text]


@asset(
    name="gold_yhct_chunks",
    key_prefix=["gold", "chunks"],
    group_name="gold",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={
        "silver_filtered_pages": AssetIn(
            key_prefix=["silver", "pdf"]
        )
    },
    description="Chunking text từ Silver → Gold chunks (250 từ, overlap 50)"
)
def gold_yhct_chunks(context, silver_filtered_pages: pl.DataFrame) -> Output:

    context.log.info(f"📥 Nhận {silver_filtered_pages.shape[0]} trang từ Silver")

    chunks_data = []

    for row in silver_filtered_pages.iter_rows(named=True):
        page_id   = f"{row['doc_id']}_p{row['page_num']:03d}"
        text      = row["page_text"]
        parts     = split_chunks(text)

        for idx, chunk_text in enumerate(parts):
            chunk_id = f"{page_id}_c{idx:03d}"
            chunks_data.append({
                "chunk_id":    chunk_id,
                "page_id":     page_id,
                "doc_id":      row["doc_id"],
                "page_num":    row["page_num"],
                "chunk_index": idx,
                "chunk_text":  chunk_text,
                "word_count":  len(chunk_text.split()),
                "source_file": row["source_file"],
                "gold_time":   datetime.utcnow(),
            })

    df = pl.DataFrame(chunks_data)

    context.log.info(f"✅ Tạo {len(df)} chunks từ {silver_filtered_pages.shape[0]} trang")
    context.log.info(f"   Trung bình: {len(df)/silver_filtered_pages.shape[0]:.1f} chunks/trang")

    preview_df = df.select(["chunk_id", "page_num", "chunk_index", "word_count"]).head(5)

    return Output(
        value=df,
        metadata={
            "total_chunks":   MetadataValue.int(len(df)),
            "total_pages":    MetadataValue.int(silver_filtered_pages.shape[0]),
            "avg_word_count": MetadataValue.float(float(df["word_count"].mean())),
            "preview":        MetadataValue.md(preview_df.to_pandas().to_markdown()),
        }
    )