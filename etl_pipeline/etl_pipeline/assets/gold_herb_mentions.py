# etl_pipeline/etl_pipeline/assets/gold_herb_mentions.py

from dagster import asset, Output, MetadataValue, AssetIn
import polars as pl
from datetime import datetime
from pathlib import Path


def load_herbs() -> list[str]:
    path = Path(__file__).parent.parent / "resources" / "dictionaries" / "herbs_dict.txt"
    return [h.strip() for h in path.read_text(encoding="utf-8").splitlines() if h.strip()]


def extract_herbs(text: str, herbs: list[str]) -> dict[str, int]:
    low   = text.lower()
    found = {}
    for herb in herbs:
        cnt = low.count(herb.lower())
        if cnt > 0:
            found[herb] = cnt
    return found


@asset(
    name="gold_herb_mentions",
    key_prefix=["gold", "herbs"],
    group_name="gold",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={
        "gold_yhct_chunks": AssetIn(
            key_prefix=["gold", "chunks"]
        )
    },
    description="Extract dược liệu từ Gold chunks → gold_herb_mentions"
)
def gold_herb_mentions(context, gold_yhct_chunks: pl.DataFrame) -> Output:

    herbs_list = load_herbs()
    context.log.info(f"📚 Danh sách dược liệu: {len(herbs_list)} vị")
    context.log.info(f"📥 Nhận {gold_yhct_chunks.shape[0]} chunks")

    records = []
    herb_total_counts = {}

    for row in gold_yhct_chunks.iter_rows(named=True):
        found = extract_herbs(row["chunk_text"], herbs_list)
        for herb, cnt in found.items():
            records.append({
                "chunk_id":      row["chunk_id"],
                "doc_id":        row["doc_id"],
                "page_num":      row["page_num"],
                "herb_name":     herb,
                "count_in_chunk": cnt,
                "gold_time":     datetime.utcnow(),
            })
            herb_total_counts[herb] = herb_total_counts.get(herb, 0) + cnt

    df = pl.DataFrame(records) if records else pl.DataFrame()

    # Top 10 dược liệu xuất hiện nhiều nhất
    top10 = sorted(herb_total_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    top10_str = "\n".join([f"  {h}: {c} lần" for h, c in top10])

    context.log.info(f"✅ Tìm thấy {len(records)} herb mentions")
    context.log.info(f"🌿 Top 10 dược liệu:\n{top10_str}")

    return Output(
        value=df,
        metadata={
            "total_mentions":  MetadataValue.int(len(records)),
            "unique_herbs":    MetadataValue.int(len(herb_total_counts)),
            "top10_herbs":     MetadataValue.md(
                "\n".join([f"- **{h}**: {c} lần" for h, c in top10])
            ),
        }
    )