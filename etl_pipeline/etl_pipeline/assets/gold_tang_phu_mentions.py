# etl_pipeline/etl_pipeline/assets/gold_tang_phu_mentions.py

from dagster import asset, Output, MetadataValue, AssetIn
import polars as pl
from datetime import datetime
from ..resources.dictionaries.tang_phu_dict import TANG_PHU_MAP


def classify_tang_phu(text: str) -> list[str]:
    low   = text.lower()
    found = []
    for tp, keywords in TANG_PHU_MAP.items():
        if any(kw in low for kw in keywords):
            found.append(tp)
    return found


@asset(
    name="gold_tang_phu_mentions",
    key_prefix=["gold", "tang_phu"],
    group_name="gold",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={
        "gold_yhct_chunks": AssetIn(
            key_prefix=["gold", "chunks"]
        )
    },
    description="Phân loại tạng phủ từ Gold chunks → gold_tang_phu_mentions"
)
def gold_tang_phu_mentions(context, gold_yhct_chunks: pl.DataFrame) -> Output:

    context.log.info(f"📥 Nhận {gold_yhct_chunks.shape[0]} chunks")

    records = []
    tp_counts = {}

    for row in gold_yhct_chunks.iter_rows(named=True):
        tang_phus = classify_tang_phu(row["chunk_text"])
        for tp in tang_phus:
            records.append({
                "chunk_id":  row["chunk_id"],
                "doc_id":    row["doc_id"],
                "page_num":  row["page_num"],
                "tang_phu":  tp,
                "gold_time": datetime.utcnow(),
            })
            tp_counts[tp] = tp_counts.get(tp, 0) + 1

    df = pl.DataFrame(records) if records else pl.DataFrame()

    context.log.info(f"✅ Tìm thấy {len(records)} tạng phủ mentions")
    for tp, cnt in sorted(tp_counts.items(), key=lambda x: x[1], reverse=True):
        context.log.info(f"   └─ {tp}: {cnt} chunks")

    return Output(
        value=df,
        metadata={
            "total_mentions": MetadataValue.int(len(records)),
            "tang_phu_stats": MetadataValue.json(tp_counts),
        }
    )