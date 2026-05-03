# etl_pipeline/etl_pipeline/assets/gold_embeddings.py

from dagster import asset, Output, MetadataValue, AssetIn
import polars as pl
from datetime import datetime
import chromadb
from sentence_transformers import SentenceTransformer


EMBED_MODEL = "keepitreal/vietnamese-sbert"
CHROMA_HOST = "chromadb"
CHROMA_PORT = 8000
COLLECTION  = "yhct_chunks"
BATCH_SIZE  = 32


@asset(
    name="gold_embeddings",
    key_prefix=["gold", "embeddings"],
    group_name="gold",
    # KHÔNG dùng io_manager_key vì asset này write thẳng vào ChromaDB
    # không trả về DataFrame để lưu vào MinIO
    compute_kind="python",
    ins={
        "gold_yhct_chunks": AssetIn(
            key_prefix=["gold", "chunks"]
        )
    },
    description="Embed chunks bằng vietnamese-sbert → ChromaDB"
)
def gold_embeddings(context, gold_yhct_chunks: pl.DataFrame) -> Output:

    context.log.info(f"📥 Nhận {gold_yhct_chunks.shape[0]} chunks để embed")

    # ── Load model ──────────────────────────────────────────────────────────
    context.log.info(f"🤖 Loading model: {EMBED_MODEL} ...")
    model = SentenceTransformer(EMBED_MODEL)
    context.log.info("✅ Model loaded!")

    # ── Kết nối ChromaDB ────────────────────────────────────────────────────
    client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
    col = client.get_or_create_collection(
        name=COLLECTION,
        metadata={"hnsw:space": "cosine"}
    )
    context.log.info(f"✅ Kết nối ChromaDB OK | collection='{COLLECTION}'")

    # ── Lấy ID đã embed để tránh duplicate ──────────────────────────────────
    existing_ids = set()
    current_count = col.count()
    if current_count > 0:
        existing_ids = set(col.get(include=[])["ids"])
    context.log.info(f"📊 Đã có {len(existing_ids)} chunks trong ChromaDB")

    # ── Filter chunks chưa embed ─────────────────────────────────────────────
    rows = [
        row for row in gold_yhct_chunks.iter_rows(named=True)
        if row["chunk_id"] not in existing_ids
    ]
    context.log.info(f"🔄 Cần embed thêm: {len(rows)} chunks")

    if not rows:
        context.log.info("✅ Tất cả chunks đã được embed rồi!")
        return Output(
            value={},
            metadata={
                "total_embedded":  MetadataValue.int(0),
                "collection_size": MetadataValue.int(current_count),
                "status":          "already_done",
            }
        )

    # ── Embed theo batch ─────────────────────────────────────────────────────
    total_done = 0

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]

        texts   = [r["chunk_text"] for r in batch]
        ids     = [r["chunk_id"]   for r in batch]
        metas   = [
            {
                "page_id":  r["page_id"],
                "doc_id":   r["doc_id"],
                "page_num": str(r["page_num"]),   # ChromaDB chỉ nhận str/int/float
                "source":   r["source_file"],
            }
            for r in batch
        ]

        vectors = model.encode(texts, show_progress_bar=False).tolist()

        col.add(
            ids=ids,
            embeddings=vectors,
            documents=texts,
            metadatas=metas,
        )

        total_done += len(batch)
        context.log.info(f"   ✓ {total_done}/{len(rows)} chunks embedded")

    final_count = col.count()
    context.log.info(f"✅ Hoàn thành! Tổng ChromaDB: {final_count} vectors")

    return Output(
        value={},
        metadata={
            "total_embedded":   MetadataValue.int(total_done),
            "collection_size":  MetadataValue.int(final_count),
            "model":            EMBED_MODEL,
            "collection":       COLLECTION,
            "chroma_host":      f"{CHROMA_HOST}:{CHROMA_PORT}",
        }
    )