# etl_pipeline/etl_pipeline/assets/gold_tang_phu_mentions.py
#
# Phát hiện chunk đề cập tạng phủ nào bằng regex compound terms.
#
# Tại sao dùng regex cứng ở đây (khác với gold_herb_mentions):
#   - 5 hệ tạng phủ là cấu trúc lý luận YHCT cố định, không thay đổi khi thêm PDF mới.
#   - Mỗi tạng phủ nhận dạng bằng cụm từ lâm sàng đặc trưng ("tỳ hư", "can khí uất kết"…)
#     – không phải tên riêng cần discovery như dược liệu.
#   - Regex compound terms đủ chính xác, tránh false positive với từ đơn ("tâm" trong "tâm lý").

import re

import polars as pl
from dagster import AssetIn, MetadataValue, Output, asset

# ── Patterns phát hiện 5 hệ tạng phủ ─────────────────────────────────────────
# Dùng cụm từ lâm sàng YHCT – tránh match từ đơn ngữ nghĩa mơ hồ
# (VD: "can" có thể là "gan" hoặc xuất hiện trong "cần", "cân"…)
TANG_PHU_PATTERNS: dict[str, re.Pattern] = {

    # Tỳ Vị – Spleen / Stomach
    "ty_vi": re.compile(
        r"tỳ\s+vị|tỳ\s+hư|tỳ\s+hàn|tỳ\s+nhiệt|tỳ\s+khí|tỳ\s+dương|tỳ\s+âm|"
        r"kiện\s+tỳ|ôn\s+tỳ|bổ\s+tỳ|hòa\s+tỳ|táo\s+thấp\s+kiện\s+tỳ|"
        r"vị\s+hàn|vị\s+nhiệt|vị\s+khí|vị\s+âm|hòa\s+vị|ôn\s+vị|hàng\s+vị|"
        r"dạ\s+dày|đầy\s+bụng|chướng\s+bụng|ăn\s+không\s+tiêu|tiêu\s+hóa\s+kém",
        re.IGNORECASE,
    ),

    # Can Đởm – Liver / Gallbladder
    "can_dom": re.compile(
        r"can\s+âm|can\s+dương|can\s+khí|can\s+hỏa|can\s+huyết|can\s+phong|can\s+hàn|"
        r"bình\s+can|sơ\s+can|thanh\s+can|bổ\s+can|dưỡng\s+can|ức\s+can|"
        r"can\s+khí\s+uất\s+kết|can\s+dương\s+thượng\s+kháng|can\s+phong\s+nội\s+động|"
        r"đởm\s+nhiệt|đởm\s+hàn|đởm\s+hư|thanh\s+đởm|lợi\s+đởm|"
        r"gan\s+mật|vàng\s+da|ứ\s+mật|viêm\s+gan",
        re.IGNORECASE,
    ),

    # Thận (+ Bàng quang) – Kidney / Bladder
    "than": re.compile(
        r"thận\s+âm|thận\s+dương|thận\s+khí|thận\s+hư|thận\s+tinh|"
        r"thận\s+nhiệt|thận\s+hàn|thận\s+không\s+nạp\s+khí|"
        r"bổ\s+thận|ôn\s+thận|thanh\s+thận|tư\s+thận|ích\s+thận|tư\s+bổ\s+thận\s+âm|"
        r"thận\s+dương\s+hư|thận\s+âm\s+hư|mệnh\s+môn\s+hỏa\s+suy|"
        r"bàng\s+quang|di\s+niệu|di\s+tinh|đái\s+dầm|tiểu\s+tiện\s+không\s+thông",
        re.IGNORECASE,
    ),

    # Phế (+ Đại tràng) – Lung / Large Intestine
    "phe_dai_trang": re.compile(
        r"phế\s+âm|phế\s+khí|phế\s+nhiệt|phế\s+hư|phế\s+hàn|phế\s+táo|phế\s+ứ|"
        r"thanh\s+phế|ôn\s+phế|bổ\s+phế|tuyên\s+phế|túc\s+phế|nhuận\s+phế|phế\s+khí\s+hư|"
        r"đại\s+tràng\s+hư|đại\s+tràng\s+nhiệt|nhuận\s+tràng|thông\s+tiện|"
        r"ho\s+khan|ho\s+có\s+đờm|khó\s+thở|đờm\s+nhiệt|táo\s+bón",
        re.IGNORECASE,
    ),

    # Tâm (+ Tiểu tràng) – Heart / Small Intestine
    "tam_tieu_trang": re.compile(
        r"tâm\s+âm|tâm\s+dương|tâm\s+khí|tâm\s+hỏa|tâm\s+huyết|"
        r"tâm\s+thần|tâm\s+thận|tâm\s+tỳ|"
        r"dưỡng\s+tâm|an\s+tâm|thanh\s+tâm|bổ\s+tâm|trấn\s+tâm|bình\s+tâm|"
        r"tâm\s+hỏa\s+vượng|tâm\s+khí\s+hư|tâm\s+huyết\s+hư|tâm\s+dương\s+hư|"
        r"tiểu\s+tràng\s+hư|tiểu\s+tràng\s+nhiệt|"
        r"mất\s+ngủ|hồi\s+hộp|đánh\s+trống\s+ngực|hay\s+quên",
        re.IGNORECASE,
    ),
}


# ── Dagster asset ─────────────────────────────────────────────────────────────

@asset(
    name="gold_tang_phu_mentions",
    key_prefix=["gold", "tang_phu"],
    group_name="gold",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={"gold_yhct_chunks": AssetIn(key_prefix=["gold", "chunks"])},
    description=(
        "Detect organ-system (Tạng Phủ) mentions in Gold chunks using YHCT clinical compound terms. "
        "Categories: ty_vi, can_dom, than, phe_dai_trang, tam_tieu_trang. "
        "One chunk can belong to multiple organ systems."
    ),
)
def gold_tang_phu_mentions(context, gold_yhct_chunks: pl.DataFrame) -> Output:
    """
    Phát hiện mỗi chunk đề cập tạng phủ nào bằng regex compound terms YHCT.

    Một chunk có thể xuất hiện trong nhiều tạng phủ (VD: bài thuốc điều trị
    cả tỳ lẫn thận) → mỗi cặp (tang_phu, chunk_id) tạo một row riêng.

    Schema output: tang_phu, chunk_id, doc_id, source_file
    """
    context.log.info(f"📥 Nhận {gold_yhct_chunks.shape[0]} chunks từ gold_yhct_chunks")

    rows: list[dict] = []

    for row in gold_yhct_chunks.iter_rows(named=True):
        text = row["chunk_text"]
        for tang_phu, pattern in TANG_PHU_PATTERNS.items():
            if pattern.search(text):
                rows.append({
                    "tang_phu":    tang_phu,
                    "chunk_id":    row["chunk_id"],
                    "doc_id":      row["doc_id"],
                    "source_file": row["source_file"],
                })

    if rows:
        df = pl.DataFrame(rows)
    else:
        df = pl.DataFrame({
            "tang_phu":    pl.Series([], dtype=pl.Utf8),
            "chunk_id":    pl.Series([], dtype=pl.Utf8),
            "doc_id":      pl.Series([], dtype=pl.Utf8),
            "source_file": pl.Series([], dtype=pl.Utf8),
        })

    # Tổng hợp để log
    summary = (
        df.group_by("tang_phu")
        .agg(pl.len().alias("chunk_count"))
        .sort("chunk_count", descending=True)
    ) if df.shape[0] > 0 else pl.DataFrame({"tang_phu": [], "chunk_count": []})

    context.log.info(
        f"✅ {df.shape[0]} rows | "
        f"{df['chunk_id'].n_unique() if df.shape[0] > 0 else 0} chunks đề cập tạng phủ"
    )
    for r in summary.iter_rows(named=True):
        context.log.info(f"   {r['tang_phu']}: {r['chunk_count']} chunks")

    return Output(
        value=df,
        metadata={
            "total_rows":    MetadataValue.int(df.shape[0]),
            "unique_chunks": MetadataValue.int(
                df["chunk_id"].n_unique() if df.shape[0] > 0 else 0
            ),
            "summary":       MetadataValue.md(summary.to_pandas().to_markdown()),
        },
    )
