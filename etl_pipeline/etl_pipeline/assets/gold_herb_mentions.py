# etl_pipeline/etl_pipeline/assets/gold_herb_mentions.py
#
# Two-pass herb extraction (trích xuất dược liệu 2 bước):
#   Pass 1 – LLM discovery : LLM đọc sample chunks → xây vocabulary từ dữ liệu thực tế
#   Pass 2 – Regex counting: dùng vocabulary vừa xây để scan toàn bộ chunks (nhanh, không tốn thêm API call)
#
# Flexible với PDF mới: mỗi lần pipeline chạy lại, vocabulary được xây lại từ đầu từ nội dung hiện tại.

import json
import os
import re
import time

import polars as pl
from dagster import AssetIn, MetadataValue, Output, asset

# ── Trigger keywords ──────────────────────────────────────────────────────────
# Lọc trước những chunk "có mùi dược liệu" để đưa vào LLM discovery.
# Không cần hoàn hảo – chỉ cần bắt được phần lớn chunk liên quan.
_HERB_TRIGGER = re.compile(
    r"vị\s+thuốc|dược\s+liệu|cây\s+thuốc|thảo\s+dược|"
    r"bài\s+thuốc|thang\s+thuốc|dùng\s+thuốc|"
    r"chủ\s+dược|tá\s+dược|quân\s+dược|"
    r"sắc\s+uống|tán\s+bột|ngâm\s+rượu|"
    r"liều\s+dùng|công\s+năng|chủ\s+trị",
    re.IGNORECASE,
)

# Từ chung – loại khỏi vocabulary để tránh nhiễu
_VOCAB_NOISE = {
    "thuốc", "vị", "dược liệu", "cây thuốc", "thảo dược",
    "bài thuốc", "thang thuốc", "đông y", "y học cổ truyền",
    "dược", "liệu", "vị thuốc", "thảo", "hoa", "lá", "rễ",
}

# Giới hạn để kiểm soát chi phí API (Groq free tier: ~30 req/min)
_DISCOVERY_SAMPLE = 150   # tối đa bao nhiêu trigger-chunk gửi LLM
_BATCH_SIZE       = 5     # số chunk mỗi lần gọi LLM
_SLEEP_SEC        = 2.1   # giây nghỉ giữa các batch


# ── Helpers ───────────────────────────────────────────────────────────────────

def _call_llm_batch(texts: list[str], client) -> set[str]:
    """
    Gọi LLM trích xuất dược liệu từ một batch chunks.

    Prompt yêu cầu JSON để dễ parse; fallback sang parse từng dòng nếu LLM
    không trả JSON đúng format.
    """
    joined = "\n\n---\n\n".join(
        f"Đoạn {i + 1}:\n{t[:600]}" for i, t in enumerate(texts)
    )
    prompt = (
        'Từ các đoạn văn y học cổ truyền dưới đây, liệt kê TẤT CẢ tên dược liệu Đông Y.\n'
        'Bao gồm cả tên Hán Việt (cam thảo, đương quy, bạch truật...) '
        'và tên dân gian Việt Nam (gừng, nghệ, ngải cứu...).\n'
        'Trả về JSON: {"herbs": ["tên1", "tên2", ...]}\n'
        'Nếu không tìm thấy dược liệu nào, trả về: {"herbs": []}\n\n'
        f"{joined}"
    )
    try:
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=500,
        )
        return _parse_herb_response(resp.choices[0].message.content)
    except Exception as exc:
        # Không crash cả pipeline nếu một batch lỗi
        return set()


def _parse_herb_response(content: str) -> set[str]:
    """
    Parse response của LLM → set tên dược liệu (lowercase).
    Thử JSON trước, fallback sang parse từng dòng.
    """
    content = content.strip()
    herbs: set[str] = set()

    # Thử tìm JSON object { ... }
    try:
        m = re.search(r"\{.*\}", content, re.DOTALL)
        if m:
            data = json.loads(m.group())
            for v in data.values():
                if isinstance(v, list):
                    herbs = {str(h).strip().lower() for h in v if str(h).strip()}
                    if herbs:
                        return herbs
    except (json.JSONDecodeError, ValueError):
        pass

    # Thử tìm JSON array [ ... ]
    try:
        m = re.search(r"\[.*\]", content, re.DOTALL)
        if m:
            data = json.loads(m.group())
            if isinstance(data, list):
                return {str(h).strip().lower() for h in data if str(h).strip()}
    except (json.JSONDecodeError, ValueError):
        pass

    # Fallback: mỗi dòng là một tên dược liệu
    for line in content.splitlines():
        line = re.sub(r"^[\s\d\.\-\*\•\"\']+", "", line).strip().strip('",')
        if line and 2 <= len(line) <= 40 and line.upper() not in {"NONE", "N/A", ""}:
            herbs.add(line.lower())

    return herbs


def _clean_vocabulary(raw: set[str]) -> set[str]:
    """Loại bỏ noise – giữ lại những chuỗi có vẻ là tên dược liệu thật."""
    return {
        h for h in raw
        if 2 <= len(h) <= 40
        and h not in _VOCAB_NOISE
        # Bỏ chuỗi chỉ toàn số hoặc ký tự đặc biệt
        and re.search(r"[a-zA-ZÀ-ỹ]", h)
    }


def _build_regex(vocabulary: set[str]) -> re.Pattern | None:
    """Compile toàn bộ vocabulary thành một regex duy nhất (greedy – dài trước)."""
    if not vocabulary:
        return None
    sorted_vocab = sorted(vocabulary, key=len, reverse=True)
    return re.compile(
        r"(?<!\w)(" + "|".join(re.escape(h) for h in sorted_vocab) + r")(?!\w)",
        re.IGNORECASE,
    )


# ── Dagster asset ─────────────────────────────────────────────────────────────

@asset(
    name="gold_herb_mentions",
    key_prefix=["gold", "herbs"],
    group_name="gold",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={"gold_yhct_chunks": AssetIn(key_prefix=["gold", "chunks"])},
    description=(
        "Extract herb mentions from Gold chunks via two-pass strategy: "
        "LLM discovery builds a dynamic vocabulary from actual PDF content, "
        "then regex counts occurrences across all chunks."
    ),
)
def gold_herb_mentions(context, gold_yhct_chunks: pl.DataFrame) -> Output:
    """
    Trích xuất dược liệu theo 2 bước:

    Bước 1 – LLM Discovery:
        Gửi sample chunks (ưu tiên chunk có trigger keywords) lên Groq.
        LLM trả về danh sách dược liệu → xây vocabulary động từ dữ liệu thực.
        Vocabulary hoàn toàn derive từ nội dung PDF, không hardcode.

    Bước 2 – Regex Counting:
        Compile vocabulary thành regex.
        Scan toàn bộ chunks → đếm số lần xuất hiện mỗi dược liệu mỗi chunk.
        Nhanh, không tốn thêm API call.

    Schema output: herb_name, chunk_id, doc_id, source_file, count_in_chunk
    """
    groq_api_key = os.getenv("GROQ_API_KEY", "")
    if not groq_api_key:
        raise RuntimeError(
            "Thiếu GROQ_API_KEY – pass 1 (LLM discovery) không thể chạy. "
            "Set biến môi trường GROQ_API_KEY trong Docker Compose."
        )

    from groq import Groq
    client = Groq(api_key=groq_api_key)

    context.log.info(f"📥 Nhận {gold_yhct_chunks.shape[0]} chunks từ gold_yhct_chunks")

    # ── Bước 1: LLM Discovery ─────────────────────────────────────────────────
    context.log.info("🔍 Bước 1: LLM discovery – xây vocabulary từ nội dung PDF...")

    all_rows = gold_yhct_chunks.iter_rows(named=True)
    trigger_chunks = [r for r in all_rows if _HERB_TRIGGER.search(r["chunk_text"])]
    context.log.info(
        f"   Chunks có trigger keywords: {len(trigger_chunks)} / {gold_yhct_chunks.shape[0]}"
    )

    # Sample đều nếu quá nhiều (ưu tiên trải đều toàn bộ tài liệu)
    if len(trigger_chunks) > _DISCOVERY_SAMPLE:
        step = max(1, len(trigger_chunks) // _DISCOVERY_SAMPLE)
        trigger_chunks = trigger_chunks[::step][:_DISCOVERY_SAMPLE]
        context.log.info(f"   Đã sample còn {len(trigger_chunks)} chunks để gửi LLM")

    raw_vocabulary: set[str] = set()
    llm_calls = 0

    for i in range(0, len(trigger_chunks), _BATCH_SIZE):
        batch      = trigger_chunks[i : i + _BATCH_SIZE]
        batch_text = [r["chunk_text"] for r in batch]
        found      = _call_llm_batch(batch_text, client)
        raw_vocabulary.update(found)
        llm_calls += 1
        context.log.info(
            f"   Batch {llm_calls}: +{len(found)} herbs → vocab tạm = {len(raw_vocabulary)}"
        )
        # Nghỉ giữa các batch để tránh vượt rate limit Groq free tier
        if i + _BATCH_SIZE < len(trigger_chunks):
            time.sleep(_SLEEP_SEC)

    vocabulary = _clean_vocabulary(raw_vocabulary)
    context.log.info(
        f"✅ Vocabulary cuối: {len(vocabulary)} dược liệu "
        f"(sau lọc noise từ {len(raw_vocabulary)} raw)"
    )
    if vocabulary:
        context.log.info(f"   Mẫu: {sorted(vocabulary)[:15]}")

    # ── Bước 2: Regex Counting ────────────────────────────────────────────────
    context.log.info("🔎 Bước 2: Regex scan toàn bộ chunks...")

    pattern = _build_regex(vocabulary)
    rows: list[dict] = []

    if pattern:
        for row in gold_yhct_chunks.iter_rows(named=True):
            # Lowercase để match không phân biệt hoa/thường
            text    = row["chunk_text"].lower()
            matches = pattern.findall(text)
            if not matches:
                continue
            counts: dict[str, int] = {}
            for m in matches:
                counts[m] = counts.get(m, 0) + 1
            for herb_name, cnt in counts.items():
                rows.append({
                    "herb_name":      herb_name,
                    "chunk_id":       row["chunk_id"],
                    "doc_id":         row["doc_id"],
                    "source_file":    row["source_file"],
                    "count_in_chunk": cnt,
                })

    # ── Build output DataFrame ─────────────────────────────────────────────────
    if rows:
        df = pl.DataFrame(rows).with_columns(pl.col("count_in_chunk").cast(pl.Int32))
    else:
        df = pl.DataFrame({
            "herb_name":      pl.Series([], dtype=pl.Utf8),
            "chunk_id":       pl.Series([], dtype=pl.Utf8),
            "doc_id":         pl.Series([], dtype=pl.Utf8),
            "source_file":    pl.Series([], dtype=pl.Utf8),
            "count_in_chunk": pl.Series([], dtype=pl.Int32),
        })

    chunks_hit = df["chunk_id"].n_unique() if df.shape[0] > 0 else 0

    top10 = (
        df.group_by("herb_name")
        .agg(pl.sum("count_in_chunk").alias("total"))
        .sort("total", descending=True)
        .head(10)
    ) if df.shape[0] > 0 else pl.DataFrame({"herb_name": [], "total": []})

    context.log.info(
        f"✅ Kết quả: {df.shape[0]} rows | "
        f"{df['herb_name'].n_unique() if df.shape[0] > 0 else 0} herbs | "
        f"{chunks_hit} chunks | {llm_calls} LLM calls"
    )

    return Output(
        value=df,
        metadata={
            "total_rows":        MetadataValue.int(df.shape[0]),
            "unique_herbs":      MetadataValue.int(df["herb_name"].n_unique() if df.shape[0] > 0 else 0),
            "vocabulary_size":   MetadataValue.int(len(vocabulary)),
            "chunks_with_herbs": MetadataValue.int(chunks_hit),
            "llm_calls":         MetadataValue.int(llm_calls),
            "top10_herbs":       MetadataValue.md(top10.to_pandas().to_markdown()),
        },
    )
