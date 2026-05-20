# evaluation/metrics.py
"""
Bộ chỉ số đánh giá RAG — tự tính, không cần gọi LLM ngoài.
Tất cả metrics đều deterministic và reproducible.
"""

import re
import math
import time
from collections import Counter
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# 1. RETRIEVAL METRICS
# ─────────────────────────────────────────────────────────────────────────────

def precision_at_k(
    chunks: list[str],
    sims: list[float],
    expected_keywords: list[str],
    k: int = 5,
    threshold: float = 0.25
) -> float:
    """
    Precision@K: tỷ lệ chunks trong top-K thực sự liên quan.
    Liên quan = chunk chứa ít nhất 1 expected_keyword VÀ sim >= threshold.
    """
    top_k = chunks[:k]
    top_sims = sims[:k]
    relevant = 0
    for chunk, sim in zip(top_k, top_sims):
        chunk_low = chunk.lower()
        has_keyword = any(kw.lower() in chunk_low for kw in expected_keywords)
        if has_keyword and sim >= threshold:
            relevant += 1
    return relevant / k if k > 0 else 0.0


def mean_reciprocal_rank(
    chunks: list[str],
    expected_keywords: list[str],
) -> float:
    """
    MRR: chunk liên quan đầu tiên ở vị trí nào?
    MRR = 1/rank_of_first_relevant_chunk
    """
    for i, chunk in enumerate(chunks):
        chunk_low = chunk.lower()
        if any(kw.lower() in chunk_low for kw in expected_keywords):
            return 1.0 / (i + 1)
    return 0.0


def ndcg_at_k(
    chunks: list[str],
    sims: list[float],
    expected_keywords: list[str],
    k: int = 5
) -> float:
    """
    nDCG@K: đánh giá ranking quality — chunk liên quan ở đầu tốt hơn ở cuối.
    """
    def relevance(chunk):
        chunk_low = chunk.lower()
        matched = sum(1 for kw in expected_keywords if kw.lower() in chunk_low)
        return min(matched, 3)  # scale 0-3

    top_k = chunks[:k]
    top_sims = sims[:k]

    dcg = sum(
        relevance(c) / math.log2(i + 2)
        for i, c in enumerate(top_k)
    )

    ideal_rels = sorted([relevance(c) for c in top_k], reverse=True)
    idcg = sum(
        r / math.log2(i + 2)
        for i, r in enumerate(ideal_rels)
    )

    return dcg / idcg if idcg > 0 else 0.0


def chunk_coverage(
    chunk_ids: list[str],
    expected_tang_phu: str,
    metadatas: list[dict]
) -> float:
    """
    Chunk coverage: bao nhiêu nguồn tài liệu khác nhau được truy xuất?
    Normalize theo số chunk tối đa (top_k).
    """
    sources = set()
    for meta in metadatas:
        if meta and "source" in meta:
            sources.add(meta["source"])
    return len(sources) / max(len(chunk_ids), 1)


# ─────────────────────────────────────────────────────────────────────────────
# 2. GENERATION METRICS (không cần LLM judge)
# ─────────────────────────────────────────────────────────────────────────────

def faithfulness_word_overlap(
    answer: str,
    context_chunks: list[str]
) -> float:
    """
    Faithfulness proxy: tỷ lệ từ trong câu trả lời xuất hiện trong context.
    Không dùng LLM — dùng word overlap.
    Score cao → answer bám context (ít hallucination).
    """
    if not answer or not context_chunks:
        return 0.0

    context_text = " ".join(context_chunks).lower()
    context_words = set(re.findall(r'\w+', context_text))

    answer_words = re.findall(r'\w+', answer.lower())
    if not answer_words:
        return 0.0

    # Loại stopwords tiếng Việt phổ biến
    stopwords = {
        "và", "của", "là", "có", "trong", "với", "được", "không",
        "các", "một", "để", "theo", "cho", "từ", "về", "khi",
        "thì", "như", "này", "đó", "bị", "lên", "ra", "vào",
        "rằng", "cũng", "đã", "sẽ", "hay", "hoặc", "nếu", "vì",
        "tôi", "bạn", "hãy", "nên", "cần", "phải"
    }
    meaningful_words = [w for w in answer_words if w not in stopwords and len(w) > 2]

    if not meaningful_words:
        return 0.0

    overlap = sum(1 for w in meaningful_words if w in context_words)
    return overlap / len(meaningful_words)


def answer_relevancy_keyword(
    answer: str,
    question: str,
    expected_keywords: list[str]
) -> float:
    """
    Answer relevancy proxy: câu trả lời có đề cập đúng keywords kỳ vọng không?
    Score = tỷ lệ expected_keywords xuất hiện trong câu trả lời.
    """
    if not answer:
        return 0.0

    answer_low = answer.lower()
    matched = sum(1 for kw in expected_keywords if kw.lower() in answer_low)
    return matched / len(expected_keywords) if expected_keywords else 0.0


def rouge_l(answer: str, reference_chunks: list[str]) -> float:
    """
    ROUGE-L: Longest Common Subsequence giữa answer và context tốt nhất.
    Đo sự trùng lặp ngữ nghĩa có cấu trúc.
    """
    def lcs_length(a: list, b: list) -> int:
        m, n = len(a), len(b)
        if m == 0 or n == 0:
            return 0
        # Space-optimized LCS
        prev = [0] * (n + 1)
        for i in range(1, m + 1):
            curr = [0] * (n + 1)
            for j in range(1, n + 1):
                if a[i-1] == b[j-1]:
                    curr[j] = prev[j-1] + 1
                else:
                    curr[j] = max(curr[j-1], prev[j])
            prev = curr
        return prev[n]

    if not answer or not reference_chunks:
        return 0.0

    answer_tokens = re.findall(r'\w+', answer.lower())
    if not answer_tokens:
        return 0.0

    best_rouge = 0.0
    for chunk in reference_chunks:
        chunk_tokens = re.findall(r'\w+', chunk.lower())
        if not chunk_tokens:
            continue
        lcs = lcs_length(answer_tokens, chunk_tokens)
        precision = lcs / len(answer_tokens)
        recall    = lcs / len(chunk_tokens)
        f1 = (2 * precision * recall / (precision + recall)
              if (precision + recall) > 0 else 0.0)
        best_rouge = max(best_rouge, f1)

    return best_rouge


def answer_completeness(
    answer: str,
    question_category: str
) -> float:
    """
    Completeness heuristic: câu trả lời có đủ thông tin không?
    Đánh giá theo độ dài và cấu trúc.
    """
    if not answer or "xin lỗi" in answer.lower() or "không tìm thấy" in answer.lower():
        return 0.0

    word_count = len(answer.split())

    # Threshold khác nhau theo category
    thresholds = {
        "duoc_lieu":  {"min": 50, "good": 100, "max": 300},
        "bai_thuoc":  {"min": 80, "good": 150, "max": 400},
        "tieu_hoa":   {"min": 60, "good": 120, "max": 350},
    }
    t = thresholds.get(question_category, {"min": 50, "good": 100, "max": 300})

    if word_count < t["min"]:
        return word_count / t["min"] * 0.5
    elif word_count <= t["good"]:
        return 0.5 + (word_count - t["min"]) / (t["good"] - t["min"]) * 0.3
    elif word_count <= t["max"]:
        return 0.8 + (word_count - t["good"]) / (t["max"] - t["good"]) * 0.2
    else:
        return 1.0


# ─────────────────────────────────────────────────────────────────────────────
# 3. SYSTEM METRICS
# ─────────────────────────────────────────────────────────────────────────────

def is_zero_result(top_sim: float, threshold: float = 0.25) -> bool:
    return top_sim < threshold


def compute_confusion(
    answer: str,
    expected_keywords: list[str],
    top_sim: float,
    sim_threshold: float = 0.25,
    keyword_threshold: float = 0.3
) -> str:
    """
    Phân loại kết quả:
    - correct:  tìm được chunk đúng VÀ trả lời đúng keyword
    - partial:  tìm được nhưng trả lời thiếu, HOẶC ngược lại
    - wrong:    không tìm được chunk liên quan
    """
    retrieved_ok = top_sim >= sim_threshold
    answer_ok    = answer_relevancy_keyword(answer, "", expected_keywords) >= keyword_threshold

    if retrieved_ok and answer_ok:
        return "correct"
    elif retrieved_ok or answer_ok:
        return "partial"
    else:
        return "wrong"


# ─────────────────────────────────────────────────────────────────────────────
# 4. AGGREGATE — tính tất cả metrics cho 1 câu hỏi
# ─────────────────────────────────────────────────────────────────────────────

def compute_all_metrics(
    question: str,
    category: str,
    expected_keywords: list[str],
    expected_tang_phu: str,
    chunks: list[str],
    chunk_ids: list[str],
    sims: list[float],
    metadatas: list[dict],
    answer: str,
    elapsed_ms: int,
    top_sim: float,
    top_k: int = 5,
    sim_threshold: float = 0.25
) -> dict:
    """Tính toàn bộ 12 metrics cho 1 câu hỏi."""

    zero = is_zero_result(top_sim, sim_threshold)

    prec   = precision_at_k(chunks, sims, expected_keywords, k=top_k)
    mrr    = mean_reciprocal_rank(chunks, expected_keywords)
    ndcg   = ndcg_at_k(chunks, sims, expected_keywords, k=top_k)
    cov    = chunk_coverage(chunk_ids, expected_tang_phu, metadatas)
    faith  = faithfulness_word_overlap(answer, chunks)
    rel    = answer_relevancy_keyword(answer, question, expected_keywords)
    rl     = rouge_l(answer, chunks)
    comp   = answer_completeness(answer, category)
    label  = compute_confusion(answer, expected_keywords, top_sim, sim_threshold)

    return {
        # Retrieval
        "top_similarity":      round(top_sim, 4),
        "precision_at_k":      round(prec,    4),
        "mrr":                 round(mrr,     4),
        "ndcg_at_k":           round(ndcg,    4),
        "chunk_coverage":      round(cov,     4),
        # Generation
        "faithfulness":        round(faith,   4),
        "answer_relevancy":    round(rel,     4),
        "rouge_l":             round(rl,      4),
        "completeness":        round(comp,    4),
        # System
        "is_zero_result":      int(zero),
        "response_ms":         elapsed_ms,
        "confusion_label":     label,
    }