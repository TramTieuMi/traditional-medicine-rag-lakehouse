# streamlit_app/rag.py

import os
import time
import chromadb
from sentence_transformers import SentenceTransformer
from groq import Groq

# ── Config từ environment ────────────────────────────────────────────────────
CHROMA_HOST  = os.getenv("CHROMA_HOST", "chromadb")
CHROMA_PORT  = int(os.getenv("CHROMA_PORT", "8000"))
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
EMBED_MODEL  = "keepitreal/vietnamese-sbert"
GROQ_MODEL   = "llama-3.1-8b-instant"
COLLECTION   = "yhct_chunks"
TOP_K        = 5
MIN_SIM      = 0.25


# ── Singleton: load 1 lần duy nhất ──────────────────────────────────────────
_model  = None
_col    = None
_groq   = None


def get_model():
    global _model
    if _model is None:
        _model = SentenceTransformer(EMBED_MODEL)
    return _model


def get_collection():
    global _col
    if _col is None:
        client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
        _col   = client.get_or_create_collection(
            name=COLLECTION,
            metadata={"hnsw:space": "cosine"}
        )
    return _col


def get_groq():
    global _groq
    if _groq is None:
        _groq = Groq(api_key=GROQ_API_KEY)
    return _groq


# ── Main RAG function ────────────────────────────────────────────────────────
def query_rag(question: str) -> dict:
    t0 = time.perf_counter()

    # 1. Embed câu hỏi
    model  = get_model()
    q_vec  = model.encode([question])[0].tolist()

    # 2. Tìm top-K chunks
    col     = get_collection()
    results = col.query(query_embeddings=[q_vec], n_results=TOP_K)

    chunks  = results["documents"][0]
    ids     = results["ids"][0]
    dists   = results["distances"][0]
    sims    = [round(1 - d, 4) for d in dists]
    top_sim = max(sims) if sims else 0

    # 3. Kiểm tra ngưỡng
    is_zero = (len(chunks) == 0 or top_sim < MIN_SIM)

    # 4. Gọi Groq LLM
    if is_zero:
        answer = (
            "Xin lỗi, tôi không tìm thấy thông tin liên quan "
            "trong tài liệu Y học cổ truyền hiện có. "
            "Vui lòng thử câu hỏi khác."
        )
    else:
        context = "\n\n---\n\n".join(chunks)
        prompt  = f"""Bạn là chuyên gia Y học cổ truyền Việt Nam.
Dựa vào các đoạn tài liệu sau đây:

{context}

Hãy trả lời câu hỏi: {question}

Yêu cầu:
- Trả lời chi tiết, rõ ràng bằng tiếng Việt
- Trích dẫn tên bài thuốc, dược liệu cụ thể nếu có
- Nêu liều lượng nếu tài liệu đề cập
- Nếu thông tin không đủ, hãy nói rõ"""

        resp   = get_groq().chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=1024,
        )
        answer = resp.choices[0].message.content

    elapsed = int((time.perf_counter() - t0) * 1000)

    return {
        "answer":   answer,
        "sources":  ids,
        "sims":     sims,
        "chunks":   chunks,
        "top_sim":  top_sim,
        "is_zero":  is_zero,
        "elapsed":  elapsed,
    }