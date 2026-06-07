# streamlit_app/rag.py

import os
import time

import chromadb
from groq import Groq
from sentence_transformers import SentenceTransformer

# ── Config ────────────────────────────────────────────────────────────────────
CHROMA_HOST  = os.getenv("CHROMA_HOST", "chromadb")
CHROMA_PORT  = int(os.getenv("CHROMA_PORT", "8000"))
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
EMBED_MODEL  = "keepitreal/vietnamese-sbert"
GROQ_MODEL   = "llama-3.1-8b-instant"
COLLECTION   = "yhct_chunks"
TOP_K        = 5
MIN_SIM      = 0.25
MAX_HISTORY  = 10   # số messages giữ lại (5 lượt user+assistant)

# System prompt định nghĩa tính cách và nguyên tắc trả lời của bot
_SYSTEM_PROMPT = """Bạn là YHCT Assistant – trợ lý Y học cổ truyền Việt Nam, thân thiện và chuyên nghiệp.

Nguyên tắc:
1. Câu hỏi thông thường (chào hỏi, cảm ơn, hỏi về bạn...): trả lời tự nhiên, ngắn gọn.
2. Câu hỏi về YHCT (bệnh, dược liệu, bài thuốc, triệu chứng...):
   - Có tài liệu tham khảo → dùng tài liệu, nêu cụ thể tên dược liệu, liều lượng, cách dùng.
   - Không có tài liệu → trả lời từ kiến thức YHCT chung, ghi rõ "theo kiến thức chung".
3. Câu hỏi tiếp nối → dùng lịch sử hội thoại để hiểu ngữ cảnh.
4. Không bịa đặt thông tin y tế. Khuyên gặp thầy thuốc khi cần thiết."""


# ── Singleton ─────────────────────────────────────────────────────────────────
_model = None
_col   = None
_groq  = None


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


# ── Main RAG function ─────────────────────────────────────────────────────────
def query_rag(question: str, history: list[dict] | None = None) -> dict:
    """
    Trả lời câu hỏi kết hợp RAG + conversation memory.

    Args:
        question: câu hỏi hiện tại của user.
        history:  danh sách các message trước ({"role": ..., "content": ...}).
                  Chỉ lấy MAX_HISTORY messages gần nhất để tránh overflow token.

    Returns dict: answer, sources, sims, chunks, top_sim, is_zero, elapsed.
    """
    t0      = time.perf_counter()
    history = history or []

    # 1. Embed câu hỏi + tìm chunks liên quan trong ChromaDB
    q_vec   = get_model().encode([question])[0].tolist()
    results = get_collection().query(query_embeddings=[q_vec], n_results=TOP_K)

    chunks  = results["documents"][0]
    ids     = results["ids"][0]
    dists   = results["distances"][0]
    sims    = [round(1 - d, 4) for d in dists]
    top_sim = max(sims) if sims else 0
    is_zero = len(chunks) == 0 or top_sim < MIN_SIM

    # 2. Xây messages cho LLM
    #    - System prompt cố định
    #    - Lịch sử hội thoại (chỉ role + content, bỏ metadata như sources/sims)
    #    - Câu hỏi hiện tại (kèm context RAG nếu có)
    messages: list[dict] = [{"role": "system", "content": _SYSTEM_PROMPT}]

    for msg in history[-MAX_HISTORY:]:
        if msg["role"] in ("user", "assistant"):
            messages.append({"role": msg["role"], "content": msg["content"]})

    if is_zero:
        # Không có context RAG → LLM tự xử lý (casual hoặc general YHCT)
        user_content = question
    else:
        # Có context RAG → đính kèm vào câu hỏi
        context      = "\n\n---\n\n".join(chunks)
        user_content = (
            f"Tài liệu tham khảo từ kho YHCT:\n{context}\n\n"
            f"Câu hỏi: {question}"
        )

    messages.append({"role": "user", "content": user_content})

    # 3. Gọi LLM – luôn gọi, kể cả câu hỏi casual hoặc không có RAG context
    resp   = get_groq().chat.completions.create(
        model=GROQ_MODEL,
        messages=messages,
        temperature=0.3,
        max_tokens=1024,
    )
    answer  = resp.choices[0].message.content
    elapsed = int((time.perf_counter() - t0) * 1000)

    return {
        "answer":  answer,
        "sources": ids   if not is_zero else [],
        "sims":    sims  if not is_zero else [],
        "chunks":  chunks,
        "top_sim": top_sim,
        "is_zero": is_zero,
        "elapsed": elapsed,
    }
