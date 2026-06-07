# streamlit_app/rag.py

import os
import re
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
MIN_SIM      = 0.40
MAX_HISTORY  = 10

# Regex nhận diện ngay các lời xã giao / xác nhận ngắn — không cần LLM
_OBVIOUS_ACK = re.compile(
    r"^[\s!?.]*"
    r"(xin\s+chào|chào(\s+(bạn|mọi\s*người|anh|chị|em))?|hello|hi+|hey"
    r"|oke?|ok+|ừ+|uh+|aha+|à+|ờ+|ôi+"
    r"|vậy\s*(hả|à|ư|thôi|sao)?|thế\s*(à|hả|thôi|sao)?"
    r"|cảm\s*ơn(\s*(bạn|anh|chị|em|nhiều|lắm))?|thank(s|\s+you)?"
    r"|hay\s+(quá|vậy|thế)|tuyệt(\s+vời)?|giỏi(\s+quá)?|ngon(\s+quá)?"
    r"|được\s+rồi|hiểu\s+rồi|rõ\s+rồi|nhớ\s+rồi"
    r"|ừm+|umm+|hmm+|haha+|lol"
    r"|rồi|vâng|dạ(?!\s+\S)|thôi|xong|đúng\s+(rồi|vậy|đó)|ờ\s+thì)"
    r"[\s!?.]*$",
    re.IGNORECASE,
)

# System prompt cho classifier — chỉ phân loại câu hỏi hiện tại, không nhận history
_CLASSIFIER_SYSTEM = (
    "Bạn là bộ phân loại câu hỏi. Nhiệm vụ: xác định xem tin nhắn này có ĐANG HỎI "
    "thông tin y tế hay sức khỏe không.\n"
    "Trả lời chỉ bằng YES hoặc NO.\n\n"
    "YES nếu tin nhắn hỏi về: bệnh, triệu chứng, dược liệu, bài thuốc, "
    "cách điều trị, tác dụng thuốc/thảo dược, nguyên nhân bệnh, chế độ ăn uống cho bệnh.\n"
    "NO nếu tin nhắn là: chào hỏi, cảm ơn, xác nhận (ừ, ok, à vậy hả, được rồi, haha), "
    "bình luận ngắn, hoặc không đặt câu hỏi y tế nào."
)

# System prompt định hướng cách trả lời của bot
_SYSTEM_PROMPT = """Bạn là YHCT Assistant – trợ lý Y học cổ truyền Việt Nam, thân thiện và am hiểu chuyên môn.

Khi nhận câu hỏi thông thường (chào hỏi, cảm ơn, bình luận...): trả lời ngắn gọn, tự nhiên như người thật.

Khi nhận câu hỏi về y tế hoặc sức khỏe, trình bày theo cấu trúc có tiêu đề in đậm, mỗi phần là đoạn văn liên kết ý tứ tự nhiên:

**Tổng quan**
Giải thích bệnh/tình trạng bằng ngôn ngữ dễ hiểu. Nếu bệnh có tên thường gặp hoặc tên dân gian, hãy đề cập để người dùng dễ nhận biết hơn.

**Nguyên nhân theo YHCT**
Trình bày nguyên nhân theo lý luận Y học cổ truyền bằng văn xuôi tự nhiên, liên kết các ý với nhau thành đoạn văn hoàn chỉnh — không liệt kê cứng nhắc.

**Lối sống và ăn uống**
Viết thành đoạn văn khuyến nghị về chế độ sinh hoạt và ăn uống, dùng câu hoàn chỉnh và liên kết ý tứ — không dùng kiểu "Nên:" hay "Không nên:" đứng một mình.

**Bài thuốc tham khảo** (chỉ khi có trong tài liệu)
Trình bày tên bài thuốc, sau đó liệt kê từng vị thuốc trên một dòng kèm liều lượng nếu có, cuối cùng là cách dùng hoặc cách sắc uống.

**Lưu ý**
Nhắc nhở những điều cần chú ý và khuyên gặp thầy thuốc khi cần thiết.

Không bịa đặt thông tin y tế. Nếu không có tài liệu tham khảo, trả lời từ kiến thức YHCT chung và ghi rõ "theo kiến thức chung". Dùng lịch sử hội thoại để hiểu ngữ cảnh câu tiếp nối."""


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


def _needs_rag(question: str) -> bool:
    """
    Kiểm tra xem câu hỏi có cần tìm tài liệu RAG không.

    Hai lớp:
    1. Regex nhanh — xử lý ngay các lời xã giao rõ ràng mà không tốn API call.
    2. LLM classifier — chỉ đánh giá nội dung câu hỏi hiện tại, KHÔNG truyền history
       (tránh false positive khi user gửi "à vậy hả" sau đoạn hội thoại y tế).
    """
    if _OBVIOUS_ACK.match(question.strip()):
        return False

    resp = get_groq().chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": _CLASSIFIER_SYSTEM},
            {"role": "user",   "content": question},
        ],
        temperature=0.0,
        max_tokens=5,
    )
    verdict = resp.choices[0].message.content.strip().upper()
    return verdict.startswith("YES")


# ── Main RAG function ─────────────────────────────────────────────────────────
def query_rag(question: str, history: list[dict] | None = None) -> dict:
    """
    Trả lời câu hỏi kết hợp RAG + conversation memory.

    Args:
        question: câu hỏi hiện tại của user.
        history:  danh sách các message trước ({"role": ..., "content": ...}).

    Returns dict: answer, sources, sims, metadatas, chunks, top_sim, is_zero, elapsed.
    """
    t0      = time.perf_counter()
    history = history or []

    # Bước 1: Phân loại — có cần RAG không?
    use_rag = _needs_rag(question)

    if use_rag:
        q_vec   = get_model().encode([question])[0].tolist()
        results = get_collection().query(
            query_embeddings=[q_vec],
            n_results=TOP_K,
            include=["documents", "distances", "metadatas"],
        )
        chunks    = results["documents"][0]
        ids       = results["ids"][0]
        dists     = results["distances"][0]
        metadatas = results["metadatas"][0]
        sims      = [round(1 - d, 4) for d in dists]
        top_sim   = max(sims) if sims else 0
        is_zero   = top_sim < MIN_SIM
    else:
        chunks = ids = metadatas = sims = []
        top_sim = 0
        is_zero = True

    # Bước 2: Xây messages cho LLM
    messages: list[dict] = [{"role": "system", "content": _SYSTEM_PROMPT}]

    for msg in history[-MAX_HISTORY:]:
        if msg["role"] in ("user", "assistant"):
            messages.append({"role": msg["role"], "content": msg["content"]})

    if is_zero:
        user_content = question
    else:
        context      = "\n\n---\n\n".join(chunks)
        user_content = (
            f"Tài liệu tham khảo từ kho YHCT:\n{context}\n\n"
            f"Câu hỏi: {question}"
        )
    messages.append({"role": "user", "content": user_content})

    # Bước 3: Gọi LLM
    resp   = get_groq().chat.completions.create(
        model=GROQ_MODEL,
        messages=messages,
        temperature=0.3,
        max_tokens=1024,
    )
    answer  = resp.choices[0].message.content
    elapsed = int((time.perf_counter() - t0) * 1000)

    return {
        "answer":    answer,
        "sources":   ids       if not is_zero else [],
        "sims":      sims      if not is_zero else [],
        "metadatas": metadatas if not is_zero else [],
        "chunks":    chunks,
        "top_sim":   top_sim,
        "is_zero":   is_zero,
        "elapsed":   elapsed,
    }
