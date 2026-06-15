import os
import re
import time
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import chromadb
from sentence_transformers import SentenceTransformer
from groq import Groq

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

# ── FastAPI App Setup ─────────────────────────────────────────────────────────
app = FastAPI(title="YHCT AI Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Medical Entities Definitions for Parsing ──────────────────────────────────
SYMPTOMS_LIST = [
    "đau dạ dày", "táo bón", "tiêu chảy", "đầy bụng", "chướng bụng", "ợ chua", "ợ hơi",
    "buồn nôn", "nôn mửa", "nôn", "đau bụng", "vàng da", "ăn không tiêu", "ăn kém", 
    "tiêu hóa kém", "mất ngủ", "hồi hộp", "hay quên", "ho khan", "ho có đờm", "khó thở",
    "đái dầm", "di tinh", "di niệu"
]

DISEASES_LIST = [
    "viêm gan", "xơ gan", "viêm đại tràng", "viêm dạ dày", "tỳ vị hư", "can khí uất kết",
    "can âm hư", "can huyết hư", "thận khí hư", "thận âm hư", "thận dương hư", "phế khí hư", 
    "phế âm hư", "tâm huyết hư", "tâm khí hư", "tỳ khí hư", "tỳ dương hư"
]

BODY_PARTS_LIST = [
    "dạ dày", "ruột", "gan", "mật", "thượng vị", "tỳ", "can", "thận", "phế", "tâm",
    "đại tràng", "tiểu tràng", "bàng quang", "mệnh môn"
]

HERBS_LIST = [
    "cam thảo", "bạch truật", "phục linh", "nhân sâm", "hoàng kỳ", "sinh địa", "thục địa",
    "sài hồ", "bán hạ", "trần bì", "nhân trần", "sơn tra", "đương quy", "gừng", "nghệ",
    "ngải cứu", "chi tử", "đại hoàng", "bạch thược", "thương truật", "hậu phác", "ý dĩ",
    "hoàng liên", "sinh khương", "mộc hương"
]

def extract_entities(text: str) -> Dict[str, List[str]]:
    """Trích xuất thực thể y tế cơ bản từ văn bản bằng keyword matching."""
    text_lower = text.lower()
    
    extracted_symptoms = [s for s in SYMPTOMS_LIST if s in text_lower]
    extracted_diseases = [d for d in DISEASES_LIST if d in text_lower]
    extracted_body_parts = [b for b in BODY_PARTS_LIST if b in text_lower]
    extracted_herbs = [h for h in HERBS_LIST if h in text_lower]
    
    return {
        "symptoms": extracted_symptoms,
        "diseases": extracted_diseases,
        "body_parts": extracted_body_parts,
        "herbs": extracted_herbs
    }

# ── Obvious Social Chat Classifier ────────────────────────────────────────────
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

_CLASSIFIER_SYSTEM = (
    "Bạn là bộ phân loại câu hỏi. Nhiệm vụ: xác định xem tin nhắn này có ĐANG HỎI "
    "thông tin y tế hay sức khỏe không.\n"
    "Trả lời chỉ bằng YES hoặc NO.\n\n"
    "YES nếu tin nhắn hỏi về: bệnh, triệu chứng, dược liệu, bài thuốc, "
    "cách điều trị, tác dụng thuốc/thảo dược, nguyên nhân bệnh, chế độ ăn uống cho bệnh.\n"
    "NO nếu tin nhắn là: chào hỏi, cảm ơn, xác nhận (ừ, ok, à vậy hả, được rồi, haha), "
    "bình luận ngắn, hoặc không đặt câu hỏi y tế nào."
)

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

# ── Singletons & Initialization ───────────────────────────────────────────────
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
        if not GROQ_API_KEY:
            raise ValueError("Missing GROQ_API_KEY environment variable.")
        _groq = Groq(api_key=GROQ_API_KEY)
    return _groq

def _needs_rag(question: str) -> bool:
    if _OBVIOUS_ACK.match(question.strip()):
        return False

    try:
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
    except Exception:
        # Fallback to RAG in case of API failure
        return True

# ── API Request & Response Schemas ────────────────────────────────────────────
class ChatMessage(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    query: str
    history: Optional[List[ChatMessage]] = []
    user_name: Optional[str] = None
    user_age: Optional[int] = None
    user_gender: Optional[str] = None

class ExtractedEntities(BaseModel):
    symptoms: List[str]
    diseases: List[str]
    body_parts: List[str]
    herbs: List[str]

class ChatResponse(BaseModel):
    answer: str
    sources: List[str]
    sims: List[float]
    metadatas: List[Dict[str, Any]]
    elapsed: int
    is_zero: bool
    extracted_entities: ExtractedEntities

# ── Health Endpoint ───────────────────────────────────────────────────────────
@app.get("/health")
def health_check():
    try:
        col = get_collection()
        count = col.count()
        return {"status": "healthy", "chromadb_connected": True, "vector_count": count}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

# ── Main Chat API Endpoint ────────────────────────────────────────────────────
@app.post("/api/chat", response_model=ChatResponse)
def api_chat(payload: ChatRequest):
    t0 = time.perf_counter()
    question = payload.query
    history = payload.history or []

    # 1. Phân loại câu hỏi
    use_rag = _needs_rag(question)

    chunks = []
    ids = []
    metadatas = []
    sims = []
    top_sim = 0
    is_zero = True

    # 2. Truy vấn RAG nếu cần thiết
    if use_rag:
        try:
            q_vec = get_model().encode([question])[0].tolist()
            results = get_collection().query(
                query_embeddings=[q_vec],
                n_results=TOP_K,
                include=["documents", "distances", "metadatas"],
            )
            chunks = results["documents"][0]
            ids = results["ids"][0]
            dists = results["distances"][0]
            metadatas = results["metadatas"][0]
            sims = [round(1 - d, 4) for d in dists]
            top_sim = max(sims) if sims else 0
            is_zero = top_sim < MIN_SIM
        except Exception as e:
            # Ghi log và bỏ qua RAG nếu lỗi ChromaDB
            is_zero = True

    # 3. Tạo messages cho LLM
    system_prompt = _SYSTEM_PROMPT
    if payload.user_name:
        system_prompt += f"\n\nThông tin người dùng hiện tại:\n- Họ và tên: {payload.user_name}\n- Tuổi: {payload.user_age or 'Không rõ'}\n- Giới tính: {payload.user_gender or 'Không rõ'}"
    messages = [{"role": "system", "content": system_prompt}]

    for msg in history[-MAX_HISTORY:]:
        if msg.role in ("user", "assistant"):
            messages.append({"role": msg.role, "content": msg.content})

    if is_zero:
        user_content = question
    else:
        context = "\n\n---\n\n".join(chunks)
        user_content = (
            f"Tài liệu tham khảo từ kho YHCT:\n{context}\n\n"
            f"Câu hỏi: {question}"
        )
    messages.append({"role": "user", "content": user_content})

    # 4. Gọi LLM sinh câu trả lời
    try:
        resp = get_groq().chat.completions.create(
            model=GROQ_MODEL,
            messages=messages,
            temperature=0.3,
            max_tokens=1024,
        )
        answer = resp.choices[0].message.content
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Groq API Error: {str(e)}")

    elapsed = int((time.perf_counter() - t0) * 1000)

    # 5. Trích xuất thực thể y tế (từ cả câu hỏi và câu trả lời)
    extracted = extract_entities(question + " " + answer)

    return ChatResponse(
        answer=answer,
        sources=ids if not is_zero else [],
        sims=sims if not is_zero else [],
        metadatas=metadatas if not is_zero else [],
        elapsed=elapsed,
        is_zero=is_zero,
        extracted_entities=ExtractedEntities(
            symptoms=extracted["symptoms"],
            diseases=extracted["diseases"],
            body_parts=extracted["body_parts"],
            herbs=extracted["herbs"]
        )
    )
