import json
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
GROQ_MODEL   = "llama-3.3-70b-versatile"
COLLECTION   = "yhct_chunks"
TOP_K        = 5
MIN_SIM      = 0.30
MAX_HISTORY  = 6

# ── FastAPI App Setup ─────────────────────────────────────────────────────────
app = FastAPI(title="YHCT AI Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_ENTITY_EXTRACT_PROMPT = """Bạn là chuyên gia y học cổ truyền Việt Nam. Phân tích câu hỏi của người dùng và trích xuất các thực thể y tế họ đề cập.

Trả về JSON thuần túy (không markdown, không giải thích) với 4 trường:
- "symptoms": danh sách triệu chứng người dùng MÔ TẢ mình đang bị (ví dụ: "đau đầu", "mất ngủ", "táo bón")
- "diseases": tên bệnh hoặc hội chứng được nhắc đến (ví dụ: "viêm dạ dày", "tiểu đường")
- "body_parts": bộ phận cơ thể liên quan (ví dụ: "dạ dày", "gan", "lưng")
- "herbs": dược liệu hoặc thuốc được hỏi (ví dụ: "gừng", "cam thảo")

Chỉ ghi các thực thể THỰC SỰ có trong câu hỏi. Nếu không có thì để mảng rỗng [].
Chuẩn hóa về dạng viết thường, bỏ dấu câu thừa.

Câu hỏi: {question}

JSON:"""


def extract_entities(question: str, groq_client: Any) -> Dict[str, List[str]]:
    """Dùng Groq LLM để trích xuất thực thể y tế từ câu hỏi người dùng."""
    _empty = {"symptoms": [], "diseases": [], "body_parts": [], "herbs": []}
    if not question or not question.strip():
        return _empty
    try:
        resp = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{
                "role": "user",
                "content": _ENTITY_EXTRACT_PROMPT.format(question=question.strip()),
            }],
            temperature=0.0,
            max_tokens=256,
        )
        raw = resp.choices[0].message.content.strip()
        # Bóc JSON ra dù model có thể thêm markdown
        if "```" in raw:
            raw = raw.split("```")[1].lstrip("json").strip()
        parsed = json.loads(raw)
        return {
            "symptoms":   [str(x).strip() for x in parsed.get("symptoms",   []) if x],
            "diseases":   [str(x).strip() for x in parsed.get("diseases",   []) if x],
            "body_parts": [str(x).strip() for x in parsed.get("body_parts", []) if x],
            "herbs":      [str(x).strip() for x in parsed.get("herbs",      []) if x],
        }
    except Exception:
        return _empty

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
    r"|rồi|vâng|dạ(?!\s+\S)|thôi|xong|đúng\s+(rồi|vậy|đó)|ờ\s+thì"
    r"|tào\s*lao|vớ\s*vẩn|nhảm\s*nhí|khùng|điên|ngu|dở|hâm|nhảm|bậy\s*bạ|tầm\s*bậy|tào\s*lao\s*mía\s*lao|sai\s*(bét|quá))"
    r"[\s!?.]*$",
    re.IGNORECASE,
)

_CLASSIFIER_SYSTEM = (
    "Bạn là bộ phân loại tin nhắn. Nhiệm vụ: xác định xem tin nhắn này có ĐANG HỎI thực sự về thông tin y tế, sức khỏe hoặc triệu chứng/bệnh tật không.\n"
    "Trả lời chỉ bằng YES hoặc NO.\n\n"
    "YES nếu tin nhắn hỏi về: bệnh, triệu chứng, dược liệu, bài thuốc, cách điều trị, tác dụng thuốc, chế độ ăn cho bệnh.\n"
    "NO nếu tin nhắn là: chào hỏi, cảm ơn, xác nhận, các câu nhận xét/từ lóng/phàn nàn/cảm thán (ví dụ: 'tào lao', 'vớ vẩn', 'khùng quá', 'nhảm nhí', 'sai bét', 'tào lao mía lao'), hoặc bất kỳ tin nhắn phi y tế nào."
)

_SYSTEM_PROMPT = """Bạn là YHCT Assistant – trợ lý Y học cổ truyền Việt Nam, thân thiện và am hiểu chuyên môn.

Khi nhận câu hỏi thông thường hoặc bình luận phi y tế (chào hỏi, cảm ơn, bình luận, phàn nàn, nhận xét hoặc từ lóng như 'tào lao', 'vớ vẩn', 'khùng'...): trả lời ngắn gọn, lịch sự, nhận lỗi/giải thích nếu có lỗi, tự nhiên như người thật. Tuyệt đối không trình bày theo dạng các mục y tế (Tổng quan, Nguyên nhân...) cho những tin nhắn này.

Khi nhận câu hỏi thực sự về y tế hoặc sức khỏe, trình bày theo cấu trúc có tiêu đề in đậm, mỗi phần là đoạn văn liên kết ý tứ tự nhiên:

**Tổng quan**
Giải thích bệnh/tình trạng bằng ngôn ngữ dễ hiểu. Nếu bệnh có tên thường gặp hoặc tên dân gian, hãy đề cập để người dùng dễ nhận biết hơn.

**Nguyên nhân theo YHCT**
Giải thích nguyên nhân gây bệnh theo lý luận Y học cổ truyền (như phong hàn, phong nhiệt, khí trệ, huyết ứ, âm hư, dương hư, thận hư...) một cách khoa học, chuẩn xác cho từng loại bệnh lý cụ thể. TUYỆT ĐỐI không dùng một khuôn mẫu (template) lặp đi lặp lại cho mọi bệnh (ví dụ: cấm chẩn đoán tất cả các chứng bệnh khác nhau như đau lưng, mắt mờ, say rượu... đều do 'sự ứ đọng của đàm' hay 'mất cân bằng ngũ hành').

**Lối sống và ăn uống**
Đưa ra khuyên về chế độ sinh hoạt và ăn uống riêng biệt, đặc thù cho từng bệnh lý của người dùng. TUYỆT ĐỐI không dùng một khuôn mẫu chung lặp đi lặp lại cho mọi bệnh (ví dụ: cấm khuyên đi bộ, yoga, thiền và ăn rau xanh trái cây cho người đang bị cảm sốt hay say rượu). Người bị cảm sốt phải khuyên nghỉ ngơi giữ ấm, xông lá, uống nước ấm; người đau dạ dày phải khuyên ăn cháo ấm, đồ mềm, không ăn đồ chua cay, không nhịn ăn; người say rượu phải khuyên uống nước ấm, nước gừng chanh, nghỉ ngơi, cấm vận động mạnh; người đau lưng phải khuyên tránh mang vác nặng, chườm ấm, nghỉ ngơi hợp lý. Viết thành đoạn văn khuyến nghị, dùng câu hoàn chỉnh và liên kết ý tế nhị — không dùng kiểu "Nên:" hay "Không nên:" đứng một mình.

**Bài thuốc tham khảo**
Áp dụng quy trình gợi ý bài thuốc nghiêm ngặt sau:
- Bước 1 (Ưu tiên hàng đầu): Trích xuất bài thuốc cụ thể trị đúng tình trạng này từ "Tài liệu tham khảo từ kho YHCT" được cung cấp. Bài thuốc được gợi ý phải trị đúng bệnh hoặc tổ hợp các triệu chứng chính của người dùng. Nếu có bài thuốc phù hợp, bạn BẮT BUỘC phải liệt kê đầy đủ các vị thuốc thành phần cụ thể kèm liều lượng định lượng rõ ràng (gam) và cách sắc/uống rõ ràng của bài thuốc đó. Nếu tài liệu tham khảo chỉ ghi tên bài thuốc chung chung (ví dụ: chỉ liệt kê 'Hương nhu tán, Lục nhất tán...') mà không có thành phần vị thuốc và liều lượng cụ thể trong văn bản đó, bạn TUYỆT ĐỐI không được tự ý bịa đặt ra liều lượng cho chúng, cũng không được coi các tên bài thuốc đó là vị thuốc; thay vào đó hãy bỏ qua bài thuốc thiếu chi tiết này và chuyển sang Bước 2.
- Bước 2 (Kiến thức YHCT chung): Nếu tài liệu tham khảo được cung cấp không có bài thuốc cụ thể, hoặc thiếu công thức/định lượng chi tiết, hoặc các bài thuốc trong tài liệu đã bị loại trừ/lặp lại: Hãy gợi ý một bài thuốc hoặc mẹo dân gian y học cổ truyền an toàn, chính thống và phổ biến từ kiến thức YHCT chung điều trị đúng căn bệnh chính (ví dụ: dùng gừng ấm, cháo hành tía tô cho người cảm lạnh mắc mưa; trà hoa cúc cho người mất ngủ...). TUYỆT ĐỐI KHÔNG ĐƯỢC tự bịa ra các tên gọi Hán-Việt nghe có vẻ cổ trang nhưng không có thực trong thực tế (ví dụ: cấm bịa ra các tên như 'Tỳ Thống Tán', 'Phúc Nguyên', 'Thủy Điệp'). Hãy dùng các tên gọi dân gian quen thuộc, chính thống và an toàn (như Trà gừng, Cháo hành tía tô, Trà hoa cúc, Nước chanh ấm...). Bạn BẮT BUỘC phải cung cấp công thức định lượng cụ thể từ kiến thức chung của bạn (ví dụ: Gừng tươi 10g, lá Tía tô 12g, sắc với 500ml nước còn 200ml uống ấm) và cách dùng rõ ràng, ghi rõ nguồn gốc là "Theo kiến thức Y học cổ truyền chung".
- Bước 3 (Từ chối nếu không biết): Nếu triệu chứng phức tạp hoặc cả tài liệu và kiến thức chung đều không có cách điều trị an toàn và chi tiết (không có thành phần và liều lượng cụ thể), ghi rõ: "Hiện tại nguồn tài liệu tham khảo và kiến thức chung chưa có bài thuốc cụ thể cho tình trạng này." và tuyệt đối không tự chế bừa ra các vị thuốc khác.
Yêu cầu bắt buộc: Tuyệt đối không được chỉ liệt kê tên bài thuốc chung chung mà không ghi rõ các vị thuốc thành phần cụ thể, liều lượng và cách uống của từng bài thuốc đó. Khách hàng cần thông tin chi tiết và an toàn để có thể thực hiện được. Nghiêm cấm hướng dẫn bôi ngoài da cho các thuốc uống (ví dụ: các bài thuốc uống dạng sắc thì phải uống ấm, cấm khuyên dùng để bôi vào cơ thể). Tuyệt đối không được viết thêm các gợi ý phụ chung chung không có liều lượng cách dùng (ví dụ: cấm ghi thêm 'ngoài ra có thể uống nước chanh ấm, trà hoa cúc...' nếu không viết rõ công thức pha, định lượng gam và cách uống của trà hoa cúc hay nước chanh đó). Đã khuyên dùng bài thuốc/mẹo nào là bắt buộc phải có đầy đủ thành phần, liều lượng và cách dùng của cái đó, còn không thì tuyệt đối cấm ghi vào câu trả lời.

**Lưu ý**
Nhắc nhở những điều cần chú ý và khuyên gặp thầy thuốc khi cần thiết.

Ràng buộc quan trọng: TUYỆT ĐỐI không được bịa đặt ra bài thuốc phi lý, tự chế ra tên dược liệu hay cách chữa trị không có trong thực tế. Nghiêm cấm việc tự chế ra tên vị thuốc hoặc bài thuốc mới bằng cách ghép các chữ cái hoặc từ ngữ trong tài liệu (ví dụ: thấy từ 'Phúc tướng' và 'Nguyên nhân' ghép thành vị thuốc 'Phúc Nguyên' là hoàn toàn bịa đặt). Chỉ được ghi các bài thuốc và vị thuốc có tên gọi rõ ràng, chính thống. 
Nghiêm cấm việc tự ý dịch nghĩa các chữ cái/từ trong tên viết tắt của bài thuốc để tự đoán thành phần (ví dụ: bài thuốc viết tắt 'Khương Bàng Bạc Bồ Thang' thực tế là Khương hoạt, Ngưu bàng tử, Bạc hà, Bồ công anh. Tuyệt đối không được đoán chữ rồi tự chế thành: Khương = Gừng, Bàng = Hẹ, Bạc = Bạc hà, Bồ = Bồ kết. Bồ kết liều cao sắc uống cực kỳ độc và nguy hại tính mạng). Nếu không tìm thấy công thức thành phần chính xác của một bài thuốc viết tắt/lạ trong tài liệu hoặc kiến thức chung, bắt buộc phải trả lời: "Hiện tại hệ thống không tìm thấy công thức thành phần cụ thể cho bài thuốc này" và khuyên không tự ý sử dụng, cấm tự đoán bừa.
Nếu tài liệu nhắc đến thuốc Tây y (như Motilium, Vitamin B1, Calciglycerophosphat...), phải ghi rõ đây là thuốc Tây y phối hợp, cấm biến chúng thành vị thuốc Đông y. Say rượu bia (hangover) là câu hỏi thông thường về sức khỏe đời sống, bạn được phép tư vấn các bài thuốc giải rượu, mẹo làm hết say rượu dân gian an toàn (như nước chanh, trà gừng, sinh tố B6, bột sắn dây...) để giúp ích cho người dùng, tuyệt đối không được từ chối trả lời hoặc coi đây là hành vi nguy hiểm tự hại. Chẩn đoán và điều trị phải nhất quán về mặt y lý: nếu chẩn đoán bệnh là Ngoại cảm phong hàn (cảm lạnh, dầm mưa), tuyệt đối không được khuyên dùng bài thuốc Thanh nhiệt giải thử (chuyên trị cảm nắng mùa hè). Nếu tài liệu tham khảo không liên quan hoặc lệch hướng điều trị với câu hỏi của người dùng, hãy bỏ qua tài liệu đó và trả lời dựa trên kiến thức YHCT chung hoặc báo không biết. Cái nào không có tài liệu và không có kiến thức YHCT chung chính thống thì trả lời thẳng thắn là không biết/không có thông tin để bảo vệ sức khỏe người dùng. Dùng lịch sử hội thoại để hiểu ngữ cảnh câu tiếp nối."""

SYSTEM_PROMPT = _SYSTEM_PROMPT

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
    q = question.strip().lower()
    
    # 1. Khớp regex xã giao sẵn có (chào hỏi, cảm ơn, từ lóng ngắn)
    if _OBVIOUS_ACK.match(q):
        return False

    # 2. Khớp các từ khóa phàn nàn hệ thống, tốc độ, lỗi hoặc từ lóng/chửi bới
    system_chat_keywords = [
        "chậm", "tốc độ", "lag", "giật", "phản hồi", "lỗi", "error", 
        "tào lao", "vớ vẩn", "khùng", "ngu", "nhảm", "bịa", "giỡn mặt", "lặp lại"
    ]
    if any(kw in q for kw in system_chat_keywords):
        return False

    # 3. Mặc định là chạy RAG cho tất cả câu hỏi y khoa còn lại.
    # Không cần gọi Groq LLM phân loại (tiết kiệm 1 cuộc gọi API, tăng tốc phản hồi từ 2-3 giây).
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

def _reformulate_query(question: str, history: List[ChatMessage]) -> str:
    """Sử dụng LLM để viết lại câu hỏi hội thoại và lịch sử thành câu hỏi tìm kiếm độc lập."""
    if not history:
        return question

    # Nếu câu hỏi mới giống hệt hoặc lặp lại câu hỏi trước đó của người dùng (bỏ qua khoảng trắng và viết hoa), không cần viết lại
    last_user_msg = next((msg.content for msg in reversed(history) if msg.role == "user"), None)
    if last_user_msg:
        def normalize_str(s: str) -> str:
            return re.sub(r'[!?.,\s]+', '', s.strip().lower())
        if normalize_str(last_user_msg) == normalize_str(question):
            return question

    # Lấy tối đa 4 tin nhắn gần nhất làm ngữ cảnh viết lại
    hist_str = ""
    for msg in history[-4:]:
        role_name = "User" if msg.role == "user" else "Assistant"
        hist_str += f"{role_name}: {msg.content}\n"

    prompt = (
        "Bạn là trợ lý ảo chuyên viết lại câu hỏi hội thoại thành câu hỏi tìm kiếm độc lập.\n"
        "Nhiệm vụ: Dựa vào câu hỏi mới nhất của người dùng, hãy viết lại thành một câu hỏi tìm kiếm "
        "đầy đủ, rõ ràng và độc lập. Chỉ dùng lịch sử hội thoại để hiểu ngữ cảnh khi câu hỏi mới có từ "
        "thay thế như 'nó', 'đó', 'vậy' — nếu câu hỏi mới đã rõ ràng thì KHÔNG được thêm chủ đề từ lịch sử.\n"
        "QUAN TRỌNG: Câu hỏi viết lại phải bám sát ĐÚNG CHỦ ĐỀ mà người dùng đang hỏi trong câu hỏi mới nhất, "
        "không được thay thế hay trộn lẫn với các chủ đề khác trong lịch sử. Nếu câu hỏi mới lặp lại câu hỏi trước đó, "
        "hãy giữ nguyên nội dung cốt lõi của câu hỏi mới, không được tự ý bôi ra các từ khóa y khoa khác.\n"
        "Yêu cầu: CHỈ trả về câu hỏi đã được viết lại, không giải thích gì thêm.\n\n"
        f"Lịch sử hội thoại (chỉ dùng khi cần):\n{hist_str}\n"
        f"Câu hỏi mới nhất của người dùng: {question}\n\n"
        "Câu hỏi viết lại:"
    )

    try:
        resp = get_groq().chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": "You are a query rewriter."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0,
            max_tokens=100,
        )
        rewritten = resp.choices[0].message.content.strip()
        rewritten = rewritten.strip('"\'')
        return rewritten
    except Exception:
        # Fallback về câu hỏi gốc nếu gọi LLM lỗi
        return question

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
            # Viết lại câu hỏi thành câu tìm kiếm độc lập dựa trên ngữ cảnh lịch sử
            search_query = _reformulate_query(question, history)
            q_vec = get_model().encode([search_query])[0].tolist()
            results = get_collection().query(
                query_embeddings=[q_vec],
                n_results=TOP_K,
                include=["documents", "distances", "metadatas"],
            )
            raw_chunks = results["documents"][0]
            raw_ids = results["ids"][0]
            dists = results["distances"][0]
            raw_metadatas = results["metadatas"][0]
            raw_sims = [round(1 - d, 4) for d in dists]

            # Lọc chỉ giữ lại tài liệu có độ tương đồng cá nhân >= MIN_SIM
            filtered_indices = [i for i, sim in enumerate(raw_sims) if sim >= MIN_SIM]

            if filtered_indices:
                chunks = [raw_chunks[i] for i in filtered_indices]
                ids = [raw_ids[i] for i in filtered_indices]
                metadatas = [raw_metadatas[i] for i in filtered_indices]
                sims = [raw_sims[i] for i in filtered_indices]
                top_sim = max(sims)
                is_zero = False
            else:
                chunks = []
                ids = []
                metadatas = []
                sims = []
                top_sim = 0
                is_zero = True
        except Exception as e:
            # Ghi log và bỏ qua RAG nếu lỗi ChromaDB
            is_zero = True

    # 3. Tạo messages cho LLM
    system_prompt = SYSTEM_PROMPT
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

    # 4. Gọi LLM sinh câu trả lời (retry tối đa 3 lần nếu rate limit)
    answer = None
    last_err = None
    for attempt in range(3):
        try:
            resp = get_groq().chat.completions.create(
                model=GROQ_MODEL,
                messages=messages,
                temperature=0.3,
                max_tokens=2048,
            )
            answer = resp.choices[0].message.content
            break
        except Exception as e:
            last_err = e
            if attempt < 2:
                time.sleep(3 ** attempt)  # 1s, 3s
    if answer is None:
        raise HTTPException(status_code=500, detail=f"Groq API Error: {str(last_err)}")

    elapsed = int((time.perf_counter() - t0) * 1000)

    # 5. Trích xuất thực thể y tế từ câu hỏi người dùng bằng LLM
    extracted = extract_entities(question, get_groq())

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

# ── Dynamic Config Endpoints ──────────────────────────────────────────────────
class ConfigUpdate(BaseModel):
    min_sim: Optional[float] = None
    top_k: Optional[int] = None
    system_prompt: Optional[str] = None
    groq_model: Optional[str] = None

@app.get("/api/config")
def get_config():
    global MIN_SIM, TOP_K, SYSTEM_PROMPT, GROQ_MODEL
    return {
        "min_sim": MIN_SIM,
        "top_k": TOP_K,
        "system_prompt": SYSTEM_PROMPT,
        "groq_model": GROQ_MODEL
    }

@app.post("/api/config")
def update_config(payload: ConfigUpdate):
    global MIN_SIM, TOP_K, SYSTEM_PROMPT, GROQ_MODEL
    if payload.min_sim is not None:
        MIN_SIM = payload.min_sim
    if payload.top_k is not None:
        TOP_K = payload.top_k
    if payload.system_prompt is not None:
        SYSTEM_PROMPT = payload.system_prompt
    if payload.groq_model is not None:
        GROQ_MODEL = payload.groq_model
    return {"status": "success", "config": get_config()}

