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
GROQ_MODEL   = "llama-3.1-8b-instant"
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
    r"(xin\s+chào|chào(\s+(bạn|mọi\s*người|anh|chị|em))?|hello|hi+|hey|ơi"
    r"|oke?|ok+|okay|okey|o\s*kê|ừ+|uh+|aha+|à+|ờ+|ôi+|ò+"
    r"|vậy\s*(hả|à|ư|thôi|sao)?|thế\s*(à|hả|thôi|sao)?"
    r"|cảm\s*ơn(\s*(bạn|anh|chị|em|nhiều|lắm))?|thank(s|\s+you)?"
    r"|hay\s+(quá|vậy|thế)|tuyệt(\s+vời)?|giỏi(\s+quá)?|ngon(\s+quá)?"
    r"|được\s*(rồi)?|hiểu\s*(rồi)?|rõ\s*(rồi)?|nhớ\s*(rồi)?"
    r"|ừm+|umm+|hmm+|haha+|lol|hehe+"
    r"|rồi|vâng(\s*ạ)?|dạ(?!\s+\S)|thôi|xong|đúng\s+(rồi|vậy|đó)|ờ\s+thì"
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
Gợi ý bài thuốc theo thứ tự ưu tiên sau (tuyệt đối không hiển thị các tiêu đề "Bước 1", "Bước 2", "Bước 3" ra câu trả lời):
- Ưu tiên 1 (Trích xuất từ tài liệu): Trích xuất bài thuốc cụ thể trị đúng tình trạng này từ "Tài liệu tham khảo từ kho YHCT" được cung cấp. Phải liệt kê đầy đủ vị thuốc, liều lượng (gam) và cách sắc/uống. Nếu tài liệu chỉ ghi tên bài thuốc chung chung không có thành phần liều lượng cụ thể, tuyệt đối không tự bịa đặt, hãy bỏ qua và chuyển sang Ưu tiên 2.
- Ưu tiên 2 (Từ kiến thức YHCT chung): Gợi ý bài thuốc/mẹo dân gian an toàn, quen thuộc và chính thống (ví dụ: Trà gừng, Cháo hành tía tô...) trị đúng triệu chứng chính của người dùng. Phải ghi rõ công thức định lượng cụ thể (ví dụ: Gừng tươi 10g, lá Tía tô 12g, sắc uống ấm) kèm cách dùng, ghi rõ nguồn là "Theo kiến thức Y học cổ truyền chung". Tuyệt đối không tự bịa ra các tên Hán-Việt phi thực tế.
- Ưu tiên 3 (Từ chối nếu không biết): Nếu triệu chứng phức tạp hoặc không có bài thuốc an toàn và chi tiết, ghi rõ: "Hiện tại nguồn tài liệu tham khảo và kiến thức chung chưa có bài thuốc cụ thể cho tình trạng này."
Yêu cầu bắt buộc: Tuyệt đối không được chỉ liệt kê tên bài thuốc chung chung mà không ghi rõ các vị thuốc thành phần cụ thể, liều lượng và cách sắc/uống của từng bài thuốc đó. Nghiêm cấm hướng dẫn bôi ngoài da cho các thuốc uống. Đã khuyên dùng bài thuốc nào là bắt buộc phải có đầy đủ thành phần, liều lượng và cách dùng của bài thuốc đó.

**Lưu ý**
Nhắc nhở những điều cần chú ý và khuyên gặp thầy thuốc khi cần thiết.

Ràng buộc quan trọng: TUYỆT ĐỐI không được bịa đặt ra bài thuốc phi lý, tự chế ra tên dược liệu hay cách chữa trị không có trong thực tế. Nghiêm cấm việc tự chế ra tên vị thuốc hoặc bài thuốc mới bằng cách ghép các chữ cái hoặc từ ngữ trong tài liệu (ví dụ: thấy từ 'Phúc tướng' và 'Nguyên nhân' ghép thành vị thuốc 'Phúc Nguyên' là hoàn toàn bịa đặt). Chỉ được ghi các bài thuốc và vị thuốc có tên gọi rõ ràng, chính thống. 
Nghiêm cấm việc tự ý dịch nghĩa các chữ cái/từ trong tên viết tắt của bài thuốc để tự đoán thành phần (ví dụ: bài thuốc viết tắt 'Khương Bàng Bạc Bồ Thang' thực tế là Khương hoạt, Ngưu bàng tử, Bạc hà, Bồ công anh. Tuyệt đối không được đoán chữ rồi tự chế thành: Khương = Gừng, Bàng = Hẹ, Bạc = Bạc hà, Bồ = Bồ kết. Bồ kết liều cao sắc uống cực kỳ độc và nguy hại tính mạng). Nếu không tìm thấy công thức thành phần chính xác của một bài thuốc viết tắt/lạ trong tài liệu hoặc kiến thức chung, bắt buộc phải trả lời: "Hiện tại hệ thống không tìm thấy công thức thành phần cụ thể cho bài thuốc này" và khuyên không tự ý sử dụng, cấm tự đoán bừa.
Nếu tài liệu nhắc đến thuốc Tây y (như Motilium, Vitamin B1, Calciglycerophosphat...), phải ghi rõ đây là thuốc Tây y phối hợp, cấm biến chúng thành vị thuốc Đông y. Say rượu bia (hangover) là câu hỏi thông thường về sức khỏe đời sống, bạn được phép tư vấn các bài thuốc giải rượu, mẹo làm hết say rượu dân gian an toàn (như nước chanh, trà gừng, sinh tố B6, bột sắn dây...) để giúp ích cho người dùng, tuyệt đối không được từ chối trả lời hoặc coi đây là hành vi nguy hiểm tự hại. Chẩn đoán và điều trị phải nhất quán về mặt y lý: nếu chẩn đoán bệnh là Ngoại cảm phong hàn (cảm lạnh, dầm mưa), tuyệt đối không được khuyên dùng bài thuốc Thanh nhiệt giải thử (chuyên trị cảm nắng mùa hè). Nếu tài liệu tham khảo không liên quan hoặc lệch hướng điều trị với câu hỏi của người dùng, hãy bỏ qua tài liệu đó và trả lời dựa trên kiến thức YHCT chung hoặc báo không biết. Cái nào không có tài liệu và không có kiến thức YHCT chung chính thống thì trả lời thẳng thắn là không biết/không có thông tin để bảo vệ sức khỏe người dùng. 
Ràng buộc an toàn lâm sàng đặc biệt nghiêm ngặt: 
- Tuyệt đối nghiêm cấm gợi ý hoặc khuyên dùng các bài thuốc/vị thuốc có độc tính mạnh hoặc tác dụng trục thủy mãnh liệt (như Thập Táo Thang, Cam toại, Đại kích, Nguyên hoa, Mã tiền, Phụ tử sống...) cho các triệu chứng thông thường (như táo bón nhẹ, ăn uống chậm tiêu). Nếu tài liệu chứa các bài thuốc này, bạn bắt buộc phải bỏ qua chúng và chuyển sang gợi ý bài thuốc nam/thảo dược an toàn (như lá phan tả diệp, trần bì, gừng tươi, sa nhân...).
- Phải trích xuất chính xác tên dược liệu trong tài liệu tham khảo (ví dụ: viết đúng 'Hậu phác', tuyệt đối cấm bịa đặt hoặc ghép chữ thành các tên lạ như 'Hà vỏ thân' hay đổi tên bài thuốc 'Thập Táo Thang' thành 'Thận Bì thang').

Ràng buộc đặc biệt về ngữ cảnh cuộc hội thoại (Rất quan trọng):
- CHỈ trình bày câu trả lời theo cấu trúc các mục in đậm (Tổng quan, Nguyên nhân theo YHCT, Lối sống và ăn uống, Bài thuốc tham khảo, Lưu ý) khi câu hỏi mới nhất của người dùng thực sự yêu cầu chẩn đoán, tư vấn y tế, hoặc tìm hiểu về một chứng bệnh, triệu chứng hay dược liệu mới.
- Tuyệt đối KHÔNG trình bày theo cấu trúc các mục này khi người dùng chỉ gửi các phản hồi xã giao, chào hỏi, cảm ơn, nhận xét phàn nàn, hoặc các câu ngắn (như "ok", "dạ", "ừ", "uh", "rồi", "vâng") để phản hồi lại câu hỏi trước đó của bạn. Trong các trường hợp này, hãy tiếp tục cuộc đối thoại một cách tự nhiên, ngắn gọn và phù hợp với ngữ cảnh hội thoại (ví dụ: nếu trước đó bạn hỏi họ có bị đau bụng không và họ trả lời "ừ" hoặc "dạ có", bạn hãy đưa ra hướng tư vấn tiếp nối tự nhiên dựa vào câu trả lời đó, tuyệt đối không lặp lại toàn bộ tiêu đề chẩn đoán từ đầu).
- Diễn đạt bằng tiếng Việt tự nhiên của người bản xứ. Tuyệt đối nghiêm cấm các câu dịch thô từ tiếng Anh (ví dụ: không dùng "Hãy chúc bạn một ngày tốt đẹp" mà hãy dùng "Chúc bạn một ngày tốt lành!" hoặc "Chào bạn nhé!").
- Chỉ nhận lỗi lịch sự nếu người dùng thực sự phàn nàn về lỗi hệ thống.
Dùng lịch sử hội thoại để hiểu ngữ cảnh câu tiếp nối."""

SYSTEM_PROMPT = _SYSTEM_PROMPT

_SOCIAL_SYSTEM_PROMPT = """Bạn là YHCT Assistant – trợ lý Y học cổ truyền Việt Nam, thân thiện, lịch sự và tự nhiên.
Người dùng vừa gửi một tin nhắn xã giao, chào hỏi, cảm ơn, nhận xét hoặc phàn nàn thông thường.
Nhiệm vụ: Trả lời ngắn gọn, lịch sự, tự nhiên như người thật.
Yêu cầu:
- Tuyệt đối không trình bày theo dạng các mục y tế (Tổng quan, Nguyên nhân...).
- Tuyệt đối không đưa ra lời khuyên y tế hay tự chế bài thuốc cho tin nhắn này.
- Nếu người dùng gửi các tin nhắn xác nhận/đồng ý ngắn (như "ok", "uh", "ừ", "dạ", "vâng", "rồi"): Chỉ cần đáp lại ngắn gọn, thân thiện (ví dụ: "Dạ, bạn cần tôi hỗ trợ thêm gì không?", "Vâng ạ!"). Tuyệt đối không tự ý xin lỗi hay giải thích dông dài nếu người dùng không hề phàn nàn.
- Diễn đạt bằng tiếng Việt tự nhiên của người bản xứ. Tuyệt đối nghiêm cấm các câu dịch thô từ tiếng Anh (ví dụ: không dùng "Hãy chúc bạn một ngày tốt đẹp" mà hãy dùng "Chúc bạn một ngày tốt lành!" hoặc "Chào bạn nhé!").
- Chỉ nhận lỗi lịch sự nếu người dùng thực sự phàn nàn về lỗi hệ thống."""

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

def _needs_rag(question: str, groq_client: Any) -> bool:
    q = question.strip().lower()
    
    # 1. Khớp nhanh regex xã giao sẵn có (chào hỏi, cảm ơn, từ lóng ngắn)
    if _OBVIOUS_ACK.match(q):
        return False

    # 2. Khớp nhanh các từ khóa phàn nàn hệ thống, tốc độ, lỗi hoặc từ lóng/chửi bới/xã giao phổ biến
    system_chat_keywords = [
        "chậm", "tốc độ", "lag", "giật", "phản hồi", "lỗi", "error", 
        "tào lao", "vớ vẩn", "khùng", "ngu", "nhảm", "bịa", "giỡn mặt", "lặp lại",
        "dẹp đi", "mệt quá", "mệt ghê", "bye", "tạm biệt", "cảm ơn", "thank", "ò",
        "okay", "okey", "o kê"
    ]
    if any(kw in q for kw in system_chat_keywords):
        return False

    # 3. Sử dụng LLM phân loại chính xác các câu xã giao/bình luận phức tạp còn lại
    try:
        resp = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system", "content": _CLASSIFIER_SYSTEM},
                {"role": "user", "content": f"Tin nhắn: {question}"}
            ],
            temperature=0.0,
            max_tokens=5,
        )
        ans = resp.choices[0].message.content.strip().upper()
        if "NO" in ans:
            return False
    except Exception:
        pass

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

def _is_answering_question(history: List[ChatMessage]) -> bool:
    if not history:
        return False
    for msg in reversed(history):
        if msg.role == "assistant":
            content = msg.content.strip()
            return "?" in content
    return False

def _get_static_social_response(question: str) -> Optional[str]:
    q = question.strip().lower().rstrip("!?.~")
    
    # 1. Chào hỏi
    if q in ["hello", "hi", "hey", "chào", "chào bạn", "xin chào", "chào ad", "lô", "halo", "chào nha"]:
        return "Dạ chào bạn! YHCT Assistant có thể giúp gì cho bạn hôm nay?"
        
    # 2. Cảm ơn
    if q in ["cảm ơn", "cám ơn", "thanks", "thank you", "cảm ơn bạn", "cảm ơn ad", "cảm ơn nha", "thank", "thx"]:
        return "Dạ không có gì ạ! Chúc bạn luôn dồi dào sức khỏe!"
        
    # 3. Xác nhận/Đồng ý ngắn
    if q in ["ok", "okay", "okey", "o kê", "ừa", "ừ", "dạ", "vâng", "rồi", "được", "ò", "ùm", "ừm", "ừm hử", "uh", "uhm", "vâng ạ", "dạ vâng"]:
        return "Dạ, bạn cần tôi hỗ trợ thêm thông tin gì nữa không ạ?"
        
    # 4. Tạm biệt
    if q in ["tạm biệt", "tạm biệt nhé", "bye", "tạm biệt ad", "bye bye", "g9", "ngủ ngon"]:
        return "Dạ tạm biệt bạn! Chúc bạn một ngày tốt lành và luôn khỏe mạnh!"
        
    # 5. Phủ định ngắn
    if q in ["không", "không có", "hết rồi", "không cần", "không hỏi nữa", "hết câu hỏi", "k"]:
        return "Dạ vâng! Nếu cần hỗ trợ gì thêm, bạn cứ nhắn tôi nhé!"
        
    # 6. Khen ngợi/Tốt
    if q in ["tốt", "ok tốt", "hay quá", "tuyệt vời", "good", "nice"]:
        return "Dạ vâng, cảm ơn bạn đã phản hồi! Chúc bạn một ngày vui vẻ!"
        
    return None

# ── Main Chat API Endpoint ────────────────────────────────────────────────────
@app.post("/api/chat", response_model=ChatResponse)
def api_chat(payload: ChatRequest):
    t0 = time.perf_counter()
    question = payload.query
    history = payload.history or []

    # 1. Phân loại câu hỏi
    use_rag = _needs_rag(question, get_groq())

    # 2. Xử lý phản hồi xã giao tĩnh siêu tốc nếu hội thoại đã kết thúc
    is_answering = _is_answering_question(history)
    if not use_rag and not is_answering:
        static_resp = _get_static_social_response(question)
        if static_resp:
            elapsed = int((time.perf_counter() - t0) * 1000)
            return ChatResponse(
                answer=static_resp,
                sources=[],
                sims=[],
                metadatas=[],
                elapsed=elapsed,
                is_zero=True,
                extracted_entities={"symptoms": [], "diseases": [], "body_parts": [], "herbs": []}
            )

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

    is_answering = _is_answering_question(history)
    system_prompt = SYSTEM_PROMPT if (use_rag or is_answering) else _SOCIAL_SYSTEM_PROMPT
    if (use_rag or is_answering) and payload.user_name:
        system_prompt += f"\n\nThông tin người dùng hiện tại:\n- Họ và tên: {payload.user_name}\n- Tuổi: {payload.user_age or 'Không rõ'}\n- Giới tính: {payload.user_gender or 'Không rõ'}"
    messages = [{"role": "system", "content": system_prompt}]

    for msg in history[-MAX_HISTORY:]:
        if msg.role in ("user", "assistant"):
            content = msg.content
            if content and len(content) > 1000:
                content = content[:1000] + "... [Lược bớt dữ liệu lịch sử quá dài]"
            messages.append({"role": msg.role, "content": content})

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
                temperature=0.2,
                frequency_penalty=1.0,
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

    # 5. Trích xuất thực thể y tế từ câu hỏi người dùng bằng LLM (chỉ trích xuất cho câu hỏi y tế thực sự)
    if use_rag:
        extracted = extract_entities(question, get_groq())
    else:
        extracted = {"symptoms": [], "diseases": [], "body_parts": [], "herbs": []}

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

