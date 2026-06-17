# streamlit_app/app.py

import os
import streamlit as st
import uuid
from rag import query_rag

MINIO_PUBLIC_URL = os.getenv("MINIO_PUBLIC_URL", "http://localhost:9000")

st.set_page_config(
    page_title="YHCT Chatbot",
    page_icon="🌿",
    layout="wide"
)

# ── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"], .stApp {
    font-family: 'Outfit', sans-serif !important;
}

#MainMenu, footer, header { visibility: hidden; }

.chat-header {
    background-color: #ffffff;
    border-bottom: 1.5px solid #eef4f0;
    border-top: none;
    border-left: none;
    border-right: none;
    padding: 16px 24px;
    margin-bottom: 28px;
    border-radius: 14px;
    box-shadow: 0 4px 15px rgba(26, 107, 60, 0.04);
}
.chat-header h3 { margin: 0; font-size: 20px; font-weight: 700; color: #1a6b3c !important; }
.chat-header p  { margin: 4px 0 0; font-size: 13.5px; color: #555555 !important; opacity: 0.8; }

.user-bubble {
    background: linear-gradient(135deg, #1a6b3c 0%, #2d9e5f 100%);
    color: white;
    padding: 14px 20px;
    border-radius: 20px 20px 4px 20px;
    margin: 8px 0 8px auto;
    width: fit-content;
    max-width: 75%;
    font-size: 15px; line-height: 1.5;
    box-shadow: 0 4px 12px rgba(26, 107, 60, 0.15);
}
.assistant-bubble {
    background-color: #ffffff;
    color: #1c3225;
    padding: 18px 22px;
    border-radius: 20px 20px 20px 4px;
    margin: 8px auto 8px 0;
    width: fit-content;
    max-width: 75%;
    font-size: 15px; line-height: 1.6;
    border: 1.5px solid #eef4f0;
    box-shadow: 0 4px 12px rgba(26, 107, 60, 0.02);
}
.source-tag {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    background-color: rgba(45, 158, 95, 0.08);
    color: #1a6b3c;
    border: 1px solid rgba(45, 158, 95, 0.15);
    border-radius: 20px;
    padding: 4px 12px; font-size: 12.5px;
    margin: 6px 6px 0 0;
    text-decoration: none;
    font-weight: 500;
    transition: all 0.2s ease;
}
.source-tag:hover {
    background-color: rgba(45, 158, 95, 0.15) !important;
    transform: translateY(-1px);
    box-shadow: 0 2px 6px rgba(26, 107, 60, 0.1);
    color: #1a6b3c !important;
}
.meta-info { font-size:12px; color: #888888; opacity: 0.7; margin-top:8px; }

/* Custom styled Streamlit buttons to look like Suggestion Cards */
div.stButton > button {
    text-align: left !important;
    padding: 20px !important;
    border-radius: 16px !important;
    border: 1px solid #eef4f0 !important;
    background-color: #ffffff !important;
    color: #555555 !important;
    min-height: 120px !important;
    display: block !important;
    width: 100% !important;
    white-space: pre-wrap !important;
    line-height: 1.5 !important;
    box-shadow: 0 4px 12px rgba(26, 107, 60, 0.03) !important;
    transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1) !important;
}
div.stButton > button:hover {
    border-color: #2d9e5f !important;
    background-color: rgba(45, 158, 95, 0.04) !important;
    transform: translateY(-2px) !important;
    box-shadow: 0 8px 24px rgba(26, 107, 60, 0.08) !important;
}
div.stButton > button p,
div.stButton > button span {
    font-size: 13.5px !important;
    color: #555555 !important;
    margin: 0 !important;
    line-height: 1.5 !important;
}
div.stButton > button p::first-line,
div.stButton > button span::first-line,
div.stButton > button::first-line {
    font-size: 15px !important;
    font-weight: 600 !important;
    color: #1a6b3c !important;
    line-height: 1.8 !important;
}

/* Custom chat input styling to match user web chat */
div[data-testid="stChatInput"] {
    padding: 12px 0 !important;
}
div[data-testid="stChatInput"] textarea {
    border: 1.5px solid #c0dfc9 !important;
    border-radius: 12px !important;
    background-color: #ffffff !important;
    color: #1a1a1a !important;
    padding: 12px 16px !important;
}
div[data-testid="stChatInput"] textarea:focus {
    border-color: #2d9e5f !important;
    box-shadow: 0 0 0 3px rgba(45, 158, 95, 0.15) !important;
}

/* Dark mode overrides */
@media (prefers-color-scheme: dark) {
    .chat-header {
        background-color: #1e221f;
        border-color: #2d5a3c;
    }
    .chat-header h3 {
        color: #4cd184 !important;
    }
    .chat-header p {
        color: #b0c2b5 !important;
    }
    .assistant-bubble {
        background-color: #1e221f;
        color: #e2e8f0;
        border-color: #2d5a3c;
    }
    div.stButton > button {
        background-color: #1e221f !important;
        border-color: #2d5a3c !important;
        color: #b0c2b5 !important;
    }
    div.stButton > button:hover {
        background-color: rgba(45, 158, 95, 0.08) !important;
        border-color: #2d9e5f !important;
    }
    div.stButton > button p,
    div.stButton > button span {
        color: #b0c2b5 !important;
    }
    div.stButton > button p::first-line,
    div.stButton > button span::first-line,
    div.stButton > button::first-line {
        color: #4cd184 !important;
    }
    div[data-testid="stChatInput"] textarea {
        background-color: #1e221f !important;
        border-color: #2d5a3c !important;
        color: #e2e8f0 !important;
    }
}
</style>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────────
if "session_id" not in st.session_state:
    st.session_state.session_id = uuid.uuid4().hex[:10]
if "messages" not in st.session_state:
    st.session_state.messages = []
if "selected_suggestion" not in st.session_state:
    st.session_state.selected_suggestion = None

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="chat-header">
    <h3>🌿 Trợ lý Y học Cổ truyền AI</h3>
    <p>Hệ thống tra cứu bài thuốc và phương pháp điều trị Y học Cổ truyền</p>
</div>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _book_display_name(source_file: str) -> str:
    """Chuyển tên file PDF thành tên sách đẹp hơn để hiển thị."""
    return source_file.replace(".pdf", "").replace("_", " ").title()


def _build_sources_html(sources: list, sims: list, metadatas: list) -> str:
    """
    Nhóm sources theo tên sách, mỗi sách hiển thị một lần kèm danh sách trang.
    Mỗi tag là link PDF có thể bấm (trỏ tới MinIO).
    """
    books: dict[str, dict] = {}
    for src, sim, meta in zip(sources, sims, metadatas):
        sf   = meta.get("source", src)
        page = str(meta.get("page_num", "?"))
        if sf not in books:
            books[sf] = {"pages": set(), "max_sim": sim}
        books[sf]["pages"].add(page)
        books[sf]["max_sim"] = max(books[sf]["max_sim"], sim)

    parts = []
    for sf, data in books.items():
        pages_sorted = sorted(
            data["pages"],
            key=lambda x: int(x) if x.isdigit() else 0,
        )
        book_name = _book_display_name(sf)
        pdf_url   = f"{MINIO_PUBLIC_URL}/yhct-docs/{sf}"
        parts.append(
            f'<a href="{pdf_url}" download="{sf}" target="_blank" class="source-tag">'
            f'📚 {book_name} · tr.{", ".join(pages_sorted)}'
            f'</a>'
        )
    return "".join(parts)


# ── Chat history ──────────────────────────────────────────────────────────────
if not st.session_state.messages:
    st.markdown('<div style="text-align:center; padding: 20px 0 10px 0;"><span style="font-size:4rem;">🌿</span></div>', unsafe_allow_html=True)
    st.markdown('<h2 style="text-align:center; font-weight:700; color:#1a6b3c; margin-bottom:12px; font-family:\'Outfit\'">Xin chào! Tôi có thể giúp gì cho bạn?</h2>', unsafe_allow_html=True)
    st.markdown('<p style="text-align:center; color:#555; max-width:640px; margin: 0 auto 32px auto; font-size:14.5px; line-height:1.6; font-family:\'Outfit\'">Hỏi về bài thuốc, dược liệu hoặc triệu chứng bệnh tiêu hóa theo YHCT.</p>', unsafe_allow_html=True)
    
    col_l, col_c, col_r = st.columns([1, 4, 1])
    with col_c:
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🌱 Bài thuốc trị đau dạ dày\nTìm hiểu các bài thuốc giảm đau và viêm loét dạ dày.", key="sug_1", use_container_width=True):
                st.session_state.selected_suggestion = "Bài thuốc trị đau dạ dày"
                st.rerun()
            if st.button("🏥 Chữa táo bón bằng thảo dược\nCác giải pháp nhuận tràng từ dược liệu tự nhiên.", key="sug_3", use_container_width=True):
                st.session_state.selected_suggestion = "Chữa táo bón bằng thảo dược"
                st.rerun()
        with col2:
            if st.button("💊 Cam thảo có tác dụng gì?\nTra cứu công dụng và lưu ý khi sử dụng cam thảo.", key="sug_2", use_container_width=True):
                st.session_state.selected_suggestion = "Cam thảo có tác dụng gì?"
                st.rerun()
            if st.button("🌿 Bài thuốc trị tiêu chảy\nTra cứu các bài thuốc đông y trị tiêu chảy thường gặp.", key="sug_4", use_container_width=True):
                st.session_state.selected_suggestion = "Bài thuốc trị tiêu chảy"
                st.rerun()
else:
    for msg in st.session_state.messages:
        if msg["role"] == "user":
            st.markdown(
                f'<div class="user-bubble">\n{msg["content"]}\n</div>',
                unsafe_allow_html=True
            )
        else:
            sources_html = _build_sources_html(
                msg.get("sources",   []),
                msg.get("sims",      []),
                msg.get("metadatas", []),
            )
            # Hiển thị timer chỉ khi có RAG (không phải tin xã giao)
            timer_html = (
                f'\n<div class="meta-info">⏱ {msg.get("elapsed", 0)}ms</div>\n'
                if msg.get("sources")
                else ""
            )
            sources_block = (
                f'\n<div style="margin-top:10px">{sources_html}</div>\n'
                if sources_html
                else ""
            )
            st.markdown(
f"""<div class="assistant-bubble">
{msg["content"]}
{sources_block}{timer_html}
</div>""", 
                unsafe_allow_html=True
            )

# ── Input ─────────────────────────────────────────────────────────────────────
question = st.chat_input("Hỏi về bài thuốc, dược liệu, triệu chứng...")

query = question or st.session_state.selected_suggestion

if query:
    st.session_state.selected_suggestion = None
    history = list(st.session_state.messages)

    st.session_state.messages.append({
        "role": "user", "content": query
    })

    with st.spinner("💬 Đang suy nghĩ..."):
        result = query_rag(query, history=history)

    st.session_state.messages.append({
        "role":      "assistant",
        "content":   result["answer"],
        "sources":   result["sources"],
        "sims":      result["sims"],
        "metadatas": result["metadatas"],
        "elapsed":   result["elapsed"],
    })

    st.rerun()

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="text-align:center; margin-top:40px; font-size:12px; color:#aaa;">
    Session {st.session_state.session_id} ·
    {len(st.session_state.messages)//2} câu hỏi
</div>
""", unsafe_allow_html=True)
