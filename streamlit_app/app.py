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
.stApp { background-color: #f0f4f0; }
#MainMenu, footer, header { visibility: hidden; }

.chat-header {
    background: linear-gradient(135deg, #1a6b3c, #2d9e5f);
    padding: 20px 28px;
    border-radius: 16px;
    margin-bottom: 24px;
    color: white;
}
.chat-header h1 { margin:0; font-size:24px; font-weight:700; color:white !important; }
.chat-header p  { margin:4px 0 0; font-size:14px; opacity:0.85; color:white !important; }

.user-bubble {
    background: #1a6b3c; color: white;
    padding: 12px 18px;
    border-radius: 18px 18px 4px 18px;
    margin: 8px 0 8px 20%;
    font-size: 15px; line-height: 1.5;
}
.assistant-bubble {
    background: white; color: #1a1a1a;
    padding: 14px 18px;
    border-radius: 18px 18px 18px 4px;
    margin: 8px 20% 8px 0;
    font-size: 15px; line-height: 1.6;
    border: 1px solid #e0e0e0;
}
.source-tag {
    display: inline-block;
    background: #f0f7f3; color: #1a6b3c;
    border: 1px solid #c0dfc9;
    border-radius: 20px;
    padding: 3px 10px; font-size: 12px;
    margin: 4px 4px 0 0;
    text-decoration: none;
}
.source-tag:hover { background: #d8efe2; }
.meta-info { font-size:12px; color:#888; margin-top:6px; }
.welcome-box {
    background: white; border: 1px solid #e0e0e0;
    border-radius: 16px; padding: 28px;
    text-align: center; margin: 40px auto; max-width: 520px;
}
.welcome-box h3 { color:#1a6b3c; font-size:20px; margin-bottom:8px; }
.welcome-box p  { color:#666; font-size:14px; margin-bottom:20px; }
.suggestion-btn {
    background:#f0f7f3; border:1px solid #c0dfc9;
    border-radius:20px; padding:8px 16px;
    font-size:13px; color:#1a6b3c;
    margin:4px; display:inline-block;
}
</style>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────────
if "session_id" not in st.session_state:
    st.session_state.session_id = uuid.uuid4().hex[:10]
if "messages" not in st.session_state:
    st.session_state.messages = []

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="chat-header">
    <h1>🌿 YHCT Assistant</h1>
    <p>Hệ thống tra cứu Y học cổ truyền — Bệnh tiêu hóa</p>
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
            f'<a href="{pdf_url}" target="_blank" class="source-tag">'
            f'📚 {book_name} · tr.{", ".join(pages_sorted)}'
            f'</a>'
        )
    return "".join(parts)


# ── Chat history ──────────────────────────────────────────────────────────────
if not st.session_state.messages:
    st.markdown("""
    <div class="welcome-box">
        <h3>Xin chào! Tôi có thể giúp gì cho bạn?</h3>
        <p>Hỏi về bài thuốc, dược liệu hoặc triệu chứng bệnh tiêu hóa theo YHCT.</p>
        <div>
            <span class="suggestion-btn">🌱 Bài thuốc trị đau dạ dày</span>
            <span class="suggestion-btn">💊 Cam thảo có tác dụng gì?</span>
            <span class="suggestion-btn">🏥 Chữa táo bón bằng thảo dược</span>
            <span class="suggestion-btn">🌿 Bài thuốc trị tiêu chảy</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
else:
    for msg in st.session_state.messages:
        if msg["role"] == "user":
            st.markdown(
                f'<div class="user-bubble">{msg["content"]}</div>',
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
                f'<div class="meta-info">⏱ {msg.get("elapsed", 0)}ms</div>'
                if msg.get("sources")
                else ""
            )
            sources_block = (
                f'<div style="margin-top:10px">{sources_html}</div>'
                if sources_html
                else ""
            )
            st.markdown(f"""
            <div class="assistant-bubble">
                {msg["content"]}
                {sources_block}
                {timer_html}
            </div>
            """, unsafe_allow_html=True)

# ── Input ─────────────────────────────────────────────────────────────────────
question = st.chat_input("Hỏi về bài thuốc, dược liệu, triệu chứng...")

if question:
    history = list(st.session_state.messages)

    st.session_state.messages.append({
        "role": "user", "content": question
    })

    with st.spinner("💬 Đang suy nghĩ..."):
        result = query_rag(question, history=history)

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
