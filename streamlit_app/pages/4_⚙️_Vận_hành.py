# streamlit_app/pages/3_operations.py

import os
import streamlit as st
import httpx
import datetime

st.set_page_config(page_title="YHCT Operations", page_icon="⚙️", layout="wide")

DAGSTER_URL = os.getenv("DAGSTER_URL", "http://dagster:3001")
AI_SERVICE_URL = os.getenv("AI_SERVICE_URL", "http://ai-service:8000")

# ── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"], .stApp {
    font-family: 'Outfit', sans-serif !important;
}

.card {
    background-color: var(--secondary-background-color, #ffffff);
    color: var(--text-color, #1a1a1a);
    border-radius: 20px;
    padding: 24px;
    border: 1px solid var(--border-color, #c0dfc9);
    margin-bottom: 24px;
    box-shadow: 0 8px 24px rgba(26, 107, 60, 0.04);
    transition: all 0.3s ease;
}
.card:hover {
    transform: translateY(-2px);
    box-shadow: 0 12px 30px rgba(26, 107, 60, 0.08);
    border-color: #1a6b3c;
}
.check-pass {
    color: #2e7d32;
    font-weight: bold;
}
.check-fail {
    color: #c62828;
    font-weight: bold;
}
.check-none {
    color: #ef6c00;
    font-style: italic;
}
</style>
""", unsafe_allow_html=True)

st.title("⚙️ Giám sát & Vận hành Kỹ thuật")
st.markdown("---")

# ── Dagster helpers ───────────────────────────────────────────────────────────
def _discover_repo_info() -> tuple[str, str, str] | None:
    query = """
    query {
      repositoriesOrError {
        ... on RepositoryConnection {
          nodes {
            name
            location { name }
            pipelines { name }
          }
        }
        ... on PythonError { message }
      }
    }
    """
    try:
        r = httpx.post(f"{DAGSTER_URL}/graphql", json={"query": query}, timeout=10)
        r.raise_for_status()
        data = r.json()
        nodes = data["data"]["repositoriesOrError"].get("nodes", [])
        if not nodes:
            return None
        repo_name = nodes[0]["name"]
        loc_name  = nodes[0]["location"]["name"]
        pipelines = [p["name"] for p in nodes[0].get("pipelines", [])]
        job_name = "__ASSET_JOB" if "__ASSET_JOB" in pipelines else (pipelines[0] if pipelines else "__ASSET_JOB")
        return loc_name, repo_name, job_name
    except Exception:
        return None

def _launch_pipeline(loc_name: str, repo_name: str, job_name: str) -> tuple[str | None, str | None]:
    mutation = """
    mutation LaunchRun($executionParams: ExecutionParams!) {
      launchRun(executionParams: $executionParams) {
        ... on LaunchRunSuccess {
          run { runId status }
        }
        ... on PipelineNotFoundError { message }
        ... on InvalidSubsetError    { message }
        ... on PythonError           { message }
      }
    }
    """
    variables = {
        "executionParams": {
            "selector": {
                "repositoryLocationName": loc_name,
                "repositoryName":         repo_name,
                "jobName":                job_name,
            },
            "executionMetadata": {},
            "runConfigData":     "{}",
        }
    }
    try:
        r = httpx.post(
            f"{DAGSTER_URL}/graphql",
            json={"query": mutation, "variables": variables},
            timeout=30,
        )
        r.raise_for_status()
        result = r.json()["data"]["launchRun"]
        if "run" in result:
            return result["run"]["runId"], None
        elif "message" in result:
            return None, result["message"]
    except Exception as e:
        return None, str(e)
    return None, "Unknown error"

def _get_asset_checks_status() -> list[dict]:
    query = """
    query {
      assetNodes {
        assetKey { path }
        assetChecks {
          name
          description
          latestEvaluation {
            passed
            metadataEntries {
              label
              textValue
              floatValue
              intValue
            }
          }
        }
      }
    }
    """
    try:
        r = httpx.post(f"{DAGSTER_URL}/graphql", json={"query": query}, timeout=10)
        r.raise_for_status()
        data = r.json()
        nodes = data["data"].get("assetNodes", [])
        checks = []
        for node in nodes:
            path = " -> ".join(node["assetKey"]["path"])
            for check in node.get("assetChecks", []):
                passed = None
                metadata = {}
                eval_info = check.get("latestEvaluation")
                if eval_info:
                    passed = eval_info.get("passed")
                    for entry in eval_info.get("metadataEntries", []):
                        val = entry.get("textValue") or entry.get("floatValue") or entry.get("intValue")
                        metadata[entry["label"]] = val
                checks.append({
                    "asset": path,
                    "name": check["name"],
                    "description": check.get("description", ""),
                    "passed": passed,
                    "metadata": metadata
                })
        return checks
    except Exception:
        return []

# Layout columns
col_rag, col_dagster = st.columns([1, 1])

# ── COLUMN 1: RAG CONFIGURATION ───────────────────────────────────────────────
with col_rag:
    st.subheader("🌿 Cấu hình tham số AI RAG")
    st.markdown("Cập nhật các tham số hoạt động của Chatbot AI và System Prompt mà không cần restart container.")
    
    # Fetch current configuration
    current_config = None
    try:
        r = httpx.get(f"{AI_SERVICE_URL}/api/config", timeout=5)
        if r.status_code == 200:
            current_config = r.json()
    except Exception as e:
        st.error(f"❌ Không thể kết nối với AI Service tại `{AI_SERVICE_URL}`. Lỗi: {e}")
        
    if current_config:
        st.success("✅ Đã kết nối với AI Service")
        
        with st.form("rag_config_form"):
            min_sim = st.slider(
                "Độ khớp tối thiểu (MIN_SIM - Cosine Similarity Threshold)",
                min_value=0.0, max_value=1.0, 
                value=float(current_config.get("min_sim", 0.40)), 
                step=0.05,
                help="Các tài liệu tham khảo có độ khớp nhỏ hơn giá trị này sẽ bị bỏ qua."
            )
            
            top_k = st.slider(
                "Số lượng văn bản tham chiếu (TOP_K chunks)",
                min_value=1, max_value=10, 
                value=int(current_config.get("top_k", 5)), 
                step=1,
                help="Số lượng chunks tài liệu tối đa gửi cho LLM làm ngữ cảnh trả lời."
            )
            
            groq_model = st.text_input(
                "Mô hình LLM sử dụng (Groq Model ID)",
                value=current_config.get("groq_model", "llama-3.1-8b-instant")
            )
            
            sys_prompt = st.text_area(
                "System Prompt (Chỉ dẫn AI)",
                value=current_config.get("system_prompt", ""),
                height=300,
                help="Chỉ dẫn cấu trúc trả lời, phong cách tư vấn của Trợ lý YHCT."
            )
            
            submit_btn = st.form_submit_button("💾 Lưu và Cập nhật cấu hình", type="primary")
            
            if submit_btn:
                update_payload = {
                    "min_sim": min_sim,
                    "top_k": top_k,
                    "system_prompt": sys_prompt,
                    "groq_model": groq_model
                }
                try:
                    resp = httpx.post(f"{AI_SERVICE_URL}/api/config", json=update_payload, timeout=5)
                    if resp.status_code == 200:
                        st.toast("✅ Cập nhật cấu hình RAG thành công!", icon="🌿")
                        st.rerun()
                    else:
                        st.error(f"Cập nhật thất bại: {resp.text}")
                except Exception as ex:
                    st.error(f"Lỗi gửi yêu cầu cập nhật: {ex}")
    else:
        st.warning("⚠ Không thể lấy thông tin cấu hình từ AI Service. Hãy kiểm tra dịch vụ.")

# ── COLUMN 2: DATA QUALITY & PIPELINE ────────────────────────────────────────
with col_dagster:
    st.subheader("🤖 Giám sát chất lượng dữ liệu & Pipeline")
    
    # 1. Pipeline trigger
    st.markdown("#### 1. Kích hoạt Pipeline")
    repo_info = _discover_repo_info()
    if repo_info:
        loc_name, repo_name, job_name = repo_info
        st.info(f"📍 **Dagster Job:** `{job_name}` · **Location:** `{loc_name}`")
        if st.button("🚀 Khởi chạy toàn bộ Pipeline (Bronze -> Silver -> Gold)", type="primary"):
            run_id, err = _launch_pipeline(loc_name, repo_name, job_name)
            if run_id:
                st.success(f"✅ Đã trigger thành công! Run ID: `{run_id[:8]}`")
                st.markdown(f"[Mở Dagster UI để theo dõi]({DAGSTER_URL}/runs/{run_id})")
            else:
                st.error(f"❌ Kích hoạt thất bại: {err}")
    else:
        st.warning("⚠ Không thể kết nối với Dagster. Hãy kiểm tra container `dagster`.")
        
    st.markdown("---")
    
    # 2. Data Quality Checks (Asset Checks)
    st.markdown("#### 2. Kết quả kiểm tra chất lượng (Data Quality Gates)")
    checks = _get_asset_checks_status()
    if checks:
        for check in checks:
            passed = check["passed"]
            if passed is True:
                status_str = '<span class="check-pass">✔ PASS</span>'
            elif passed is False:
                status_str = '<span class="check-fail">✘ FAIL</span>'
            else:
                status_str = '<span class="check-none">Chưa chạy</span>'
                
            st.markdown(f"""
            <div class="card">
                <h5>🛡 Asset check: <code>{check['name']}</code></h5>
                <p style="font-size: 13px; margin: 4px 0;"><b>Mô tả:</b> {check['description']}</p>
                <p style="font-size: 13px; margin: 4px 0;"><b>Tầng dữ liệu:</b> <code>{check['asset']}</code></p>
                <p style="font-size: 14px; margin: 8px 0 0 0;"><b>Kết quả:</b> {status_str}</p>
            """, unsafe_allow_html=True)
            
            # Show metadata if passed
            if check["metadata"]:
                st.markdown("**Siêu dữ liệu kiểm tra:**")
                st.json(check["metadata"])
            st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("Chưa có kết quả Data Quality check nào được ghi lại, hoặc Dagster chưa được chạy.")
