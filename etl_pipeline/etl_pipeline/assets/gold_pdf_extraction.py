# etl_pipeline/etl_pipeline/assets/gold_pdf_extraction.py
#
# Open Extraction (Khám phá Mở): Trích xuất cấu trúc từ PDF ra CSV.
# Sử dụng LLM để đọc Sách PDF và xuất ra dữ liệu có cấu trúc (Bài thuốc, Vị thuốc) 
# bám sát theo định dạng YHCT. Kết quả được lưu trực tiếp thành file CSV vật lý.

import json
import os
import re
import time
import polars as pl
from dagster import AssetIn, MetadataValue, Output, asset

# ── Trigger Keywords ────────────────────────────────────────────────────────
# Để tiết kiệm chi phí API, chỉ gửi cho LLM các đoạn văn có mùi bài thuốc/vị thuốc
_EXTRACTION_TRIGGER = re.compile(
    r"bài\s+thuốc|thang\s+thuốc|dược\s+liệu|vị\s+thuốc|chủ\s+trị|công\s+dụng|công\s+năng|"
    r"sắc\s+uống|tán\s+bột|ngày\s+uống|liều\s+dùng|trị\s+chứng|phương\s+thuốc",
    re.IGNORECASE,
)

# Cấu hình Batching cho LLM
_SAMPLE_LIMIT = 50   # Tạm giới hạn 50 chunks để chạy thử nhanh (có thể tăng lên khi chạy thật)
_BATCH_SIZE   = 3    # Số chunks gửi trong 1 lần gọi API
_SLEEP_SEC    = 2.1  # Giây nghỉ để tránh Rate Limit của Groq Free Tier

# Thư mục xuất file CSV vật lý
_OUTPUT_DIR = "/opt/dagster/app/data/extracted"

def _build_few_shot_prompt() -> str:
    """Đọc dữ liệu mẫu từ CSV gốc để làm Few-shot Prompt dạy AI."""
    base_dir = "/opt/dagster/app/data/raw/yhct"
    if not os.path.exists(base_dir):
        base_dir = os.path.join(os.path.dirname(__file__), "../../../../data/raw/yhct")
        
    prompt = """Bạn là một Chuyên gia Y Học Cổ Truyền (YHCT).
Nhiệm vụ của bạn là đọc các đoạn văn bản trích từ sách y học và bóc tách dữ liệu có cấu trúc.
Tuyệt đối KHÔNG bịa đặt thêm dữ liệu không có trong văn bản. Nếu không tìm thấy, hãy trả về danh sách rỗng.

YÊU CẦU ĐẦU RA (OUTPUT FORMAT):
Chỉ trả về DUY NHẤT một đối tượng JSON hợp lệ theo đúng cấu trúc sau:
{
  "formulas": [
    {
      "formula_name_vi": "Tên bài thuốc",
      "function": "Công năng",
      "indications": "Chủ trị",
      "usage": "Cách dùng",
      "ingredients": [
        {
          "herb_name": "Tên vị thuốc",
          "dosage_value": "Số lượng",
          "dosage_unit": "Đơn vị"
        }
      ]
    }
  ],
  "independent_herbs": [
    {
      "herb_name": "Tên vị thuốc",
      "temp_property": "Tính vị",
      "meridian": "Quy kinh",
      "indications": "Chủ trị"
    }
  ]
}

NẾU ĐOẠN VĂN KHÔNG CHỨA BÀI THUỐC HAY VỊ THUỐC NÀO, TRẢ VỀ:
{"formulas": [], "independent_herbs": []}

--- VÍ DỤ VỀ CÁCH BẠN PHẢI BÓC TÁCH (DỮ LIỆU CHUẨN TỪ TỪ ĐIỂN YHCT) ---
"""
    try:
        # Load a couple of formulas
        f_path = os.path.join(base_dir, "Formula.csv")
        if os.path.exists(f_path):
            df_f = pl.read_csv(f_path, n_rows=2)
            prompt += "\nVí dụ Bài Thuốc:\n"
            for row in df_f.iter_rows(named=True):
                prompt += f"- Tên: {row.get('formula_name_vi', '')}, Công năng: {row.get('function', '')}, Chủ trị: {row.get('indications', '')}\n"
                
        # Load a couple of herbs
        h_path = os.path.join(base_dir, "HerbMaterial.csv")
        if os.path.exists(h_path):
            df_h = pl.read_csv(h_path, n_rows=2)
            prompt += "\nVí dụ Vị Thuốc:\n"
            for row in df_h.iter_rows(named=True):
                prompt += f"- Tên: {row.get('herb_name', '')}, Tính vị: {row.get('temp_property', '')}, Chủ trị: {row.get('indication_vi', '')}\n"
    except Exception as e:
        print(f"Lỗi khi đọc file CSV làm mẫu: {e}")
        
    return prompt

def _call_extraction_llm(texts: list[str], client, system_prompt: str) -> dict:
    """Gọi LLM bằng Groq API để trích xuất JSON."""
    joined_text = "\n\n---\n\n".join(f"ĐOẠN VĂN {i+1}:\n{t}" for i, t in enumerate(texts))
    prompt = f"{system_prompt}\n\nĐỌC VÀ TRÍCH XUẤT TỪ CÁC ĐOẠN VĂN SAU:\n{joined_text}"
    
    try:
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=2000,
            response_format={"type": "json_object"} # Ép LLM trả về JSON chuẩn
        )
        content = resp.choices[0].message.content
        return json.loads(content)
    except Exception as e:
        print(f"LLM Error: {e}")
        return {"formulas": [], "independent_herbs": []}

@asset(
    name="gold_pdf_extraction",
    key_prefix=["gold", "extraction"],
    group_name="gold",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={"gold_yhct_chunks": AssetIn(key_prefix=["gold", "chunks"])},
    description="Automated Open Extraction: LLM reads PDF chunks and generates structured CSV datasets.",
)
def gold_pdf_extraction(context, gold_yhct_chunks: pl.DataFrame) -> Output:
    """
    Asset bóc tách tri thức mở từ Sách PDF ra file CSV.
    """
    groq_api_key = os.getenv("GROQ_API_KEY", "")
    if not groq_api_key:
        raise RuntimeError("Thiếu GROQ_API_KEY để chạy mô hình AI.")

    from groq import Groq
    client = Groq(api_key=groq_api_key)

    # Đảm bảo thư mục output tồn tại (fallback for local vs docker)
    out_dir = _OUTPUT_DIR
    if not os.path.exists(out_dir):
        out_dir = os.path.join(os.path.dirname(__file__), "../../../../data/extracted")
        os.makedirs(out_dir, exist_ok=True)

    context.log.info(f"📥 Bắt đầu bóc tách tri thức từ {gold_yhct_chunks.shape[0]} chunks.")

    # 1. Lọc mồi (Trigger Filtering)
    all_rows = gold_yhct_chunks.iter_rows(named=True)
    trigger_chunks = [r for r in all_rows if _EXTRACTION_TRIGGER.search(r["chunk_text"])]
    context.log.info(f"   Phát hiện {len(trigger_chunks)} chunks tiềm năng chứa Bài thuốc/Vị thuốc.")

    if len(trigger_chunks) > _SAMPLE_LIMIT:
        step = max(1, len(trigger_chunks) // _SAMPLE_LIMIT)
        trigger_chunks = trigger_chunks[::step][:_SAMPLE_LIMIT]
        context.log.info(f"   Đã sample lại còn {len(trigger_chunks)} chunks để chạy demo (tránh lố API).")

    # Danh sách lưu kết quả
    extracted_formulas = []
    extracted_components = []
    extracted_herbs = []

    llm_calls = 0

    # 2. Xử lý qua LLM
    system_prompt = _build_few_shot_prompt()
    for i in range(0, len(trigger_chunks), _BATCH_SIZE):
        batch = trigger_chunks[i : i + _BATCH_SIZE]
        batch_text = [r["chunk_text"] for r in batch]
        
        result_json = _call_extraction_llm(batch_text, client, system_prompt)
        llm_calls += 1
        
        # Parse kết quả
        formulas = result_json.get("formulas", [])
        indep_herbs = result_json.get("independent_herbs", [])
        
        source_doc = batch[0]["source_file"]
        chunk_id = batch[0]["chunk_id"]

        for f in formulas:
            f_name = f.get("formula_name_vi", "").strip()
            if not f_name:
                continue
            
            extracted_formulas.append({
                "formula_name_vi": f_name,
                "function": f.get("function", ""),
                "indications": f.get("indications", ""),
                "usage": f.get("usage", ""),
                "source_pdf": source_doc,
                "chunk_id": chunk_id
            })
            
            for ing in f.get("ingredients", []):
                h_name = ing.get("herb_name", "").strip()
                if not h_name:
                    continue
                extracted_components.append({
                    "formula_name_vi": f_name,
                    "herb_name": h_name,
                    "dosage_value": ing.get("dosage_value", ""),
                    "dosage_unit": ing.get("dosage_unit", "")
                })

        for h in indep_herbs:
            h_name = h.get("herb_name", "").strip()
            if not h_name:
                continue
            extracted_herbs.append({
                "herb_name": h_name,
                "temp_property": h.get("temp_property", ""),
                "meridian": h.get("meridian", ""),
                "indications": h.get("indications", ""),
                "source_pdf": source_doc
            })

        context.log.info(f"   Batch {llm_calls}: Tích lũy được {len(extracted_formulas)} Bài thuốc, {len(extracted_herbs)} Vị thuốc độc lập.")
        
        if i + _BATCH_SIZE < len(trigger_chunks):
            time.sleep(_SLEEP_SEC)

    # 3. Chuyển đổi thành Polars DataFrame
    df_f = pl.DataFrame(extracted_formulas) if extracted_formulas else pl.DataFrame({"formula_name_vi": [], "function": [], "indications": [], "usage": [], "source_pdf": [], "chunk_id": []})
    df_c = pl.DataFrame(extracted_components) if extracted_components else pl.DataFrame({"formula_name_vi": [], "herb_name": [], "dosage_value": [], "dosage_unit": []})
    df_h = pl.DataFrame(extracted_herbs) if extracted_herbs else pl.DataFrame({"herb_name": [], "temp_property": [], "meridian": [], "indications": [], "source_pdf": []})

    # 4. Ghi trực tiếp ra file CSV vật lý
    path_f = os.path.join(out_dir, "pdf_extracted_formulas.csv")
    path_c = os.path.join(out_dir, "pdf_extracted_formula_components.csv")
    path_h = os.path.join(out_dir, "pdf_extracted_herbs.csv")

    df_f.write_csv(path_f)
    df_c.write_csv(path_c)
    df_h.write_csv(path_h)

    # Thêm UTF-8 BOM để Excel hiển thị đúng tiếng Việt
    for path in [path_f, path_c, path_h]:
        if os.path.exists(path):
            with open(path, "rb") as f:
                content = f.read()
            if not content.startswith(b"\xef\xbb\xbf"):
                with open(path, "wb") as f:
                    f.write(b"\xef\xbb\xbf" + content)

    context.log.info(f"✅ Đã ghi thành công 3 file CSV vào thư mục: {out_dir}")

    # Output ra Dagster bảng tóm tắt
    summary_df = pl.DataFrame({
        "dataset_name": ["Formulas", "Components", "Independent Herbs"],
        "records_extracted": [df_f.shape[0], df_c.shape[0], df_h.shape[0]],
        "file_path": [path_f, path_c, path_h]
    })

    return Output(
        value=summary_df,
        metadata={
            "formulas_count": MetadataValue.int(df_f.shape[0]),
            "components_count": MetadataValue.int(df_c.shape[0]),
            "herbs_count": MetadataValue.int(df_h.shape[0]),
            "llm_api_calls": MetadataValue.int(llm_calls),
            "output_directory": MetadataValue.text(out_dir),
            "preview": MetadataValue.md(summary_df.to_pandas().to_markdown()),
        },
    )
