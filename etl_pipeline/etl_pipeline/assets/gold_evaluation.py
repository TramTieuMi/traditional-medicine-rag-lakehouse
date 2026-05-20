# etl_pipeline/etl_pipeline/assets/gold_evaluation.py
"""
Dagster asset: tự động chạy evaluation sau khi gold_embeddings hoàn thành.
Kết quả được log vào MLflow và lưu artifact vào MinIO.
"""

import os
import sys
import time
import tempfile
from datetime import datetime

import polars as pl
import mlflow
from dagster import asset, Output, MetadataValue, AssetIn

# Thêm evaluation folder vào path
sys.path.insert(0, "/opt/dagster/app/evaluation")
try:
    from metrics import compute_all_metrics
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False

# ── Config ────────────────────────────────────────────────────────────────────
CHROMA_HOST   = os.getenv("CHROMA_HOST",   "chromadb")
CHROMA_PORT   = int(os.getenv("CHROMA_PORT", "8000"))
GROQ_API_KEY  = os.getenv("GROQ_API_KEY",  "")
MLFLOW_URI    = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
EMBED_MODEL   = "keepitreal/vietnamese-sbert"
GROQ_MODEL    = "llama-3.1-8b-instant"
COLLECTION    = "yhct_chunks"
TOP_K         = 5
SIM_THRESHOLD = 0.25

# 22 câu hỏi test — 3 categories
TEST_QUESTIONS = [
    {"id":"TH001","category":"tieu_hoa",  "question":"Bài thuốc nào chữa đau dạ dày theo y học cổ truyền?",      "expected_keywords":["dạ dày","cam thảo","bạch truật","bán hạ"]},
    {"id":"TH002","category":"tieu_hoa",  "question":"Điều trị táo bón bằng thảo dược như thế nào?",              "expected_keywords":["táo bón","đại hoàng","nhuận tràng"]},
    {"id":"TH003","category":"tieu_hoa",  "question":"Phương pháp chữa tiêu chảy trong y học cổ truyền?",         "expected_keywords":["tiêu chảy","tỳ","kiện tỳ","phục linh"]},
    {"id":"TH004","category":"tieu_hoa",  "question":"Bài thuốc điều trị đầy bụng chướng bụng?",                  "expected_keywords":["đầy bụng","hậu phác","mộc hương","tiêu thực"]},
    {"id":"TH005","category":"tieu_hoa",  "question":"Chữa buồn nôn nôn mửa bằng thuốc đông y?",                  "expected_keywords":["nôn","bán hạ","sinh khương","hòa vị"]},
    {"id":"TH006","category":"tieu_hoa",  "question":"Điều trị viêm đại tràng mạn theo YHCT?",                    "expected_keywords":["đại tràng","tỳ hư","kiện tỳ"]},
    {"id":"TH007","category":"tieu_hoa",  "question":"Bài thuốc trị ợ chua ợ hơi?",                               "expected_keywords":["ợ chua","vị","hòa vị"]},
    {"id":"TH008","category":"tieu_hoa",  "question":"Điều trị viêm gan theo y học cổ truyền?",                   "expected_keywords":["viêm gan","can","nhân trần","chi tử"]},
    {"id":"DL001","category":"duoc_lieu", "question":"Cam thảo có công dụng và liều dùng như thế nào?",            "expected_keywords":["cam thảo","bổ tỳ","giải độc","liều"]},
    {"id":"DL002","category":"duoc_lieu", "question":"Bạch truật dùng trong những bài thuốc nào?",                 "expected_keywords":["bạch truật","kiện tỳ","táo thấp"]},
    {"id":"DL003","category":"duoc_lieu", "question":"Phục linh có tác dụng gì trong y học cổ truyền?",            "expected_keywords":["phục linh","kiện tỳ","an thần"]},
    {"id":"DL004","category":"duoc_lieu", "question":"Hoàng kỳ có tác dụng bổ khí như thế nào?",                  "expected_keywords":["hoàng kỳ","bổ khí","thăng dương"]},
    {"id":"DL005","category":"duoc_lieu", "question":"Sài hồ có công dụng sơ can giải uất như thế nào?",           "expected_keywords":["sài hồ","can","giải uất","sơ can"]},
    {"id":"DL006","category":"duoc_lieu", "question":"Nhân trần dùng chữa vàng da viêm gan?",                      "expected_keywords":["nhân trần","vàng da","viêm gan"]},
    {"id":"DL007","category":"duoc_lieu", "question":"Sơn tra có tác dụng tiêu thực như thế nào?",                 "expected_keywords":["sơn tra","tiêu thực","tiêu hóa"]},
    {"id":"BT001","category":"bai_thuoc", "question":"Bài thuốc Tứ Quân Tử thang gồm những vị nào?",               "expected_keywords":["tứ quân tử","nhân sâm","bạch truật","phục linh","cam thảo"]},
    {"id":"BT002","category":"bai_thuoc", "question":"Bài thuốc Lục Quân Tử thang điều trị tỳ vị hư?",             "expected_keywords":["lục quân tử","tỳ vị","bán hạ","trần bì"]},
    {"id":"BT003","category":"bai_thuoc", "question":"Bài Bình Vị tán điều trị đầy bụng thấp trở?",                "expected_keywords":["bình vị","thương truật","hậu phác","trần bì"]},
    {"id":"BT004","category":"bai_thuoc", "question":"Bài Tiêu Dao tán dùng điều trị can khí uất kết?",             "expected_keywords":["tiêu dao","can","uất","sài hồ","bạch thược"]},
    {"id":"BT005","category":"bai_thuoc", "question":"Bài thuốc Bổ Trung Ích Khí thang có công dụng gì?",           "expected_keywords":["bổ trung ích khí","hoàng kỳ","nhân sâm","đương quy"]},
    {"id":"BT006","category":"bai_thuoc", "question":"Sâm Linh Bạch Truật tán dùng chữa bệnh gì?",                 "expected_keywords":["sâm linh bạch truật","tỳ hư","tiêu chảy","ý dĩ"]},
    {"id":"BT007","category":"bai_thuoc", "question":"Bài Tiêu Dao tán kết hợp với Tứ Vật thang điều trị gì?",     "expected_keywords":["tiêu dao","tứ vật","can huyết hư","bạch thược"]},
]


def _run_rag_single(question: str, model, col, groq_client) -> dict:
    t0 = time.perf_counter()
    q_vec   = model.encode([question])[0].tolist()
    results = col.query(
        query_embeddings=[q_vec],
        n_results=TOP_K,
        include=["documents", "distances", "metadatas"]
    )
    chunks    = results["documents"][0]
    ids       = results["ids"][0]
    dists     = results["distances"][0]
    metadatas = results["metadatas"][0]
    sims      = [round(1 - d, 4) for d in dists]
    top_sim   = max(sims) if sims else 0.0

    if not chunks or top_sim < SIM_THRESHOLD:
        answer = "Xin lỗi, không tìm thấy thông tin liên quan."
    else:
        context = "\n\n---\n\n".join(chunks)
        prompt  = (
            "Bạn là chuyên gia Y học cổ truyền Việt Nam.\n"
            f"Dựa vào tài liệu:\n{context}\n\n"
            f"Trả lời: {question}\n"
            "Yêu cầu: tiếng Việt, trích dẫn dược liệu và liều dùng nếu có."
        )
        resp   = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3, max_tokens=512,
        )
        answer = resp.choices[0].message.content

    return {
        "chunks": chunks, "ids": ids, "sims": sims,
        "metadatas": metadatas, "top_sim": top_sim,
        "answer": answer, "elapsed": int((time.perf_counter()-t0)*1000),
    }


@asset(
    name="gold_evaluation",
    key_prefix=["gold", "evaluation"],
    group_name="gold",
    io_manager_key="minio_io_manager",
    compute_kind="python",
    ins={
        "gold_embeddings": AssetIn(key_prefix=["gold", "embeddings"])
    },
    description="Auto-evaluate RAG pipeline — 12 metrics, 3 categories → MLflow + MinIO"
)
def gold_evaluation(context, gold_embeddings) -> Output:

    if not METRICS_AVAILABLE:
        context.log.warning("⚠️  metrics.py không tìm thấy tại /opt/dagster/app/evaluation/")
        context.log.warning("    Copy evaluation/ vào container hoặc mount volume")
        return Output(
            value=pl.DataFrame(),
            metadata={"status": MetadataValue.text("skipped — metrics.py not found")}
        )

    # ── Load dependencies một lần ─────────────────────────────────────────
    context.log.info(f"🤖 Loading embedding model: {EMBED_MODEL}")
    from sentence_transformers import SentenceTransformer
    import chromadb
    from groq import Groq

    model = SentenceTransformer(EMBED_MODEL)
    col   = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT).get_collection(COLLECTION)
    groq  = Groq(api_key=GROQ_API_KEY)
    context.log.info(f"✅ ChromaDB ready — {col.count()} vectors in collection")

    # ── MLflow ────────────────────────────────────────────────────────────
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("YHCT_RAG_Evaluation")
    run_name = f"auto_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    # ── Chạy evaluation ───────────────────────────────────────────────────
    rows = []
    context.log.info(f"🧪 Evaluating {len(TEST_QUESTIONS)} questions...")

    for i, q in enumerate(TEST_QUESTIONS):
        context.log.info(f"  [{i+1:02d}/{len(TEST_QUESTIONS)}] {q['id']} — {q['question'][:55]}")
        try:
            rag = _run_rag_single(q["question"], model, col, groq)
        except Exception as e:
            context.log.error(f"  ❌ RAG error: {e}")
            continue

        m = compute_all_metrics(
            question          = q["question"],
            category          = q["category"],
            expected_keywords = q["expected_keywords"],
            expected_tang_phu = q.get("expected_tang_phu", "ty_vi"),
            chunks            = rag["chunks"],
            chunk_ids         = rag["ids"],
            sims              = rag["sims"],
            metadatas         = rag["metadatas"],
            answer            = rag["answer"],
            elapsed_ms        = rag["elapsed"],
            top_sim           = rag["top_sim"],
            top_k             = TOP_K,
            sim_threshold     = SIM_THRESHOLD,
        )
        rows.append({
            "id":             q["id"],
            "category":       q["category"],
            "question":       q["question"],
            "answer_preview": rag["answer"][:250],
            **m,
        })
        context.log.info(
            f"     sim={m['top_similarity']:.3f} | "
            f"prec={m['precision_at_k']:.3f} | "
            f"faith={m['faithfulness']:.3f} | "
            f"{m['confusion_label']}"
        )
        time.sleep(0.5)  # Groq rate limit

    df = pl.DataFrame(rows)

    # ── Aggregate ─────────────────────────────────────────────────────────
    num_cols = [
        "top_similarity","precision_at_k","mrr","ndcg_at_k",
        "faithfulness","answer_relevancy","rouge_l","completeness",
        "chunk_coverage","is_zero_result","response_ms"
    ]
    df_agg = df.group_by("category").agg(
        [pl.mean(c).alias(c) for c in num_cols]
    )

    n = len(df)
    global_metrics = {
        "avg_top_similarity":    float(df["top_similarity"].mean()),
        "avg_precision_at_k":    float(df["precision_at_k"].mean()),
        "avg_mrr":               float(df["mrr"].mean()),
        "avg_ndcg_at_k":         float(df["ndcg_at_k"].mean()),
        "avg_faithfulness":      float(df["faithfulness"].mean()),
        "avg_answer_relevancy":  float(df["answer_relevancy"].mean()),
        "avg_rouge_l":           float(df["rouge_l"].mean()),
        "avg_completeness":      float(df["completeness"].mean()),
        "zero_result_rate":      float(df["is_zero_result"].mean()),
        "avg_response_ms":       float(df["response_ms"].mean()),
        "correct_rate":          float((df["confusion_label"] == "correct").sum() / n),
        "partial_rate":          float((df["confusion_label"] == "partial").sum() / n),
        "wrong_rate":            float((df["confusion_label"] == "wrong").sum()   / n),
    }

    # ── Log vào MLflow ────────────────────────────────────────────────────
    run_id = "local"
    try:
        with mlflow.start_run(run_name=run_name):
            mlflow.log_params({
                "embed_model":   EMBED_MODEL,
                "llm_model":     GROQ_MODEL,
                "top_k":         TOP_K,
                "sim_threshold": SIM_THRESHOLD,
                "n_questions":   n,
            })
            mlflow.log_metrics(global_metrics)

            # Per-category
            for row in df_agg.iter_rows(named=True):
                cat = row["category"]
                mlflow.log_metrics({
                    f"{cat}_precision_at_k":   float(row["precision_at_k"]),
                    f"{cat}_mrr":              float(row["mrr"]),
                    f"{cat}_faithfulness":     float(row["faithfulness"]),
                    f"{cat}_answer_relevancy": float(row["answer_relevancy"]),
                    f"{cat}_rouge_l":          float(row["rouge_l"]),
                    f"{cat}_top_similarity":   float(row["top_similarity"]),
                })

            # CSV artifact
            with tempfile.NamedTemporaryFile(
                suffix=".csv", delete=False, mode="w", encoding="utf-8"
            ) as f:
                df.to_pandas().to_csv(f, index=False)
                tmpcsv = f.name
            mlflow.log_artifact(tmpcsv, artifact_path="eval_data")
            run_id = mlflow.active_run().info.run_id

        context.log.info(f"✅ MLflow logged — Run ID: {run_id}")
    except Exception as e:
        context.log.warning(f"⚠️  MLflow error (non-fatal): {e}")

    # ── Summary log ───────────────────────────────────────────────────────
    context.log.info(f"\n{'='*55}")
    context.log.info("📊 EVALUATION SUMMARY")
    context.log.info(f"{'='*55}")
    context.log.info(f"  Questions evaluated : {n}")
    context.log.info(f"  Avg Precision@5     : {global_metrics['avg_precision_at_k']:.4f}")
    context.log.info(f"  Avg MRR             : {global_metrics['avg_mrr']:.4f}")
    context.log.info(f"  Avg nDCG@5          : {global_metrics['avg_ndcg_at_k']:.4f}")
    context.log.info(f"  Avg Faithfulness    : {global_metrics['avg_faithfulness']:.4f}")
    context.log.info(f"  Avg ROUGE-L         : {global_metrics['avg_rouge_l']:.4f}")
    context.log.info(f"  Zero-result rate    : {global_metrics['zero_result_rate']*100:.1f}%")
    context.log.info(f"  Correct rate        : {global_metrics['correct_rate']*100:.1f}%")
    context.log.info(f"  Avg response time   : {global_metrics['avg_response_ms']:.0f} ms")
    context.log.info(f"  MLflow UI           : {MLFLOW_URI}")
    context.log.info(f"{'='*55}")

    # Preview table
    preview = df.select([
        "id", "category", "top_similarity", "precision_at_k",
        "faithfulness", "rouge_l", "confusion_label"
    ]).head(10)

    return Output(
        value=df,
        metadata={
            "total_questions":      MetadataValue.int(n),
            "avg_precision_at_k":   MetadataValue.float(global_metrics["avg_precision_at_k"]),
            "avg_faithfulness":     MetadataValue.float(global_metrics["avg_faithfulness"]),
            "avg_rouge_l":          MetadataValue.float(global_metrics["avg_rouge_l"]),
            "zero_result_rate_pct": MetadataValue.float(global_metrics["zero_result_rate"] * 100),
            "correct_rate_pct":     MetadataValue.float(global_metrics["correct_rate"] * 100),
            "avg_response_ms":      MetadataValue.float(global_metrics["avg_response_ms"]),
            "mlflow_run_id":        MetadataValue.text(run_id),
            "mlflow_ui":            MetadataValue.url(MLFLOW_URI),
            "preview":              MetadataValue.md(preview.to_pandas().to_markdown()),
        }
    )