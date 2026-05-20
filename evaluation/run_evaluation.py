# evaluation/run_evaluation.py
"""
Evaluation pipeline hoàn chỉnh — Mức C:
  1. Load 30 câu hỏi test (3 categories)
  2. Chạy RAG pipeline cho từng câu
  3. Tính 12 metrics
  4. Log MLflow (params, metrics, charts, artifacts)
  5. Export CSV + JSON chi tiết
  6. Export báo cáo PDF tự động

Chạy: python evaluation/run_evaluation.py
      hoặc: python evaluation/run_evaluation.py --experiment "chunk500_topk10"
"""

import os
import sys
import json
import time
import argparse
import logging
from datetime import datetime
from pathlib import Path

import chromadb
import mlflow
import mlflow.sklearn
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from sentence_transformers import SentenceTransformer
from groq import Groq

# Thêm root vào sys.path để import được metrics
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
from metrics import compute_all_metrics

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ── Config từ environment (giống rag.py) ─────────────────────────────────────
CHROMA_HOST   = os.getenv("CHROMA_HOST",   "chromadb")
CHROMA_PORT   = int(os.getenv("CHROMA_PORT", "8000"))
GROQ_API_KEY  = os.getenv("GROQ_API_KEY",  "")
MLFLOW_URI    = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
EMBED_MODEL   = "keepitreal/vietnamese-sbert"
GROQ_MODEL    = "llama-3.1-8b-instant"
COLLECTION    = "yhct_chunks"
TOP_K         = 5
SIM_THRESHOLD = 0.25

COLORS = {
    "tieu_hoa":  "#2d9e5f",
    "duoc_lieu": "#1a6b9e",
    "bai_thuoc": "#9e6b1a",
}
CATEGORY_LABELS = {
    "tieu_hoa":  "Bệnh tiêu hóa",
    "duoc_lieu": "Dược liệu",
    "bai_thuoc": "Bài thuốc",
}


# ─────────────────────────────────────────────────────────────────────────────
# RAG query — tái sử dụng logic từ rag.py
# ─────────────────────────────────────────────────────────────────────────────

_embed_model = None
_chroma_col  = None
_groq_client = None

def get_embed_model():
    global _embed_model
    if _embed_model is None:
        log.info(f"Loading embedding model: {EMBED_MODEL}")
        _embed_model = SentenceTransformer(EMBED_MODEL)
    return _embed_model

def get_chroma():
    global _chroma_col
    if _chroma_col is None:
        client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
        _chroma_col = client.get_or_create_collection(
            name=COLLECTION,
            metadata={"hnsw:space": "cosine"}
        )
        log.info(f"ChromaDB connected — {_chroma_col.count()} vectors")
    return _chroma_col

def get_groq():
    global _groq_client
    if _groq_client is None:
        _groq_client = Groq(api_key=GROQ_API_KEY)
    return _groq_client


def run_rag(question: str) -> dict:
    """Chạy RAG pipeline, trả về dict đầy đủ kèm chunks, ids, sims, metadatas."""
    t0 = time.perf_counter()

    model   = get_embed_model()
    q_vec   = model.encode([question])[0].tolist()

    col     = get_chroma()
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

    # Kiểm tra ngưỡng
    if len(chunks) == 0 or top_sim < SIM_THRESHOLD:
        answer = (
            "Xin lỗi, tôi không tìm thấy thông tin liên quan "
            "trong tài liệu Y học cổ truyền hiện có."
        )
    else:
        context = "\n\n---\n\n".join(chunks)
        prompt  = f"""Bạn là chuyên gia Y học cổ truyền Việt Nam.
Dựa vào các đoạn tài liệu sau đây:

{context}

Hãy trả lời câu hỏi: {question}

Yêu cầu:
- Trả lời chi tiết, rõ ràng bằng tiếng Việt
- Trích dẫn tên bài thuốc, dược liệu cụ thể nếu có
- Nêu liều lượng nếu tài liệu đề cập
- Nếu thông tin không đủ, hãy nói rõ"""

        resp   = get_groq().chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=1024,
        )
        answer = resp.choices[0].message.content

    elapsed = int((time.perf_counter() - t0) * 1000)
    return {
        "answer":    answer,
        "chunks":    chunks,
        "ids":       ids,
        "sims":      sims,
        "metadatas": metadatas,
        "top_sim":   top_sim,
        "elapsed":   elapsed,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CHART GENERATORS
# ─────────────────────────────────────────────────────────────────────────────

def _apply_style(ax):
    ax.set_facecolor("#f8f9fa")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#dee2e6")
    ax.spines["bottom"].set_color("#dee2e6")
    ax.tick_params(colors="#495057")
    ax.yaxis.label.set_color("#495057")
    ax.xaxis.label.set_color("#495057")
    ax.title.set_color("#212529")


def plot_metrics_radar(df_agg: pd.DataFrame, out_path: str):
    """Radar chart so sánh 5 metrics chính theo 3 categories."""
    metrics = ["precision_at_k", "mrr", "faithfulness", "answer_relevancy", "rouge_l"]
    labels  = ["Precision@5", "MRR", "Faithfulness", "Relevancy", "ROUGE-L"]
    cats    = df_agg["category"].unique()

    angles = np.linspace(0, 2 * np.pi, len(metrics), endpoint=False).tolist()
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(7, 6), subplot_kw=dict(polar=True))
    fig.patch.set_facecolor("white")

    for cat in cats:
        row = df_agg[df_agg["category"] == cat].iloc[0]
        vals = [row[m] for m in metrics] + [row[metrics[0]]]
        ax.plot(angles, vals, linewidth=2, color=COLORS.get(cat, "#666"), label=CATEGORY_LABELS.get(cat, cat))
        ax.fill(angles, vals, alpha=0.15, color=COLORS.get(cat, "#666"))

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels, fontsize=10, color="#495057")
    ax.set_ylim(0, 1)
    ax.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(["0.2","0.4","0.6","0.8","1.0"], fontsize=8, color="#adb5bd")
    ax.grid(color="#dee2e6", linestyle="--", alpha=0.7)
    ax.set_facecolor("#f8f9fa")

    ax.legend(loc="upper right", bbox_to_anchor=(1.3, 1.1), fontsize=9)
    ax.set_title("Metrics theo category", fontsize=13, pad=18, color="#212529", fontweight="bold")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()


def plot_similarity_distribution(df: pd.DataFrame, out_path: str):
    """Histogram phân bố top_similarity theo category."""
    fig, axes = plt.subplots(1, 3, figsize=(13, 4), sharey=True)
    fig.patch.set_facecolor("white")
    fig.suptitle("Phân bố cosine similarity theo category",
                 fontsize=13, color="#212529", fontweight="bold", y=1.02)

    for ax, (cat, label) in zip(axes, CATEGORY_LABELS.items()):
        sub = df[df["category"] == cat]["top_similarity"]
        color = COLORS.get(cat, "#666")
        ax.hist(sub, bins=8, color=color, alpha=0.8, edgecolor="white", linewidth=0.8)
        ax.axvline(sub.mean(), color="#e63946", linestyle="--", linewidth=1.5,
                   label=f"Mean: {sub.mean():.2f}")
        ax.axvline(SIM_THRESHOLD, color="#f4a261", linestyle=":", linewidth=1.5,
                   label=f"Threshold: {SIM_THRESHOLD}")
        ax.set_title(label, fontsize=11, color="#212529")
        ax.set_xlabel("Cosine similarity", fontsize=9)
        ax.set_ylabel("Số câu hỏi", fontsize=9)
        ax.legend(fontsize=8)
        _apply_style(ax)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()


def plot_confusion_matrix(df: pd.DataFrame, out_path: str):
    """Stacked bar: correct / partial / wrong theo category."""
    fig, ax = plt.subplots(figsize=(8, 5))
    fig.patch.set_facecolor("white")

    labels_list = list(CATEGORY_LABELS.keys())
    display     = [CATEGORY_LABELS[c] for c in labels_list]

    correct = [len(df[(df["category"]==c) & (df["confusion_label"]=="correct")]) for c in labels_list]
    partial = [len(df[(df["category"]==c) & (df["confusion_label"]=="partial")])  for c in labels_list]
    wrong   = [len(df[(df["category"]==c) & (df["confusion_label"]=="wrong")])    for c in labels_list]

    x = np.arange(len(labels_list))
    w = 0.5

    b1 = ax.bar(x, correct, w, label="Correct",  color="#2d9e5f", alpha=0.85)
    b2 = ax.bar(x, partial, w, bottom=correct, label="Partial", color="#f4a261", alpha=0.85)
    b3 = ax.bar(x, wrong,   w, bottom=[c+p for c,p in zip(correct,partial)],
                label="Wrong", color="#e63946", alpha=0.85)

    ax.set_xticks(x)
    ax.set_xticklabels(display, fontsize=11)
    ax.set_ylabel("Số câu hỏi", fontsize=10)
    ax.set_title("Phân loại kết quả theo category", fontsize=13, fontweight="bold", color="#212529")
    ax.legend(fontsize=9)

    # Ghi số lên bar
    for bar in [b1, b2, b3]:
        for rect in bar:
            h = rect.get_height()
            if h > 0:
                ax.text(
                    rect.get_x() + rect.get_width() / 2,
                    rect.get_y() + h / 2,
                    str(int(h)), ha="center", va="center",
                    fontsize=10, color="white", fontweight="bold"
                )

    _apply_style(ax)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()


def plot_metrics_heatmap(df_agg: pd.DataFrame, out_path: str):
    """Heatmap tổng hợp tất cả metrics theo category."""
    metric_cols = [
        "top_similarity", "precision_at_k", "mrr", "ndcg_at_k",
        "faithfulness", "answer_relevancy", "rouge_l", "completeness",
        "chunk_coverage",
    ]
    metric_labels = [
        "Top similarity", "Precision@5", "MRR", "nDCG@5",
        "Faithfulness", "Ans. relevancy", "ROUGE-L", "Completeness",
        "Chunk coverage",
    ]

    cats    = [CATEGORY_LABELS.get(c, c) for c in df_agg["category"].tolist()]
    matrix  = df_agg[metric_cols].values

    fig, ax = plt.subplots(figsize=(11, 4))
    fig.patch.set_facecolor("white")

    im = ax.imshow(matrix, cmap="RdYlGn", vmin=0, vmax=1, aspect="auto")
    plt.colorbar(im, ax=ax, shrink=0.8)

    ax.set_xticks(range(len(metric_labels)))
    ax.set_xticklabels(metric_labels, rotation=35, ha="right", fontsize=9)
    ax.set_yticks(range(len(cats)))
    ax.set_yticklabels(cats, fontsize=10)
    ax.set_title("Tổng hợp metrics theo category (mean)", fontsize=12, fontweight="bold", color="#212529")

    for i in range(len(cats)):
        for j in range(len(metric_labels)):
            val = matrix[i, j]
            color = "white" if val < 0.4 or val > 0.75 else "black"
            ax.text(j, i, f"{val:.2f}", ha="center", va="center",
                    fontsize=9, color=color, fontweight="bold")

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()


def plot_response_time(df: pd.DataFrame, out_path: str):
    """Box plot thời gian phản hồi theo category."""
    fig, ax = plt.subplots(figsize=(7, 4))
    fig.patch.set_facecolor("white")

    data  = [df[df["category"]==c]["response_ms"].values for c in CATEGORY_LABELS]
    bp = ax.boxplot(data, patch_artist=True, notch=False,
                    medianprops=dict(color="white", linewidth=2))

    for patch, cat in zip(bp["boxes"], CATEGORY_LABELS):
        patch.set_facecolor(COLORS[cat])
        patch.set_alpha(0.8)

    ax.set_xticklabels([CATEGORY_LABELS[c] for c in CATEGORY_LABELS], fontsize=10)
    ax.set_ylabel("Response time (ms)", fontsize=10)
    ax.set_title("Phân bố thời gian phản hồi theo category", fontsize=12,
                 fontweight="bold", color="#212529")
    _apply_style(ax)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()


# ─────────────────────────────────────────────────────────────────────────────
# PDF REPORT
# ─────────────────────────────────────────────────────────────────────────────

def export_pdf_report(
    df: pd.DataFrame,
    df_agg: pd.DataFrame,
    chart_paths: dict,
    params: dict,
    out_path: str
):
    """Tạo báo cáo PDF đầy đủ bằng matplotlib."""
    from matplotlib.backends.backend_pdf import PdfPages
    from matplotlib.gridspec import GridSpec

    GREEN  = "#2d9e5f"
    DARK   = "#212529"
    GRAY   = "#6c757d"
    LIGHT  = "#f8f9fa"

    metric_cols = [
        "top_similarity","precision_at_k","mrr","ndcg_at_k",
        "faithfulness","answer_relevancy","rouge_l","completeness"
    ]

    with PdfPages(out_path) as pdf:

        # ── Page 1: Cover ─────────────────────────────────────────────────
        fig = plt.figure(figsize=(8.27, 11.69))
        fig.patch.set_facecolor("white")
        ax  = fig.add_axes([0, 0, 1, 1])
        ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.axis("off")

        ax.add_patch(mpatches.FancyBboxPatch((0,0), 1, 1, fc=LIGHT, ec="none"))
        ax.add_patch(mpatches.FancyBboxPatch((0, 0.82), 1, 0.18, fc=GREEN, ec="none"))
        ax.text(0.5, 0.90, "🌿 YHCT RAG System", ha="center", va="center",
                fontsize=22, color="white", fontweight="bold")
        ax.text(0.5, 0.85, "Báo cáo đánh giá mô hình", ha="center", va="center",
                fontsize=14, color="white")

        # KPIs
        kpis = [
            ("Tổng câu hỏi",        f"{len(df)}"),
            ("Avg Precision@5",      f"{df['precision_at_k'].mean():.3f}"),
            ("Avg Faithfulness",     f"{df['faithfulness'].mean():.3f}"),
            ("Zero-result rate",     f"{df['is_zero_result'].mean()*100:.1f}%"),
            ("Avg response",         f"{df['response_ms'].mean():.0f} ms"),
            ("Correct answers",      f"{(df['confusion_label']=='correct').sum()}/{len(df)}"),
        ]
        for i, (label, val) in enumerate(kpis):
            row, col = divmod(i, 3)
            xc = 0.18 + col * 0.32
            yc = 0.66 - row * 0.12
            ax.add_patch(mpatches.FancyBboxPatch(
                (xc-0.13, yc-0.045), 0.26, 0.09,
                boxstyle="round,pad=0.01", fc="white",
                ec=GREEN, lw=1.5, zorder=2
            ))
            ax.text(xc, yc+0.018, val, ha="center", va="center",
                    fontsize=18, color=GREEN, fontweight="bold", zorder=3)
            ax.text(xc, yc-0.018, label, ha="center", va="center",
                    fontsize=8, color=GRAY, zorder=3)

        ax.text(0.5, 0.40, "Tham số thực nghiệm", ha="center", va="center",
                fontsize=12, color=DARK, fontweight="bold")
        param_text = "\n".join([f"  {k}: {v}" for k,v in params.items()])
        ax.text(0.5, 0.30, param_text, ha="center", va="center",
                fontsize=9, color=GRAY, linespacing=1.8)

        ax.text(0.5, 0.06,
                f"Sinh ngày: {datetime.now().strftime('%d/%m/%Y %H:%M')}",
                ha="center", va="center", fontsize=9, color=GRAY)
        ax.text(0.5, 0.03,
                "Hệ thống Datalake House tích hợp Chatbot YHCT — Đồ án tốt nghiệp",
                ha="center", va="center", fontsize=8, color=GRAY, style="italic")
        pdf.savefig(fig, bbox_inches="tight"); plt.close()

        # ── Page 2-4: Charts ──────────────────────────────────────────────
        chart_pages = [
            ("chart_radar",    "Radar chart — Metrics theo category"),
            ("chart_heatmap",  "Heatmap — Tổng hợp metrics"),
            ("chart_confusion","Confusion matrix theo category"),
            ("chart_hist",     "Phân bố cosine similarity"),
            ("chart_time",     "Thời gian phản hồi"),
        ]
        for key, title in chart_pages:
            if key not in chart_paths or not Path(chart_paths[key]).exists():
                continue
            fig = plt.figure(figsize=(8.27, 6))
            fig.patch.set_facecolor("white")
            ax_t = fig.add_axes([0, 0.9, 1, 0.1])
            ax_t.axis("off")
            ax_t.text(0.5, 0.5, title, ha="center", va="center",
                      fontsize=13, color=DARK, fontweight="bold")
            ax_i = fig.add_axes([0.05, 0.02, 0.9, 0.87])
            ax_i.axis("off")
            img = plt.imread(chart_paths[key])
            ax_i.imshow(img)
            pdf.savefig(fig, bbox_inches="tight"); plt.close()

        # ── Page 5: Aggregated table ──────────────────────────────────────
        fig, ax = plt.subplots(figsize=(8.27, 3))
        fig.patch.set_facecolor("white")
        ax.axis("off")
        ax.set_title("Bảng tổng hợp metrics theo category (mean)",
                     fontsize=12, fontweight="bold", color=DARK, pad=10)

        display_metrics = ["top_similarity","precision_at_k","mrr","faithfulness",
                           "answer_relevancy","rouge_l","completeness"]
        col_labels = ["Category","Top sim","Prec@5","MRR","Faith","Rel","ROUGE-L","Comp"]
        table_data = []
        for _, row in df_agg.iterrows():
            r = [CATEGORY_LABELS.get(row["category"], row["category"])]
            r += [f"{row[m]:.3f}" for m in display_metrics]
            table_data.append(r)

        tbl = ax.table(
            cellText=table_data, colLabels=col_labels,
            loc="center", cellLoc="center"
        )
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(9)
        tbl.scale(1.2, 1.8)
        for (r, c), cell in tbl.get_celld().items():
            if r == 0:
                cell.set_facecolor(GREEN)
                cell.set_text_props(color="white", fontweight="bold")
            elif r % 2 == 0:
                cell.set_facecolor("#f0f7f3")
            cell.set_edgecolor("#dee2e6")

        pdf.savefig(fig, bbox_inches="tight"); plt.close()

        # ── Page 6: Per-question detail ───────────────────────────────────
        chunk_size = 12
        for page_i in range(0, len(df), chunk_size):
            chunk = df.iloc[page_i:page_i+chunk_size]
            fig, ax = plt.subplots(figsize=(8.27, 11.69))
            fig.patch.set_facecolor("white")
            ax.axis("off")

            title = f"Chi tiết từng câu hỏi (trang {page_i//chunk_size + 1})"
            ax.set_title(title, fontsize=11, fontweight="bold",
                         color=DARK, pad=8)

            col_labels = ["ID","Category","Sim","Prec","Faith","Rel","Label","ms"]
            table_data = []
            for _, row in chunk.iterrows():
                table_data.append([
                    row["id"],
                    CATEGORY_LABELS.get(row["category"], row["category"])[:8],
                    f"{row['top_similarity']:.2f}",
                    f"{row['precision_at_k']:.2f}",
                    f"{row['faithfulness']:.2f}",
                    f"{row['answer_relevancy']:.2f}",
                    row["confusion_label"],
                    str(row["response_ms"]),
                ])
            tbl = ax.table(
                cellText=table_data, colLabels=col_labels,
                loc="center", cellLoc="center"
            )
            tbl.auto_set_font_size(False)
            tbl.set_fontsize(8)
            tbl.scale(1.1, 1.6)

            label_colors = {"correct": "#d4edda", "partial": "#fff3cd", "wrong": "#f8d7da"}
            for (r, c), cell in tbl.get_celld().items():
                if r == 0:
                    cell.set_facecolor(GREEN)
                    cell.set_text_props(color="white", fontweight="bold")
                elif r > 0 and c == 6:
                    label_val = table_data[r-1][6]
                    cell.set_facecolor(label_colors.get(label_val, "white"))
                elif r % 2 == 0:
                    cell.set_facecolor(LIGHT)
                cell.set_edgecolor("#dee2e6")

            pdf.savefig(fig, bbox_inches="tight"); plt.close()

    log.info(f"PDF report saved: {out_path}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="YHCT RAG Evaluation — Mức C")
    parser.add_argument("--experiment", default="YHCT_RAG_Evaluation",
                        help="MLflow experiment name")
    parser.add_argument("--run-name",  default=None,
                        help="MLflow run name (default: timestamp)")
    parser.add_argument("--top-k",     type=int, default=TOP_K)
    parser.add_argument("--threshold", type=float, default=SIM_THRESHOLD)
    parser.add_argument("--chunk-size", default="250", help="chunk size label (metadata only)")
    parser.add_argument("--no-pdf",    action="store_true", help="Bỏ qua tạo PDF")
    args = parser.parse_args()

    run_name = args.run_name or datetime.now().strftime("run_%Y%m%d_%H%M%S")

    # ── Load test data ────────────────────────────────────────────────────
    test_path = ROOT / "test_data" / "test_questions.json"
    with open(test_path, encoding="utf-8") as f:
        questions = json.load(f)
    log.info(f"Loaded {len(questions)} test questions")

    # ── Output dirs ───────────────────────────────────────────────────────
    out_dir   = ROOT / "reports" / run_name
    chart_dir = out_dir / "charts"
    out_dir.mkdir(parents=True, exist_ok=True)
    chart_dir.mkdir(parents=True, exist_ok=True)

    # ── MLflow setup ──────────────────────────────────────────────────────
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(args.experiment)

    params = {
        "embed_model":  EMBED_MODEL,
        "llm_model":    GROQ_MODEL,
        "top_k":        args.top_k,
        "sim_threshold":args.threshold,
        "chunk_size":   args.chunk_size,
        "collection":   COLLECTION,
        "n_questions":  len(questions),
    }

    # ── Run evaluation ────────────────────────────────────────────────────
    rows = []
    log.info("Starting evaluation...")

    for i, q in enumerate(questions):
        log.info(f"[{i+1:02d}/{len(questions)}] {q['id']} — {q['question'][:60]}")
        try:
            rag_out = run_rag(q["question"])
        except Exception as e:
            log.error(f"  RAG error: {e}")
            continue

        metrics = compute_all_metrics(
            question         = q["question"],
            category         = q["category"],
            expected_keywords= q["expected_keywords"],
            expected_tang_phu= q["expected_tang_phu"],
            chunks           = rag_out["chunks"],
            chunk_ids        = rag_out["ids"],
            sims             = rag_out["sims"],
            metadatas        = rag_out["metadatas"],
            answer           = rag_out["answer"],
            elapsed_ms       = rag_out["elapsed"],
            top_sim          = rag_out["top_sim"],
            top_k            = args.top_k,
            sim_threshold    = args.threshold,
        )

        row = {
            "id":       q["id"],
            "category": q["category"],
            "question": q["question"],
            "answer":   rag_out["answer"][:300],
            **metrics,
        }
        rows.append(row)
        log.info(
            f"  sim={metrics['top_similarity']:.3f}  "
            f"prec={metrics['precision_at_k']:.3f}  "
            f"faith={metrics['faithfulness']:.3f}  "
            f"label={metrics['confusion_label']}"
        )

        # Rate-limit safety (Groq free tier)
        time.sleep(0.5)

    df = pd.DataFrame(rows)

    # ── Aggregate per category ────────────────────────────────────────────
    agg_cols = [
        "top_similarity","precision_at_k","mrr","ndcg_at_k",
        "chunk_coverage","faithfulness","answer_relevancy",
        "rouge_l","completeness","is_zero_result","response_ms"
    ]
    df_agg = df.groupby("category")[agg_cols].mean().reset_index()

    # ── Generate charts ───────────────────────────────────────────────────
    chart_paths = {
        "chart_radar":    str(chart_dir / "radar.png"),
        "chart_hist":     str(chart_dir / "similarity_dist.png"),
        "chart_confusion":str(chart_dir / "confusion.png"),
        "chart_heatmap":  str(chart_dir / "heatmap.png"),
        "chart_time":     str(chart_dir / "response_time.png"),
    }
    log.info("Generating charts...")
    plot_metrics_radar(df_agg, chart_paths["chart_radar"])
    plot_similarity_distribution(df, chart_paths["chart_hist"])
    plot_confusion_matrix(df, chart_paths["chart_confusion"])
    plot_metrics_heatmap(df_agg, chart_paths["chart_heatmap"])
    plot_response_time(df, chart_paths["chart_time"])

    # ── Export CSV / JSON ─────────────────────────────────────────────────
    csv_path  = str(out_dir / "results_detail.csv")
    json_path = str(out_dir / "results_detail.json")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_json(json_path, orient="records", force_ascii=False, indent=2)
    log.info(f"Exported: {csv_path}")

    # ── PDF report ────────────────────────────────────────────────────────
    pdf_path = str(out_dir / "evaluation_report.pdf")
    if not args.no_pdf:
        log.info("Generating PDF report...")
        export_pdf_report(df, df_agg, chart_paths, params, pdf_path)

    # ── MLflow logging ────────────────────────────────────────────────────
    with mlflow.start_run(run_name=run_name):
        # Params
        mlflow.log_params(params)

        # Global metrics
        global_metrics = {
            "avg_top_similarity":   float(df["top_similarity"].mean()),
            "avg_precision_at_k":   float(df["precision_at_k"].mean()),
            "avg_mrr":              float(df["mrr"].mean()),
            "avg_ndcg_at_k":        float(df["ndcg_at_k"].mean()),
            "avg_faithfulness":     float(df["faithfulness"].mean()),
            "avg_answer_relevancy": float(df["answer_relevancy"].mean()),
            "avg_rouge_l":          float(df["rouge_l"].mean()),
            "avg_completeness":     float(df["completeness"].mean()),
            "avg_chunk_coverage":   float(df["chunk_coverage"].mean()),
            "zero_result_rate":     float(df["is_zero_result"].mean()),
            "avg_response_ms":      float(df["response_ms"].mean()),
            "correct_rate":         float((df["confusion_label"]=="correct").mean()),
            "partial_rate":         float((df["confusion_label"]=="partial").mean()),
            "wrong_rate":           float((df["confusion_label"]=="wrong").mean()),
        }
        mlflow.log_metrics(global_metrics)

        # Per-category metrics
        for _, row in df_agg.iterrows():
            cat = row["category"]
            mlflow.log_metrics({
                f"{cat}_precision_at_k":   float(row["precision_at_k"]),
                f"{cat}_mrr":              float(row["mrr"]),
                f"{cat}_faithfulness":     float(row["faithfulness"]),
                f"{cat}_answer_relevancy": float(row["answer_relevancy"]),
                f"{cat}_rouge_l":          float(row["rouge_l"]),
                f"{cat}_top_similarity":   float(row["top_similarity"]),
                f"{cat}_zero_result_rate": float(row["is_zero_result"]),
            })

        # Log charts
        for key, path in chart_paths.items():
            if Path(path).exists():
                mlflow.log_artifact(path, artifact_path="charts")

        # Log CSV, JSON, PDF
        mlflow.log_artifact(csv_path,  artifact_path="data")
        mlflow.log_artifact(json_path, artifact_path="data")
        if not args.no_pdf and Path(pdf_path).exists():
            mlflow.log_artifact(pdf_path, artifact_path="reports")

        run_id = mlflow.active_run().info.run_id

    # ── Summary ───────────────────────────────────────────────────────────
    log.info("\n" + "="*60)
    log.info("✅ EVALUATION COMPLETE")
    log.info("="*60)
    log.info(f"  Run ID:             {run_id}")
    log.info(f"  Questions:          {len(df)}")
    log.info(f"  Avg Precision@5:    {global_metrics['avg_precision_at_k']:.4f}")
    log.info(f"  Avg Faithfulness:   {global_metrics['avg_faithfulness']:.4f}")
    log.info(f"  Avg ROUGE-L:        {global_metrics['avg_rouge_l']:.4f}")
    log.info(f"  Zero-result rate:   {global_metrics['zero_result_rate']*100:.1f}%")
    log.info(f"  Correct rate:       {global_metrics['correct_rate']*100:.1f}%")
    log.info(f"  Avg response time:  {global_metrics['avg_response_ms']:.0f} ms")
    log.info(f"  MLflow UI:          {MLFLOW_URI}")
    log.info(f"  PDF report:         {pdf_path}")
    log.info("="*60)


if __name__ == "__main__":
    main()