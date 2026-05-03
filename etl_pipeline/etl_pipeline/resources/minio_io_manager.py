# etl_pipeline/etl_pipeline/resources/minio_io_manager.py

from dagster import IOManager, OutputContext, InputContext
from minio import Minio
import polars as pl
import uuid
import os


class MinIOIOManager(IOManager):
    """
    IO Manager tự viết: tự động lưu/đọc Polars DataFrame
    dưới dạng Parquet vào MinIO.
    
    Asset key ["bronze", "pdf", "bronze_pdf_pages"]
    → MinIO path: yhct-bronze/bronze/pdf/bronze_pdf_pages.parquet
    """

    def __init__(self, config: dict):
        self.config = config

        # Xử lý endpoint: bỏ http:// nếu có
        endpoint = config["endpoint_url"]
        for prefix in ["http://", "https://"]:
            if endpoint.startswith(prefix):
                endpoint = endpoint[len(prefix):]

        self.client = Minio(
            endpoint=endpoint,
            access_key=config["access_key"],
            secret_key=config["secret_key"],
            secure=False
        )

        # Tạo bucket nếu chưa có
        for bucket in [config["bronze_bucket"], config["silver_bucket"], config["gold_bucket"]]:
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
                print(f"[MinIOIOManager] Đã tạo bucket: {bucket}")

    def _get_bucket_and_key(self, context) -> tuple[str, str]:
        """
        Tự động chọn bucket dựa vào layer đầu tiên của asset key.
        ["bronze", "pdf", "bronze_pdf_pages"] → bucket: yhct-bronze, key: bronze/pdf/bronze_pdf_pages.parquet
        ["silver", "pdf", "silver_filtered_pages"] → bucket: yhct-silver, key: ...
        ["gold", "chunks", "gold_yhct_chunks"] → bucket: yhct-gold, key: ...
        """
        key_path = context.asset_key.path  # list[str]
        layer = key_path[0]  # "bronze" | "silver" | "gold"

        bucket_map = {
            "bronze": self.config["bronze_bucket"],
            "silver": self.config["silver_bucket"],
            "gold":   self.config["gold_bucket"],
        }

        bucket = bucket_map.get(layer, self.config["bronze_bucket"])
        object_key = "/".join(key_path) + ".parquet"

        return bucket, object_key

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        """Lưu Polars DataFrame → Parquet → MinIO"""
        if obj is None or obj.is_empty():
            context.log.warning("DataFrame rỗng, bỏ qua việc lưu vào MinIO.")
            return

        bucket, object_key = self._get_bucket_and_key(context)
        tmp_path = f"/tmp/dagster_out_{uuid.uuid4().hex}.parquet"

        try:
            obj.write_parquet(tmp_path)
            self.client.fput_object(bucket, object_key, tmp_path)
            context.log.info(f"[MinIO] ✅ Đã lưu: s3://{bucket}/{object_key} ({obj.shape[0]} rows)")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def load_input(self, context: InputContext) -> pl.DataFrame:
        """Đọc Parquet từ MinIO → Polars DataFrame"""
        bucket, object_key = self._get_bucket_and_key(context)
        tmp_path = f"/tmp/dagster_in_{uuid.uuid4().hex}.parquet"

        try:
            self.client.fget_object(bucket, object_key, tmp_path)
            df = pl.read_parquet(tmp_path)
            context.log.info(f"[MinIO] ✅ Đã đọc: s3://{bucket}/{object_key} ({df.shape[0]} rows)")
            return df
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)