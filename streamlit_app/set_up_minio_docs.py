import os
import json
from minio import Minio

def main():
    # MinIO config inside Docker network
    endpoint = "minio:9000"
    access_key = "minio"
    secret_key = "minio123"
    bucket_name = "yhct-docs"
    
    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    
    # 1. Tạo bucket nếu chưa có
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"✅ Đã tạo bucket: {bucket_name}")
    else:
        print(f"ℹ Bucket '{bucket_name}' đã tồn tại.")
        
    # 2. Thiết lập policy public read-only cho bucket
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": ["*"]},
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket_name}/*"]
            }
        ]
    }
    client.set_bucket_policy(bucket_name, json.dumps(policy))
    print(f"✅ Đã thiết lập quyền Public Read-Only cho bucket: {bucket_name}")
    
    # 3. Quét thư mục chứa PDF gốc trong container và upload lên MinIO
    raw_dir = "/app/data/raw"
    if not os.path.exists(raw_dir):
        print(f"❌ Thư mục {raw_dir} không tồn tại trong container.")
        return
        
    pdf_files = [f for f in os.listdir(raw_dir) if f.lower().endswith(".pdf")]
    print(f"📂 Tìm thấy {len(pdf_files)} file PDF trong thư mục raw.")
    
    for f in pdf_files:
        file_path = os.path.join(raw_dir, f)
        # Kiểm tra xem file đã tồn tại trên MinIO chưa
        try:
            client.stat_object(bucket_name, f)
            print(f"  ⏭  Bỏ qua: {f} (đã tồn tại trên MinIO)")
        except Exception:
            print(f"  📤 Đang upload: {f} ...")
            try:
                client.fput_object(bucket_name, f, file_path, content_type="application/pdf")
                print(f"  ✅ Upload thành công: {f}")
            except Exception as upload_err:
                print(f"  ❌ Lỗi khi upload {f}: {upload_err}")

if __name__ == "__main__":
    main()
