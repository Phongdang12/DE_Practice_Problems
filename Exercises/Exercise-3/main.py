import boto3
from botocore import UNSIGNED
from botocore.config import Config

# Với Credentials	                 Với UNSIGNED
# ✅ Truy cập private buckets	❌ Chỉ truy cập public buckets
# ✅ Đầy đủ permissions	        ✅ Chỉ read public data
# ❌ Cần setup AWS credentials	✅ Không cần credentials
# ❌ Phức tạp hơn	            ✅ Đơn giản hơn

#.gz là định dạng nén nhưng khác với .zip
#gz dùng nén một file đơn lẻ(WinRAR), trong khi .zip có thể nén nhiều file và thư mục.

def main():
    # your code 
    s3=boto3.client(
        's3',
        region_name='us-east-1',
        config=Config(signature_version=UNSIGNED)
    )
    response=s3.get_object(Bucket='commoncrawl',Key='crawl-data/CC-MAIN-2022-05/wet.paths.gz')
    gz_content=response['Body'].read()
    with open('wet.paths.gz', 'wb') as f:
        f.write(gz_content)
    
    pass


if __name__ == "__main__":
    main()
