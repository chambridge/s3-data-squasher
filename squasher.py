import os
import boto3
import pandas as pd

s3 = boto3.resource('s3')

bucket_name = os.getenv("S3_BUCKET")
prefix = os.getenv("DOWNLOAD_PREFIX")
output_file = os.getenv("OUTPUT_FILE")
bucket = s3.Bucket(bucket_name)

files = []
print(f"prefix={prefix}")
for bucket_object in bucket.objects.filter(Prefix=prefix):
    print(f"key={bucket_object.key}")
    os.makedirs(os.path.dirname(bucket_object.key), exist_ok=True)
    bucket.download_file(bucket_object.key, bucket_object.key)
    files.append(bucket_object.key)

print(f"files={files}")
df = pd.concat(map(pd.read_csv, files), ignore_index=True)
os.makedirs("output", exist_ok=True)
df.to_csv(f"output/{output_file}", index=False)
          