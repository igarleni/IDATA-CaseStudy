import awswrangler as wr
import os

def lambda_handler(event, context):
    csv = wr.s3.read_csv(f"s3://{os.environ['DataBucket']}/{event['raw_zipfile_key']}")
    event['parquet_path'] = "2m Sales Records/"
    csv.columns = csv.columns.str.lower()
    csv.columns = csv.columns.str.replace(" ", "_")
    wr.s3.to_parquet(csv, f"s3://{os.environ['DataBucket']}/{event['parquet_path']}", dataset=True,
                     partition_cols=["country"])
    return event
