import boto3
import time


def lambda_handler(event, context):
    athena_client = boto3.client('athena')
    query = "MSCK REPAIR TABLE idata.sales"
    response = athena_client.start_query_execution(QueryString=query, ResultConfiguration={
        'OutputLocation': 's3://athena-query-results-987145268139/Unsaved/'})
    waiter = athena_client.get_query_execution(QueryExecutionId=response["QueryExecutionId"])
    while waiter['QueryExecution']['Status']['State'] not in ('SUCCEEDED', 'FAILED'):
        waiter = athena_client.get_query_execution(QueryExecutionId=response["QueryExecutionId"])
        time.sleep(3)

    event["table_update"] = waiter['QueryExecution']['Status']['State']
    return event