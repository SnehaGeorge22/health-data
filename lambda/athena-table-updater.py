# Lambda: athena-table-updater
import boto3

athena_client = boto3.client('athena')

def lambda_handler(event, context):
    database = event['database']
    tables = event['tables']
    
    results = []
    
    for table in tables:
        # Run MSCK REPAIR to add partitions
        query = f"MSCK REPAIR TABLE {database}.{table}"
        
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={
                'OutputLocation': 's3://healthcare-pipeline-athena-results-sg/'
            }
        )
        
        results.append({
            'table': table,
            'query_id': response['QueryExecutionId']
        })
    
    return {
        'statusCode': 200,
        'results': results
    }