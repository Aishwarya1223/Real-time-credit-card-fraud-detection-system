import json
import boto3
import csv

kinesis_client = boto3.client('kinesis')
kinesis_stream_name = 'credit-card-transaction-stream'
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        print("Event Received:", json.dumps(event))
        
        detail = event.get('detail', {})
        
        if 'requestParameters' not in detail:
            print("Error: requestParameters missing in event detail")
            return {
                "statusCode": 400,
                "body": "Error: requestParameters missing in event detail"
            }

        request_parameters = detail['requestParameters']
        bucket_name = request_parameters.get('bucketName', '')
        key = request_parameters.get('key', '')

        print(f"Bucket Name: {bucket_name}")
        print(f"Key: {key}")

        if not bucket_name or not key:
            print(f"Missing data in payload: {{'bucket': '{bucket_name}', 'key': '{key}'}}")
            return {
                "statusCode": 400,
                "body": f"Error: Missing bucket or key in payload"
            }

        if key.startswith('transaction_data_folder/') and key.endswith('.csv'):
            # Retrieve the CSV file from S3
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            file_content = response['Body'].read().decode('utf-8').splitlines()
            csv_reader = csv.DictReader(file_content)

            for row in csv_reader:
                payload = {
                    'trans_num': row.get('trans_num'),
                    'trans_date_trans_time': row.get('trans_date_trans_time'),
                    'merchant': row.get('merchant'),
                    'category': row.get('category'),
                    'amt': float(row.get('amt', 0)),
                    'city': row.get('city'),
                    'state': row.get('state'),
                    'lat': float(row.get('lat', 0)),
                    'long': float(row.get('long', 0)),
                    'city_pop': int(row.get('city_pop', 0)),
                    'merch_lat': float(row.get('merch_lat', 0)),
                    'merch_long': float(row.get('merch_long', 0)),
                }

                # Send data to Kinesis
                partition_key = str(row.get('trans_num', 'default_key'))

                response = kinesis_client.put_record(
                    StreamName=kinesis_stream_name,
                    Data=json.dumps(payload),
                    PartitionKey=partition_key
                )

                # Debug log for success
                print(f"Sent to Kinesis: {json.dumps(payload)}")

            return {
                "statusCode": 200,
                "body": f"CSV file '{key}' processed and sent to Kinesis."
            }
        else:
            print(f"Ignored file: {key}")
            return {
                "statusCode": 200,
                "body": f"File '{key}' is not a CSV, skipped."
            }

    except Exception as e:
        print(f"Error: {e}")
        return {
            "statusCode": 500,
            "body": f"Error processing event: {str(e)}"
        }
