import boto3
import json
import base64
import pickle
import sklearn
import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier,AdaBoostClassifier,HistGradientBoostingClassifier,VotingClassifier
from sklearn.tree import DecisionTreeClassifier
from io import BytesIO
import importlib.util

# AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('fraud-detection-results')
sns_client=boto3.client('sns')
sns_arn='arn:aws:sns:us-east-1:692859936584:fraud-alert-topic'
bucket_name = 'credit-card-fraud-detection-system-bucket'
model = None
preprocessing = None

# Load preprocessing module from S3
def load_preprocessing_module(bucket_name, s3_key):
    try:
        print(f"Loading preprocessing module from S3: {bucket_name}/{s3_key}")
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        with open('/tmp/preprocessing_function.py', 'wb') as f:
            f.write(response['Body'].read())

        spec = importlib.util.spec_from_file_location('preprocessing', '/tmp/preprocessing_function.py')
        preprocessing_module = importlib.util.module_from_spec(spec)

        if not spec or not spec.loader:
            raise ImportError("Failed to load preprocessing module.")
        spec.loader.exec_module(preprocessing_module)
        print("Preprocessing module loaded successfully.")
        return preprocessing_module
    except Exception as e:
        print(f"Error loading preprocessing module: {e}")
        raise

# Load model using pickle from S3
def load_model_from_s3(bucket_name, model_key):
    try:
        print(f"Loading model from S3: {bucket_name}/{model_key}")
        response = s3_client.get_object(Bucket=bucket_name, Key=model_key)
        model_data = BytesIO(response['Body'].read())
        model = joblib.load(model_data)

        if not isinstance(model, (VotingClassifier, RandomForestClassifier, AdaBoostClassifier, HistGradientBoostingClassifier, DecisionTreeClassifier)):
            raise ValueError("Loaded model is not a valid Classifier.")
        print("Model loaded successfully.")
        return model
    except Exception as e:
        print(f"Error loading model: {e}")
        raise

def load_resources():
    global model, preprocessing
    if model is None:
        model = load_model_from_s3(bucket_name, 'voting_classifier_dt_rf_ada.pkl')
    if preprocessing is None:
        preprocessing = load_preprocessing_module(bucket_name, 'preprocessing_functions.py')

# Lambda handler
def lambda_handler(event, context):
    try:
        print("Lambda function invoked.")
        load_resources()

        for record in event.get('Records', []):
            try:
                decoded_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
                payload = json.loads(decoded_data)

                required_keys = ['trans_num','amt', 'lat', 'long', 'trans_date_trans_time', 'city', 'city_pop', 'merch_lat','category', 'merch_long']
                if not all(key in payload for key in required_keys):
                    print(f"Missing data in payload: {payload}")
                    continue

                print(f"Processing payload: {payload}")

                # Preprocess and predict
                features_df = preprocessing.preprocess_data(payload)
                if features_df is None:
                    print("Error: Preprocessing returned None. Skipping.")
                    continue
                pred_prob = model.predict(features_df)[0]
                fraud_prediction = int(pred_prob > 0.35)


                table.put_item(Item={
                    'transaction_id': str(payload.get('trans_num', 'unknown')),
                    'is_fraud': fraud_prediction,
                    'raw_data': json.dumps(payload)
                })

                print(json.dumps({
                    "status": "Processing Complete",
                    "transaction_id": payload.get('trans_num', 'unknown'),
                    "fraud_prediction": fraud_prediction
                }))
                
                if fraud_prediction == 1:
                    print(f"⚠️ Fraud Detected! Transaction ID: {payload.get('trans_num', 'unknown')}")
                    
                    sns_client.publish(
                        TopicArn=sns_arn,
                        Message=f"Fraudulent transaction detected!\n\nTransaction ID: {payload.get('trans_num')}\nAmount: ${payload.get('amt')}\nCity: {payload.get('city')}",
                        Subject="Fraud Alert: Suspicious Transaction Detected"
                    )

            except Exception as record_error:
                print(f"Error processing record: {record_error}")

        return {
            'statusCode': 200,
            'body': 'Data processed and predictions stored successfully'
        }
    except Exception as e:
        print(f"Lambda failed with error: {e}")
        return {
            'statusCode': 500,
            'body': str(e)
        }
