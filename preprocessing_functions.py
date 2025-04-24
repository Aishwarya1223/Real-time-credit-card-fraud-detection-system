import numpy as np
import pandas as pd
import boto3
import pickle
import joblib
from io import BytesIO
from sklearn.ensemble import RandomForestClassifier,VotingClassifier,AdaBoostClassifier,HistGradientBoostingClassifier
from geopy.distance import geodesic

bucket_name = 'credit-card-fraud-detection-system-bucket'
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('fraud-detection-results')

# Load Encoding Parameters
def load_cyclic_encoding_from_s3(bucket_name, s3_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    encoding_data = response['Body'].read()
    cols_max_values = pickle.loads(encoding_data)
    return cols_max_values

loaded_cols_max_values = load_cyclic_encoding_from_s3(bucket_name, 'cyclic_encoding.pkl')

# Cyclic Encoding Function
def cyclic_encode(df, col, max_value):
    df[f'{col}_sin'] = np.sin(2 * np.pi * df[col] / max_value)
    df[f'{col}_cos'] = np.cos(2 * np.pi * df[col] / max_value)

# Model and Scaler Loading Functions
def load_model_from_s3():
    response = s3_client.get_object(Bucket=bucket_name, Key='voting_classifier_dt_rf_ada.pkl')
    model_data = response['Body'].read()
    model = joblib.load(model_data)
    return model

def load_scaler_from_s3():
    response = s3_client.get_object(Bucket=bucket_name, Key='robust_scaler.pkl')
    scaler_data = response['Body'].read()
    scaler = joblib.load(scaler_data)
    return scaler

def load_encoding_from_s3(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    encoding_data = response['Body'].read()
    encoding_map = pickle.loads(encoding_data)
    return encoding_map


def load_target_encoding_from_s3(bucket_name, file_key):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    buffer = BytesIO(response['Body'].read())

    # Force conversion to dict to avoid ambiguity errors
    encoding_dict = dict(pickle.load(buffer))  
    print(f"Encoding dictionary loaded from S3: {file_key}")
    return encoding_dict


def apply_target_encoding(df, col, file_key):
    encoding_dict = load_target_encoding_from_s3(bucket_name, file_key)

    if not encoding_dict:
        print(f"Warning: No encoding dictionary found for '{col}'. Using global mean as fallback.")
        df[f'{col}_encoded'] = 0
        return

    # Compute global mean for fallback
    global_mean = sum(encoding_dict.values()) / len(encoding_dict)

    # Apply encoding safely
    df[f'{col}_encoded'] = df[col].map(encoding_dict).fillna(global_mean)

    print(f"Target encoding applied to '{col}'.")




def calculate_distance(row):
    # Check if any of the required values are null using individual checks
    if (pd.isna(row['lat']) | pd.isna(row['long']) |
        pd.isna(row['merch_lat']) | pd.isna(row['merch_long'])):
        return np.nan
    return geodesic((row['lat'], row['long']), (row['merch_lat'], row['merch_long'])).km




# Preprocessing Function
def preprocess_data(payload):
    # Convert payload to DataFrame
    features_df = pd.DataFrame([{
    'trans_date_trans_time': payload['trans_date_trans_time'],
    'category': payload['category'],
    'amt': payload['amt'],
    'city': payload['city'],
    'state': payload['state'],
    'lat': payload['lat'],
    'long': payload['long'],
    'city_pop': payload['city_pop'],
    'merch_lat': payload['merch_lat'],
    'merch_long': payload['merch_long']
    }])
    
    

    features_df=features_df.rename(columns={'trans_date_trans_time':'transaction_time'})
    # Convert to datetime
    features_df['transaction_time'] = pd.to_datetime(features_df['transaction_time'])
    
    features_df['transaction_time_year']=features_df['transaction_time'].dt.year
    features_df['transaction_time_month']=features_df['transaction_time'].dt.month
    features_df['transaction_time_day']=features_df['transaction_time'].dt.day
    features_df['transaction_time_hour']=features_df['transaction_time'].dt.hour
    features_df['transaction_time_min']=features_df['transaction_time'].dt.minute
    features_df['transaction_time_sec']=features_df['transaction_time'].dt.second
    
    features_df.drop(columns=['transaction_time'], inplace=True)

    
    
    #Encode category and state encoding
    
    apply_target_encoding(features_df, 'category', 'category_encoding.pkl')
    apply_target_encoding(features_df, 'state', 'state_encoding.pkl')
    apply_target_encoding(features_df, 'city', 'kfold_encoding.pkl')
    features_df.drop(['category','city','state'],axis=1,inplace=True)
    
    # Apply cyclic encoding
    for col, max_value in loaded_cols_max_values.items():
        cyclic_encode(features_df, col, max_value)

    # Drop original columns
    features_df.drop(columns=['transaction_time_day', 'transaction_time_hour', 
                               'transaction_time_min', 'transaction_time_sec','transaction_time_month'], inplace=True)

    # Log Transformations
    features_df['amt_log_transformed'] = np.log1p(features_df['amt'])
    features_df['city_pop_log_transformed'] = np.log1p(features_df['city_pop'])
    
    features_df.drop(columns=['amt', 'city_pop'], inplace=True)

    features_df['distance'] = features_df.apply(lambda row: calculate_distance(row), axis=1).fillna(0)
    
    
    features_df.drop(columns=[ 'lat', 'long', 'merch_lat','merch_long'],axis=1,inplace=True)
    # Reorder columns
    
    expected_columns = ['amt_log_transformed', 'city_pop_log_transformed', 'city_encoded',
       'distance', 'transaction_time_min_sin', 'transaction_time_sec_cos',
       'transaction_time_min_cos', 'transaction_time_sec_sin',
       'transaction_time_day_sin', 'transaction_time_day_cos']
    features_df = features_df.reindex(columns=[col for col in expected_columns if col in features_df.columns])
    

    return features_df
