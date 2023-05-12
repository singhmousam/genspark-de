import boto3
import pandas as pd
import io


def write_read_file_from_s3(access_key, secret_key, bucket_name,input_path,dest_path):
    print('Initializing s3 session')
    s3 = boto3.client('s3', aws_access_key_id=access_key, 
                      aws_secret_access_key=secret_key)
    print('Writing data to path s3://' + '/' + dest_path)
    s3.upload_file(input_path, bucket_name, dest_path)
    print("Successfully uploaded to S3")
    
    print('Reading data from path:s3//'+ '/' + dest_path)
    res = s3.get_object(Bucket= bucket_name, Key=dest_path)
    df = pd.read_csv(io.BytesIO(res['Body'].read()), encoding='utf8')
    return df
    
if __name__ == '__main__':
    print('S3 job connect')
    write_read_file_from_s3(1, 2, 3, 4,5)
    