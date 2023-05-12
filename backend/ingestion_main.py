import configparser
import argparse
import logging
import pandas as pd
from s3_connector import write_read_file_from_s3
logFile= 'ingestion_app.log'
logger = logging.getLogger(__name__)
logging.basicConfig(filename = logFile,filemode = 'a', 
                    level=logging.INFO, format='%(levelname)s: %(asctime)s: %(message)s')

parser = argparse.ArgumentParser()
parser.add_argument(
    '--config_path', default='', type=str,
    help="The source config file path")

args = parser.parse_args()
config_path = args.config_path
logging.info('---------------------------------------------------------------------------------------------')
logging.info('Read config file: ' + config_path)
config = configparser.ConfigParser()
config.read(config_path)
config.read(r'config.ini')   

logging.info('Fetch parameters')
access_key = config.get('aws', 'access_key')
secret_key = config.get('aws', 'secret_key')


bucket_name = config.get('aws', 'bucket_name')
input_path = config.get('inputs', 'input_path')

dest_path=config.get('aws','dest_path')

logging.info('Running read file from s3 operation')
df = write_read_file_from_s3(access_key, secret_key, bucket_name,input_path, dest_path)
print(df.head(5))

logging.info('Operation successful')


