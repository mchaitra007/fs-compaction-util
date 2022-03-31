from sagemaker.spark.processing import PySparkProcessor
from sagemaker import get_execution_role
from random import randint
import sagemaker
import logging
import boto3
import json
import botocore

logger = logging.getLogger('__name__')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

role = get_execution_role()
logger.info(f'Role = {role}')
sagemaker_session = sagemaker.Session()
region = sagemaker_session.boto_region_name
default_bucket = sagemaker_session.default_bucket()
account_id = sagemaker_session.account_id()
prefix = 'sagemaker-feature-store'
logger.info(f'Default bucket = {default_bucket}')
logger.info(f'Account ID = {account_id}')
mode_incremental = 'incremental'
mode_full = 'full'
mode_day = 'day'
partition_hour = 'hour'

sagemaker_client = boto3.client('sagemaker')

def compact_feature_group(feature_group_name: str, compact_mode: str, partition_mode: str, compact_uri: str, year: str, month: str, day: str):
    logger.info(f'\nFeature group name is {feature_group_name}')
    
    if not compact_mode:
        compact_mode = partition_hour
        
    if not partition_mode:
        partition_mode = mode_hour
        
    logger.info(f'\nCompact Mode {compact_mode}')
    logger.info(f'\nPartition Mode {partition_mode}')
    
    if compact_mode == mode_day and (not year or not month or not day):
        logger.info(f'\nMode \'day\' requires year, month and day parameters') 
        return
    
    if feature_group_name and len(feature_group_name):
        try: 
            feature_group = sagemaker_client.describe_feature_group(FeatureGroupName=feature_group_name)
            base_uri = feature_group['OfflineStoreConfig']['S3StorageConfig']['ResolvedOutputS3Uri']+'/'
            fg_table = feature_group['OfflineStoreConfig']['DataCatalogConfig']['TableName']
            logger.info(f'FG Table name  = {fg_table}')
            if not compact_uri:
                compact_uri = (f's3a://{default_bucket}/{prefix}/{account_id}'
                              f'/sagemaker/{region}/compact-offline-store/{fg_table}/data/')
            
            logger.info(f'base small files: {base_uri}')
            logger.info(f'dest compacted: {compact_uri}')

            pyspark_processor = PySparkProcessor(framework_version='3.0', #'2.4', # Spark version
                                             role=role,
                                             instance_type='ml.m5.4xlarge',
                                             instance_count=1,
                                             base_job_name='offline-compaction',
                                             env={'AWS_DEFAULT_REGION': region,
                                                  'mode': 'python'}
                                             )

            """
            Arguments to pass to pyspark_processor:
                feature_group_name: Name of the feature group
                s3_input_uri_prefix: Base S3 URI where the original files are stored
                s3_output_uri_prefix: Target S3 URI where compated files will be stored
                compact_mode: Valid values are 
                    'incremental' which will process previous day files, 
                    'full' which will process all files till date
                    'day' which will process all files for a sepcific date  
                partition_mode: Valid values are   
                    'day' which will compact all hourly files into one single file for the day
                    'hour' which will compact all hourly files into one single file for each hour
                year, month, day: pass a specific day to be processed, this overrides compact_mode
            """

            processor_arguments = ['--feature_group_name', fg_table, # replace w/ fg name later
                                               '--s3_input_uri_prefix', base_uri,
                                               '--s3_output_uri_prefix', compact_uri,
                                               '--compact_mode', compact_mode,
                                               '--partition_mode', partition_mode,                                             
                                               '--region_name', region
                                              ]
            if compact_mode == mode_day:
                processor_arguments.extend(['--year',year,'--month', month,'--day', day])
            logger.info(processor_arguments)
            pyspark_processor.run(submit_app='./featurestore_offline_compact_spark.py',
                              arguments = processor_arguments,
                              spark_event_logs_s3_uri=f's3://{default_bucket}/spark-logs',
                              logs=True,  # set logs=True to enable logging
                              wait=True)
        except botocore.client.ClientError as error:
            logger.info(f'Feature group not found {error}')
            raise error
    else:
        logger.info('Feature group name is empty')
    return