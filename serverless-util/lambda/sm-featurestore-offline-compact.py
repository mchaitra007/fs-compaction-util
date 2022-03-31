from random import randint
import logging
import boto3
import json
import botocore
import os
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sagemaker_client = boto3.client('sagemaker')
sts_client = boto3.client('sts')
session = boto3.session.Session()
prefix = 'sagemaker-feature-store'
bucket_prefix = 'sagemaker'
mode_incremental = 'incremental'
mode_full = 'full'
mode_day = 'day'
partition_hour = 'hour'
partition_day = 'day'
    

def get_unique_job_name(base_name: str):
    """ Returns a unique job name based on a given base_name
        and the current timestamp """
    timestamp = time.strftime('%Y%m%d-%H%M%S')
    return f'{base_name}-{timestamp}'
    
def handle(event, context):
    execution_role = os.environ["SAGEMAKER_ROLE"]
    instance_type = os.environ["SAGEMAKER_INSTANCE_TYPE"]
    instance_count = os.environ["SAGEMAKER_INSTANCE_COUNT"]
    volume_size = os.environ["SAGEMAKER_INSTANCE_VOLUME_SIZE"]
    
    logger.info(f'\n Role...{execution_role}')
    region = session.region_name
    logger.info(f'\n Region...{region}')
    creds = sts_client.get_caller_identity()
    logger.info(f"\n Creds...{execution_role}")
    account_id = creds['Account']
    
    feature_group_name = event.get('feature_group_name',None)
    logger.info(f"\n Feature group...{feature_group_name}")
    
    compact_mode = event.get('compact_mode',None)
    logger.info(f"\n Compact mode...{compact_mode}")
    
    partition_mode = event.get('partition_mode',None)
    logger.info(f"\n Partition mode...{partition_mode}")
    
    compact_uri = event.get('compact_uri',None)
    logger.info(f"\n Compact URI...{compact_uri}")

    year = event.get('year',None)
    logger.info(f"\n year...{year}")
    
    month = event.get('month',None)
    logger.info(f"\n month...{month}")
    
    day = event.get('day',None)
    logger.info(f"\n day...{day}")

    
    logger.info(f'\nFeature group name is {feature_group_name}')
    if not compact_mode:
        compact_mode = mode_incremental
        
    if not partition_mode:
        partition_mode = partition_hour
        
    logger.info(f'\nCompact Mode {compact_mode}')
    logger.info(f'\nPartition Mode {partition_mode}')
    
    if compact_mode == mode_day and (not year or not month or not day):
        logger.info(f'\nMode \'day\' requires year, month and day parameters') 
        return
    
    job_name = get_unique_job_name('offline-compaction-lambda')
    #default_bucket = f'{bucket_prefix}-{region}-{account_id}'
    if feature_group_name and len(feature_group_name):
        try: 
            feature_group = sagemaker_client.describe_feature_group(FeatureGroupName=feature_group_name)
            base_uri = feature_group['OfflineStoreConfig']['S3StorageConfig']['ResolvedOutputS3Uri']+'/'
            fg_table = feature_group['OfflineStoreConfig']['DataCatalogConfig']['TableName']
            logger.info(f'FG Table name  = {fg_table}')
            if not compact_uri:
                #compact_uri = (f's3a://{default_bucket}/{prefix}/{account_id}'
                 #             f'/sagemaker/{region}/compact-offline-store/{fg_table}/data/')
                compact_uri = base_uri.replace('offline-store','compact-offline-store')
            else:
                compact_uri = f'{compact_uri}/{fg_table}/data/'
            
            logger.info(f'base small files: {base_uri}')
            logger.info(f'dest compacted: {compact_uri}')
            
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
            
            image_uri=os.environ["SPARK_CONTAINER_IMAGE"]

            script_s3_uri = os.environ["PYSPARK_SCRIPT_PATH"]
            script_file_name = script_s3_uri.split('/')[-1]            
            logger.info(f'Script URI..{script_s3_uri}')
            logger.info(f'Script file name..{script_file_name}')
         
            response = sagemaker_client.create_processing_job(
                ProcessingInputs=[{
                    'InputName': 'script',
                    'S3Input': { 
                        'LocalPath': '/opt/ml/processing/script',
                        'S3DataDistributionType': 'FullyReplicated',
                        'S3DataType': 'S3Prefix',
                        'S3InputMode': 'File',
                        'S3Uri': script_s3_uri
                    }
                }],
                ProcessingJobName=job_name,
                RoleArn=execution_role,
                ProcessingResources={
                    'ClusterConfig': {
                        'InstanceCount': int(instance_count),
                        'InstanceType': instance_type,
                        'VolumeSizeInGB': int(volume_size)
                    }
                },
                AppSpecification={
                    'ImageUri': image_uri,
                    'ContainerArguments': processor_arguments,
                    'ContainerEntrypoint': ['smspark-submit', f'/opt/ml/processing/script/{script_file_name}']
                }
            )
            
            logger.info(f"Job name.....{response['ProcessingJobArn']}")
        except botocore.client.ClientError as error:
            logger.info(f'Feature group not found {error}')
            raise error
    else:
        logger.info('Feature group name is empty')
    return {
            'ProcessingJobName': job_name
    }

