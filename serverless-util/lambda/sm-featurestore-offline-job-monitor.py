import json
import boto3

sm = boto3.client('sagemaker')

def handle(event, context):
    job_name = event['ProcessingJobName']
    print(job_name)

    response = sm.describe_processing_job(
        ProcessingJobName=job_name
    )
    job_status = response["ProcessingJobStatus"]
    
    print(response)
    
    return {
        'ProcessingJobName': job_name,
        'ProcessingJobStatus': job_status
    }
