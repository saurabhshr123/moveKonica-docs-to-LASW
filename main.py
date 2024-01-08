import json
import boto3
import os
import logging
def lambda_handler(event, context):
    # Replace with your S3 bucket names
    source_bucket = "konica-docs"
    destination_bucket = "4-main-lasw-appdocs"
    
    # Create S3 clients for both buckets
    source_client = boto3.client('s3')
    destination_client = boto3.client('s3')
    source_prefix = '2005/'
    
    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Get the list of objects from the source bucket
    paginator = source_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=source_bucket, Prefix=source_prefix)

    #objects = source_client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)['Contents']
    # print('objects  ', objects)
    
    # Iterate over the objects
    count = 0
    for page in page_iterator:
        for obj in page['Contents']:
            #for obj in objects:
            #print(obj['Key'])
            source_key = obj['Key']
            
            destination_key = source_key
            
            #destination_key = source_key[len(source_prefix):]
            
            # Check if the object already exists in the destination bucket
            # Try to head the object
            try:
                if destination_client.head_object(Bucket=destination_bucket, Key=destination_key)['ResponseMetadata']['HTTPStatusCode'] == 200:
                    #print(f"Skipping {destination_key} as it already exists in the destination bucket.")
                    logger.info(f"Skipping {destination_key} as it already exists in the destination bucket.")
                    continue
            except Exception as e:
                pass
        
            # Copy the object from the source bucket to the destination bucket
            source_client.copy_object(CopySource={'Bucket': source_bucket, 'Key': source_key}, Bucket=destination_bucket, Key=destination_key)
            #print(f"Copied {source_key} to {destination_key}.")
            count += 1
            if count % 1000 == 0:
                logger.info(f"Copied {count} objects")
            logger.info(f"Copied {source_key} to {destination_key}.")
    
    print("All folders and contents have been moved from the source bucket to the destination bucket.")
    
    return {
        'statusCode': 200,
        'body': json.dumps('All files have been moved successfully!')
    }

