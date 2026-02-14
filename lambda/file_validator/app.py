import json
import boto3
import time
from datetime import datetime, timezone

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

table_name = 'event-platform-metadata'

def lambda_handler(event, context):
    # Retrieveing the S3 records from the SQS message
    for record in event['Records']:
        body = json.loads(record['body'])

        # Iterating through the S3 records for ETag, Bucket name & Key
        for s3_records in body['Records']:
            etag = s3_records['s3']['object']['eTag']
            bucket = s3_records['s3']['bucket']['name']
            key = s3_records['s3']['object']['key']

            # Object Key verified/added in DynamoDB table entries
            try:
                dynamodb.put_item(
                    TableName = table_name,
                    Item = {
                        'etag': {'S': etag},
                        'status': {'S': "IN_PROGRESS"},
                        'lease_expiry': {'N': str((int(time.time()) + 60))},
                        'created_at': {'S': datetime.now(timezone.utc).isoformat()},
                        'updated_at': {'S': datetime.now(timezone.utc).isoformat()},
                        'retry_count': {'N': '0'}
                    },
                    ConditionExpression = 'attribute_not_exists(etag)'
                )
                print(f'New File: {key}: Table Updated')
                print (f'Processing file, {key}')
                process_file(bucket, key, etag)

            # Catching exception if the etag already exists in the table
            except dynamodb.exceptions.ConditionalCheckFailedException:
                existing = dynamodb.get_item(
                    TableName = table_name,
                    Key = {
                        'etag': {'S': etag}
                    }
                )
                
                # Checks for multiple conditions for the existing etags
                if existing['Item']['status']['S'] == 'COMPLETED':
                    print(f'SKIP - ALREADY PROCESSED - {key}')
                    continue

                elif existing['Item']['status']['S'] == 'IN_PROGRESS' and int(existing['Item']['lease_expiry']['N']) > int(time.time()):
                    print(f'SKIP - ANOTHER WORKER PROCESSING - {key}')
                    continue
        
                elif existing['Item']['status']['S'] == 'IN_PROGRESS' and int(existing['Item']['lease_expiry']['N']) <= int(time.time()):
                    print (f'RECLAIMING DUE TO EXPIRED LEASE - {key}')

                    try:
                        dynamodb.update_item (
                            TableName = table_name,
                            Key = {
                                'etag': {'S': etag}
                            },
                            UpdateExpression = '''
                                SET #u = :updated_at, 
                                    #l = :new_lease_expiry,
                                    #r = #r + :inc
                            ''',
                            ExpressionAttributeNames = {
                                '#u': 'updated_at',
                                '#l': 'lease_expiry',
                                '#r': 'retry_count'
                            },
                            ConditionExpression = 'lease_expiry = :old_lease_expiry', # This prevents two lambda from processing the same file concurrently
                            ExpressionAttributeValues = {
                                ':updated_at': {'S': datetime.now(timezone.utc).isoformat()},
                                ':old_lease_expiry' : {'N': str(int(existing['Item']['lease_expiry']['N']))}, # This prevents two lambda from processing the same file concurrently
                                ':new_lease_expiry': {'N': str((int(time.time()) + 60))},
                                ':inc': {'N': '1'}
                            }
                        )
                        print (f'Processing reclaimed file, {key}')
                        process_file(bucket, key, etag)

                    except dynamodb.exceptions.ConditionalCheckFailedException:
                        print(f'Lost reclaim race for {key}')
                        continue

    return {
            'statusCode': 200,
        }

#Retrieving only the new or reclaimed objects
def process_file(bucket, key, etag):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read().decode('utf-8')
        print(f'Completed reading the file, {key}')

        try:
            parsed = json.loads(data)
            print(f'File {key} is a valid JSON. Processing completed')

            # Moving the processed file to a their respective folders in the same bucket
            file_name = key.split('/')[-1]

            if file_name.startswith('order'):
                new_key = f'validated/orders_events/{file_name}'
                move_approved_file(bucket, key, new_key)

            elif file_name.startswith('payment'):
                new_key = f'validated/payments_events/{file_name}'
                move_approved_file(bucket, key, new_key)

            elif file_name.startswith('user'):
                new_key = f'validated/user_events/{file_name}'
                move_approved_file(bucket, key, new_key)

            update_table(etag, key)
            
        except json.JSONDecodeError:
            print(f'Malformed JSON')
            malformed_file_handler(bucket, key, etag)
            
            dynamodb.update_item(
                TableName = table_name,
                Key = {
                    'etag': {'S': etag}
                },
                UpdateExpression = 'SET #s = :status, #u = :updated_at',
                ExpressionAttributeNames = {
                    '#s': 'status',
                    '#u': 'updated_at'
                },
                ExpressionAttributeValues = {
                    ':status': {'S': 'REJECTED'},
                    ':updated_at': {'S': datetime.now(timezone.utc).isoformat()}
                }
            )

            print(f'DATA ISSUE - {key} moved to rejected folder')

    except Exception as e:
        print(f'Error message: {e}')
        raise e
                
    
def update_table(etag, key):
    try:
        dynamodb.update_item(
            TableName = table_name,
            Key = {
                'etag': {'S': etag}
            },
            UpdateExpression = 'SET #s = :status, #u = :updated_at',
            ExpressionAttributeNames = {
                '#s': 'status',
                '#u': 'updated_at'
            },
            ExpressionAttributeValues = {
                ':status': {'S': 'COMPLETED'},
                ':updated_at': {'S': datetime.now(timezone.utc).isoformat()}
            }
        )
        print(f'Update Successful - Status: COMPLETED for file: {key}')
    except Exception as e:
        print(f'Error message: {e}')
        raise e
    
def move_approved_file(bucket, key, new_key):
    try:
        s3.copy_object(
            Bucket=bucket, 
            CopySource={
                'Bucket': bucket, 
                'Key': key
            }, 
            Key=new_key
        )
        s3.delete_object(
            Bucket=bucket, 
            Key=key
        )
        print(f'File {key} moved to {new_key}')
    except Exception as e:
        print(f'Error message: {e}')
        raise e
    
def malformed_file_handler(bucket, key, etag):
    try:
        s3.copy_object(
            Bucket=bucket, 
            CopySource={
                'Bucket': bucket, 
                'Key': key
            }, 
            Key=f'rejected/{key.split("/")[-1]}'
        )
        s3.delete_object(
            Bucket=bucket, 
            Key=key
        )
        print(f'Malformed file {key} moved to malformed folder')
    except Exception as e:
        print(f'Error message: {e}')
        raise e