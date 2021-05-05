import boto3
import os
import datetime

region = os.getenv('region')

client = boto3.client('dynamodb', region)
client = boto3.client('s3', region)

bucket = 'retail-cart-bucket'
table = 'retail_cart_table'
current_time = datetime.datetime.now().isoformat()

def handler(event, context):
    records = event['Records']
    s3record = ''
    for event in records:
        if event['eventName'] == 'INSERT':
            Op = 'INSERT'
            AccountId = event['dynamodb']['NewImage']['AccountId']['S']
            ItemSKU = event['dynamodb']['NewImage']['ItemSKU']['S']
            Status = event['dynamodb']['NewImage']['Status']['S']
            CreateDate = event['dynamodb']['NewImage']['CreateDate']['S']
            s3record = s3record+Op+','+AccountId+','+ItemSKU+','+Status+','+CreateDate+"\n"
        elif event['eventName'] == 'MODIFY':
            Op = 'UPDATE'
            AccountId = event['dynamodb']['NewImage']['AccountId']['S']
            ItemSKU = event['dynamodb']['NewImage']['ItemSKU']['S']
            Status = event['dynamodb']['NewImage']['Status']['S']
            CreateDate = event['dynamodb']['NewImage']['CreateDate']['S']
            s3record = s3record+Op+','+AccountId+','+ItemSKU+','+Status+','+CreateDate+"\n"
        elif event['eventName'] == 'REMOVE':
            Op = 'DELETE'
            AccountId = event['dynamodb']['OldImage']['AccountId']['S']
            ItemSKU = event['dynamodb']['OldImage']['ItemSKU']['S']
            Status = event['dynamodb']['OldImage']['Status']['S']
            CreateDate = event['dynamodb']['OldImage']['CreateDate']['S']
            s3record = s3record + Op + ',' + AccountId + ',' + ItemSKU + ',' + Status + ',' + CreateDate + "\n"

    put_object_in_s3(s3record)


def put_object_in_s3(s3record):
    key = table + '_' + current_time
    response = client.put_object(
        Bucket =bucket,
        Key = key,
        Body = s3record
    )