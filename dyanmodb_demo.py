import boto3
import os

region = os.getenv('region')
op = os.getenv('op')
op = str.upper(op)
status = os.getenv('status')
if status == None:
    pass
else:
    status = str.upper(status)
start_time = os.getenv('start_time')
end_time = os.getenv('end_time')
account_id = os.getenv('account_id')
if account_id == None:
    pass
else:
    account_id=str.upper(account_id)
itemsku = os.getenv('itemsku')


client = boto3.client('dynamodb', region)
tablename = 'retail_cart_table'
indexname = 'GSI1'


# "Query" API on Table to handle the following Access Patterns
# 1. View active products organized by most recently added items, for each customer
# 2. View saved products organized by most recently saved time, for each customer
# 3. View purchased products by most recently added items, for each customer

def view_products_for_customer(status, start_time, end_time, account_id):
    skstart = status+'#'+start_time
    skend = status+'#'+end_time
    accid = str.upper(account_id)

    response = client.query(
        TableName = tablename,
        Select = 'ALL_ATTRIBUTES',
        KeyConditionExpression =  'PK = :pk AND SK BETWEEN :skv1 AND :skv2',
        # ExpressionAttributeNames = {
        #
        # },
        ExpressionAttributeValues = {
            ":pk" : {"S":accid},
            ":skv1" : {"S":skstart},
            ":skv2" : {"S":skend}
        },
        ReturnConsumedCapacity = 'TOTAL'
    )

    for i in response['Items']:
        print(i)


# "Query" API on GSI1 to handle the following access pattern
# Query all customers in Active, Saved or Purchased state, for each product

def view_customers_for_product(itemsku, status):
    skstatus = status+'#'

    response = client.query(
        TableName= tablename,
        IndexName = indexname,
        Select='ALL_ATTRIBUTES',
        KeyConditionExpression='GSI1PK = :gsi1pk AND begins_with(GSI1SK, :gsi1sk)',
        # ExpressionAttributeNames = {
        #
        # },
        ExpressionAttributeValues={
            ":gsi1pk": {"S": itemsku},
            ":gsi1sk": {"S": skstatus}
        },
        ReturnConsumedCapacity='TOTAL'
    )

    for i in response['Items']:
        print(i)


# Write records using "PutItem" API after reading from file
# Read from file to write to table

def write_records():
    with open('RETAIL_CART_DATA_2.csv', 'r') as file:
        for line in file:
            email,status,create_date,itemsku,fname,lname = line.split(',')

            partition_key = str.upper(email)
            sort_key = str.upper(status)+'#'+create_date
            gsi1pk = itemsku
            gsi1sk = sort_key

            response = client.put_item(
                TableName = tablename,
                Item = {
                    'PK' : {'S':partition_key},
                    'SK' : {'S':sort_key},
                    'GSI1PK' : {'S':gsi1pk},
                    'GSI1SK' : {'S':gsi1sk},
                    'Status' : {'S': status},
                    'AccountId' : {'S': email},
                    'ItemSKU' : {'S': itemsku},
                    'CreateDate' : {'S' : create_date}
                }
            )



# Main Function
def main():
    if op == 'W':
        write_records()
    elif op == 'VPC':
        view_products_for_customer(status, start_time, end_time, account_id)
    elif op == 'VCP':
        view_customers_for_product(itemsku, status)



# Execute program
if __name__ == '__main__':
    main()