import csv
import boto3

ENV = "dev"
CSV_FILE = "./batch.csv"
FAILED_CSV = "./failure.csv"
DICTUM_TABLE = "fraud-disputes-letters-v2"
CHUNK_SIZE = 25

REGION = "us-east-1" if ENV == "prod" else "us-west-2"
PROFILE = "prod" if ENV == "prod" else "default"

sess = boto3.Session(profile_name=PROFILE, region_name=REGION)
dynamodb = sess.resource('dynamodb', REGION)
table = dynamodb.Table(DICTUM_TABLE)

# Read CSV file
def read_csv(file_name):
    with open(file_name, mode='r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        data = [row for row in csv_reader]
    return data

# Function to split data into chunks
def chunk_data(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

# Function to batch delete items from DynamoDB and track failures
def batch_delete_items(table_name, data):
    failed_deletes = []
    for chunk in chunk_data(data, CHUNK_SIZE):
        delete_requests = [{
            'DeleteRequest': {'Key': {"ticket_number": int(item['dictum_num'])}}
        } for item in chunk]
        request_items = {table_name: delete_requests}
        
        try:
            response = dynamodb.batch_write_item(RequestItems=request_items)
            unprocessed_items = response.get('UnprocessedItems', {})
            
            if unprocessed_items:
                failed_deletes.extend(unprocessed_items.get(table_name, []))
        except boto3.ClientError as e:
            print(f"Error deleting items: {e}")
            failed_deletes.extend(delete_requests)
    
    return failed_deletes

data = read_csv(CSV_FILE)

failed_items = batch_delete_items(DICTUM_TABLE, data)

if failed_items:
    # Write failed items to CSV
    with open(FAILED_CSV, 'w', newline='') as csvfile:
        fieldnames = ['dictum_num']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        # csv.DictReader
        
        writer.writeheader()
        for item in failed_items:
            writer.writerow(item['DeleteRequest']['Key'])

    print(f"Failed items have been written to {FAILED_CSV}")
else:
    print("All items were successfully deleted.")