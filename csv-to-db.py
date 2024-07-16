import json
import csv
import boto3
import psycopg

ENV = "dev"
CSV_FILE = "./batch.csv"
CHUNK_SIZE = 25

REGION = "us-east-1" if ENV == "prod" else "us-west-2"
PROFILE = "prod" if ENV == "prod" else "default"
SECRET_NAME = 'core_iam_user_write'

QUERY = """
INSERT INTO ccm.dictum_notification (dictum_num, status, s3_bucket, s3_key) 
VALUES (%s, %s, %s, %s)
ON CONFLICT (dictum_num) 
DO UPDATE SET 
    status = EXCLUDED.status, 
    s3_bucket = EXCLUDED.s3_bucket, 
    s3_key = EXCLUDED.s3_key 
RETURNING dictum_num;
"""

sess = boto3.Session(profile_name=PROFILE, region_name=REGION)

# Read CSV file
def read_csv(file_name):
    with open(file_name, mode='r', encoding='utf-8-sig') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        data = [row for row in csv_reader]
    return data

# Function to split data into chunks
def chunk_data(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

# Retrieve a secret from AWS according name
def get_secret(secret_name) -> dict[str: any]:
    # Create a Secrets Manager client
    client = sess.client('secretsmanager', REGION)

    try:
        # Retrieve the secret
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

        # Decrypts secret using the associated KMS key.
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)

    except Exception as e:
        print(f"An error occurred: {e}")

# Create connection from secret params
def get_conn(secret: dict[str: any]):
    # Get IAM token
    rds_client = boto3.client("rds")
    token = rds_client.generate_db_auth_token(
        DBHostname=secret["host"],
        Port=secret["port"],
        DBUsername=secret["username"],
        Region=REGION,
    )

    # Create DB connection
    conn = psycopg.connect(
        dbname=secret['dbname'],
        user=secret["username"],
        password=token, #secret["password"],
        host=secret["host"],
        port=secret["port"],
        sslmode="allow",
    )

    print(f"DB Connection: {not conn.closed}")
    return conn

secret = get_secret(SECRET_NAME)
conn = get_conn(secret)

data = read_csv(CSV_FILE)

with conn.cursor() as cur:
    for chunk in chunk_data(data, CHUNK_SIZE):
        for row in chunk:
            try:
                cur.execute(QUERY, (row['dictum_num'], row['status'], row['s3_bucket'], row['s3_key']))
            except psycopg.errors.DatabaseError as e:
                print(f"Error executing query for row {row}: {e}")
                conn.rollback()
                continue 
        try:
            conn.commit()
        except psycopg.errors.DatabaseError as e:
            print(f"Error committing transaction for chunk: {chunk}: {e}")
            conn.rollback()

conn.close()
