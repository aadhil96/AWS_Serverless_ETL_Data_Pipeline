import boto3
import pymysql
import pandas as pd
import io
import os

# Lambda function handler
def lambda_handler(event, context):
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    # The S3 bucket name and file name for the input (landing zone)
    source_bucket = 'processed-csv'
    csv_file_key = 'titanic_transformed.csv'
    
    # RDS MySQL connection details
    rds_host = os.environ['RDS_HOST']  # Fetch these from environment variables
    rds_user = os.environ['RDS_USER']
    rds_password = os.environ['RDS_PASSWORD']
    rds_db_name = os.environ['RDS_DB_NAME']
    table_name = 'titanic_data'  # Existing table name in your database
    
    try:
        # Step 1: Extract - Get the CSV file from S3
        response = s3.get_object(Bucket=source_bucket, Key=csv_file_key)
        
        # Read the CSV file content into a pandas DataFrame
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_content))
        
        # Step 2: Load - Insert DataFrame into RDS MySQL database
        # Connect to RDS MySQL instance
        connection = pymysql.connect(
            host=rds_host,
            user=rds_user,
            password=rds_password,
            database=rds_db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        with connection.cursor() as cursor:
            # Insert each row from the DataFrame into the MySQL table
            for _, row in df.iterrows():
                sql = f"""
                INSERT INTO {table_name} (PassengerId, Survived, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Survived=VALUES(Survived),
                    Pclass=VALUES(Pclass),
                    Name=VALUES(Name),
                    Sex=VALUES(Sex),
                    Age=VALUES(Age),
                    SibSp=VALUES(SibSp),
                    Parch=VALUES(Parch),
                    Ticket=VALUES(Ticket),
                    Fare=VALUES(Fare),
                    Cabin=VALUES(Cabin),
                    Embarked=VALUES(Embarked)
                """
                cursor.execute(sql, (
                    row['PassengerId'], row['Survived'], row['Pclass'], row['Name'],
                    row['Sex'], row['Age'], row['SibSp'], row['Parch'], row['Ticket'],
                    row['Fare'], row['Cabin'], row['Embarked']
                ))
        
        connection.commit()
        
        return {
            'statusCode': 200,
            'body': 'Data inserted successfully into RDS MySQL'
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error occurred: {str(e)}"
        }
    
    finally:
        if connection:
            connection.close()
