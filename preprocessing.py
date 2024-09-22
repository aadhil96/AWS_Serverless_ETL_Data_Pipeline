import json
import boto3
import csv
from io import StringIO

def lambda_handler(event, context):
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    # The S3 bucket name and file name for the input (landing zone)
    source_bucket = 'landing-data-ingestion'
    csv_file_key = 'titanic.csv'
    
    # The S3 bucket name for the processed file (destination)
    destination_bucket = 'processesd-csv'
    output_file_key = 'titanic_transformed.csv'
    
    try:
        # Step 1: Extract - Get the CSV file from S3
        response = s3.get_object(Bucket=source_bucket, Key=csv_file_key)
        
        # Read the CSV file content
        csv_content = response['Body'].read().decode('utf-8').splitlines()
        csv_reader = csv.reader(csv_content)
        
        # Get the header
        headers = next(csv_reader)
        
        # Step 2: Transform - Filter and clean data
        transformed_data = []
        for row in csv_reader:
            # Convert 'Age' to a float (if empty, skip the row)
            try:
                age = float(row[5])  # 'Age' is in the 6th column (index 5)
            except ValueError:
                continue  # Skip rows with no 'Age' data
            
            survived = int(row[1])  # 'Survived' is in the 2nd column (index 1)
            
            # Only include rows where the passenger survived
            if survived == 1:
                transformed_data.append(row)
        
        # Step 3: Load - Write the transformed data back to a new CSV file in S3
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)  # Write header
        writer.writerows(transformed_data)  # Write transformed data
        
        # Upload the transformed CSV to S3
        s3.put_object(Bucket=destination_bucket, Key=output_file_key, Body=output.getvalue())
        
        # Return success message
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'ETL process completed successfully.'})
        }
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
