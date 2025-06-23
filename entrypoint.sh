#!/bin/bash

set -e

echo "Starting PPP Loan Data processing..."

# Test S3 connection first
echo "Testing S3 connection..."
python s3_connection.py

if [ $? -ne 0 ]; then
    echo "Python S3 connection script failed. Please check your AWS credentials and bucket configuration."
    exit 1
fi

echo "Python Script was ran and S3 connection was successful!"

# Check if both CSV files exist locally
if [ ! -f "ppp_csvs/ppp.csv" ] || [ ! -f "ppp_csvs/ppp_subset.csv" ]; then
    echo "CSV files not found locally. Downloading from S3..."
    
    # Download files from S3
    python s3_download.py
    
    if [ $? -ne 0 ]; then
        echo "❌ Error: Failed to download files from S3"
        exit 1
    fi
    
    echo "✅ Files downloaded successfully from S3!"
else
    echo "CSV files already exist locally. Skipping S3 download."
fi

# Wait for database to be ready
echo "Waiting for database to be ready..."
sleep 10

# Run the data processing pipeline
echo "Starting PPP loan data processing pipeline..."
python run.py

echo "Pipeline execution completed!"

# Wait for Postgres to be ready for FastAPI
DB_CHECK_CMD="import psycopg2; psycopg2.connect(dbname='${DB_NAME}', user='${DB_USER}', password='${DB_PASSWORD}', host='${DB_HOST}', port='${DB_PORT}')"
echo "Waiting for Postgres to be ready for FastAPI..."
until python -c "$DB_CHECK_CMD"; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 2
done
echo "Postgres is up - starting FastAPI server"

# Launch the FastAPI server
echo "Starting FastAPI server..."
python -m uvicorn server:app --host 0.0.0.0 --port 8001 --reload

# Keep the container running for debugging or additional processing
echo "Container is ready. You can now run additional commands or start your application."
tail -f /dev/null 