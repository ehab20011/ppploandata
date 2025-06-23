#!/bin/bash

set -e

echo "🚀 Starting PPP Loan Data processing on Railway..."

# Copy Railway config to standard location
cp config.railway.conf config.conf

# Test S3 connection first
echo "🔍 Testing S3 connection..."
python s3_connection.py

if [ $? -ne 0 ]; then
    echo "❌ Python S3 connection script failed. Please check your AWS credentials and bucket configuration."
    exit 1
fi

echo "✅ S3 connection successful!"

# Always download files from S3 on Railway (ephemeral storage)
echo "📥 Downloading CSV files from S3..."
python s3_download.py

if [ $? -ne 0 ]; then
    echo "❌ Error: Failed to download files from S3"
    exit 1
fi

echo "✅ Files downloaded successfully from S3!"

# Wait for database to be ready (Railway provides this as a service)
echo "⏳ Waiting for database to be ready..."
sleep 15

# Run the data processing pipeline
echo "🔄 Starting PPP loan data processing pipeline..."
python run.py --runner=DirectRunner --save_main_session --pickle_library cloudpickle

if [ $? -ne 0 ]; then
    echo "❌ Error: Data processing pipeline failed"
    exit 1
fi

echo "✅ Data processing completed successfully!"

# Wait for Postgres to be ready for FastAPI
DB_CHECK_CMD="import psycopg2; psycopg2.connect(dbname='${DB_NAME}', user='${DB_USER}', password='${DB_PASSWORD}', host='${DB_HOST}', port='${DB_PORT}')"
echo "⏳ Waiting for Postgres to be ready for FastAPI..."
until python -c "$DB_CHECK_CMD"; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 2
done
echo "✅ Postgres is up - starting FastAPI server"

# Launch the FastAPI server
echo "🌐 Starting FastAPI server..."
python -m uvicorn server:app --host 0.0.0.0 --port $PORT --reload 