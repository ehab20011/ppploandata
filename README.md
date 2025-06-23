# PPP Loan Data Processing & Analytics Platform

A comprehensive full-stack application that automates the collection, processing, and analysis of PPP (Paycheck Protection Program) loan data. Built with FastAPI, PostgreSQL, Apache Beam, and Docker containerization.

![PPP](https://github.com/user-attachments/assets/87b2f054-01ac-4e96-bdc1-27dd73e6bc78)

## 🌟 Features

- **Automated Data Collection**: Web scraping using Playwright to download PPP loan datasets from SBA
- **Advanced Data Processing**: Apache Beam pipeline for scalable data transformation and validation
- **Robust Database**: PostgreSQL with optimized schemas and comprehensive indexing
- **Modern API**: FastAPI-powered REST API with automatic documentation
- **Interactive Frontend**: Responsive web interface for data exploration and visualization
- **Complete Containerization**: Docker Compose setup for easy deployment and development
- **Data Quality**: Comprehensive validation using Pydantic models
- **Real-time Processing**: Automated pipeline execution with error handling

## 🏗 Architecture

### Data Pipeline
1. **Data Collection (`playwright.py`)**
   - Automated web scraping of SBA PPP loan data
   - Downloads CSV files from government sources
   - Handles authentication and session management
   - Error handling and retry mechanisms

2. **Data Processing (`run.py` + `utils.py`)**
   - Apache Beam pipeline for scalable data processing
   - Pydantic-based data validation and transformation
   - Batch processing with error handling
   - Data quality checks and normalization

3. **Database Management (`sql.py`)**
   - PostgreSQL schema creation and management
   - Optimized table structures for PPP loan data
   - Comprehensive indexing for query performance
   - Data integrity constraints and validation

### API Layer (`server.py`)
- **FastAPI Framework**: High-performance async API
- **RESTful Endpoints**: Comprehensive loan data access
- **Advanced Filtering**: Search by borrower, loan number, date ranges
- **Analytics Endpoints**: Top borrowers, forgiveness analysis
- **CORS Support**: Cross-origin resource sharing for frontend integration

### Frontend (`frontend/`)
- **Modern Web Interface**: HTML5, CSS3, JavaScript
- **Responsive Design**: Mobile-friendly interface
- **Interactive Features**: Real-time data fetching and display
- **Data Visualization**: Charts and analytics dashboard

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Git
- 4GB+ RAM recommended

### Installation & Deployment

1. **Clone the repository:**
```bash
git clone <your-repo-url>
cd PPPLoanData
```

2. **Set up environment variables:**
Create a `.env` file in the project root:
```env
DB_NAME=ppp_db
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=postgres
DB_PORT=5432
```

3. **Start the application:**
```bash
docker-compose up --build
```

The platform will automatically:
- Initialize PostgreSQL database
- Download PPP loan data (if not present)
- Process and validate the data
- Create database tables and indexes
- Start the FastAPI server
- Serve the frontend application

### Access Points
- **Frontend Interface**: http://localhost
- **API Documentation**: http://localhost:8001/docs
- **Health Check**: http://localhost:8001/health

## 🔧 Configuration

### Docker Services
- **ppp-data-processor**: Main application container (FastAPI + data processing)
- **postgres**: PostgreSQL database service
- **frontend**: Nginx web server for static files

### Key Configuration Files
- `docker-compose.yml`: Service orchestration and networking
- `Dockerfile`: Application container configuration
- `config.conf`: Pipeline configuration parameters
- `requirements.txt`: Python dependencies

## 📊 Data Model

### Core Tables
1. **ppp_loan_data_airflow**
   - Loan details (number, amounts, dates)
   - Business information (name, address, type)
   - Lender information (originating and servicing)
   - Demographic data (race, ethnicity, gender)
   - Loan proceeds breakdown
   - Forgiveness information

2. **ppp_loan_data_error**
   - Error tracking and logging
   - Processing error details
   - Data validation failures

### Data Processing Features
- **Data Validation**: Pydantic models ensure data quality
- **Error Handling**: Comprehensive error tracking and reporting
- **Batch Processing**: Efficient handling of large datasets
- **Data Transformation**: Normalization and standardization

## 🔌 API Endpoints

### Core Operations
- `GET /health`: Service health check
- `GET /loans`: Retrieve loan data with pagination
- `GET /loans/search/by-borrower`: Search loans by business name
- `GET /loans/search/by-loan-number`: Get specific loan details
- `GET /loans/search/by-date-range`: Filter by approval date range
- `GET /loans/search/by-forgiveness-amount`: Find loans by forgiveness amount
- `GET /loans/top-borrowers`: Top 10 borrowers by forgiveness amount

### Query Parameters
- **Pagination**: `limit` parameter for result limiting
- **Filtering**: Date ranges, amount thresholds, business types
- **Search**: Full-text search capabilities

### Local Development Setup
1. **Create virtual environment:**
```bash
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
```

3. **Set up database:**
```bash
# Ensure PostgreSQL is running
# Update .env file with local database credentials
```

4. **Run development server:**
```bash
python -m uvicorn server:app --reload --host 0.0.0.0 --port 8001
```

### Project Structure
```
PPPLoanData/
├── frontend/              # Web interface
│   ├── index.html
│   ├── script.js
│   └── styles.css
├── ppp_csvs/             # Data storage
│   ├── ppp.csv
│   └── ppp_subset.csv
├── tests/                # Test suite
│   └── local_tests/
├── server.py             # FastAPI application
├── run.py                # Main pipeline runner
├── utils.py              # Apache Beam utilities
├── models.py             # Pydantic data models
├── sql.py                # Database management
├── playwright.py         # Data collection
├── subset.py             # Data subset creation
├── config.conf           # Configuration file
├── requirements.txt      # Python dependencies
├── Dockerfile            # Container configuration
├── docker-compose.yml    # Service orchestration
└── entrypoint.sh         # Container startup script
```

## 📈 Performance & Scalability

### Optimization Features
- **Connection Pooling**: Efficient database connection management
- **Query Optimization**: Strategic indexing and query tuning
- **Batch Processing**: Apache Beam for scalable data processing
- **Caching**: Ready for Redis integration
- **Load Balancing**: Container-ready for horizontal scaling

### Monitoring
- **Health Checks**: API endpoint monitoring
- **Error Tracking**: Comprehensive error logging
- **Performance Metrics**: Query execution monitoring
- **Resource Usage**: Container resource monitoring

## 🔍 Troubleshooting

### Common Issues

1. **Database Connection Errors**
   ```bash
   # Check database logs
   docker-compose logs postgres
   
   # Verify environment variables
   docker-compose exec ppp-data-processor env | grep DB_
   ```

2. **Data Processing Issues**
   ```bash
   # Check application logs
   docker-compose logs ppp-data-processor
   
   # Verify CSV files exist
   ls -la ppp_csvs/
   ```

3. **Port Conflicts**
   ```bash
   # Check for port usage
   lsof -i :8001 -i :5432 -i :80
   
   # Update ports in docker-compose.yml if needed
   ```

4. **Environment Variable Issues**
   ```bash
   # Ensure .env file exists and has correct values
   cat .env
   
   # Rebuild containers after .env changes
   docker-compose down -v && docker-compose up --build
   ```

### Debug Commands
```bash
# View all container logs
docker-compose logs -f

# Access container shell
docker-compose exec ppp-data-processor bash

# Check database connectivity
docker-compose exec ppp-data-processor python -c "import psycopg2; print('DB OK')"
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request
