import os
import psycopg2

from dotenv import load_dotenv

load_dotenv(dotenv_path="/app/.env")

class CreateTempTables:
    def __init__(self):
        """
        Initialize the CreateTempTables class.
        """
        #Default database configuration
        self.db_config = {
            "dbname":  os.getenv("DB_NAME", "postgres"),
            "user":    os.getenv("DB_USER", "postgres"),
            "password":os.getenv("DB_PASSWORD", ""),
            "host":    os.getenv("DB_HOST", "localhost"),
            "port":    os.getenv("DB_PORT", "5432"),
        }

    def get_connection(self):
        """Establish and return a database connection."""
        return psycopg2.connect(**self.db_config)
    
    def create_tables(self):
        """Create all temporary tables for the PPP Loan Data."""
        # Format the SQL with the state
        sql = self._get_sql_template().format(self=self)
        
        # Execute the SQL commands
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            print(f"Successfully created temporary tables for the PPP Loan Data.")
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error creating tables: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()

    def _get_sql_template(self):
        """Return the SQL template for creating temporary PPP Loan Data Table."""
        return f"""
            -- Allow Extensions for UUID and Random UUID
            CREATE EXTENSION IF NOT EXISTS pgcrypto;

            -- Drop existing tables if they exist to reset the schema (in reverse dependency order)
            DROP TABLE IF EXISTS ppp_loan_data_airflow CASCADE;
            DROP TABLE IF EXISTS ppp_loan_data_error CASCADE;

            -- Drop Custom Enum Types to allow recreation
            DROP TYPE IF EXISTS error_type_enum CASCADE;

            -- Create Custom Enum Types for PPP Loan Data
            CREATE TYPE error_type_enum as ENUM ('parsing', 'processing', 'validation', 'blanket');

            -- Create Error Table to store processing errors
            CREATE TABLE IF NOT EXISTS ppp_loan_data_error (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                error_type error_type_enum NOT NULL,
                error_message text,
                stack_trace text,
                element text,
                shard_id BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Create Table to store the PPP Loan Data
            CREATE TABLE IF NOT EXISTS ppp_loan_data_airflow (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                loan_number TEXT NOT NULL,
                date_approved TIMESTAMP NOT NULL,
                borrower_name TEXT NOT NULL,
                sba_office_code TEXT,
                processing_method TEXT,
                borrower_address TEXT,
                borrower_city TEXT,
                borrower_state TEXT,
                borrower_zip TEXT,
                loan_status_date TIMESTAMP,
                loan_status TEXT,
                term INTEGER,
                sba_guaranty_percentage FLOAT,
                initial_approval_amount FLOAT,
                current_approval_amount FLOAT,
                undisbursed_amount FLOAT,
                franchise_name TEXT,
                servicing_lender_location_id TEXT,
                servicing_lender_name TEXT,
                servicing_lender_address TEXT,
                servicing_lender_city TEXT,
                servicing_lender_state TEXT,
                servicing_lender_zip TEXT,
                rural_urban_indicator TEXT,
                hubzone_indicator TEXT,
                lmi_indicator TEXT,
                business_age_description TEXT,
                project_city TEXT,
                project_county_name TEXT,
                project_state TEXT,
                project_zip TEXT,
                cd TEXT,
                jobs_reported INTEGER,
                naics_code TEXT,
                race TEXT,
                ethnicity TEXT,
                utilities_proceed FLOAT,
                payroll_proceed FLOAT,
                mortgage_interest_proceed FLOAT,
                rent_proceed FLOAT,
                refinance_eidl_proceed FLOAT,
                health_care_proceed FLOAT,
                debt_interest_proceed FLOAT,
                business_type TEXT,
                originating_lender_location_id TEXT,
                originating_lender TEXT,
                originating_lender_city TEXT,
                originating_lender_state TEXT,
                gender TEXT,
                veteran TEXT,
                non_profit TEXT,
                forgiveness_amount FLOAT,
                forgiveness_date TIMESTAMP,
                shard_id BIGINT
            );

            -- Create Indexes for the PPP Loan Data Table
            CREATE INDEX IF NOT EXISTS idx_loan_number ON ppp_loan_data_airflow (loan_number);
            CREATE INDEX IF NOT EXISTS idx_borrower_name ON ppp_loan_data_airflow (borrower_name);
            CREATE INDEX IF NOT EXISTS idx_date_approved ON ppp_loan_data_airflow (date_approved);
        """
    
