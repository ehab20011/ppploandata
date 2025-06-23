import os
from typing import List, AsyncGenerator, Optional
from datetime import date

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from models import PPPLoanDataSchema

from sqlalchemy import String, Float, Date, Integer
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base, mapped_column, Mapped
from sqlalchemy.future import select

# Load Environment Variables from .env
load_dotenv(dotenv_path="/app/.env")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# set up SQLAlchemy
engine = create_async_engine(DATABASE_URL, echo=True)
async_session = async_sessionmaker(engine, expire_on_commit=False)
Base = declarative_base()

# intialize the app
app = FastAPI()

# only allow local dev and production domain (once i make one) to access the API
origins = [
    "http://127.0.0.1:5500",
    "http://localhost:5500",
    "http://localhost",
    "http://localhost:80",
    "http://localhost:8001",
    "https://*.railway.app",  # Allow Railway domains
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
# SQLAlchemy Model
class PPPLoanData(Base):
    __tablename__ = "ppp_loan_data_airflow" #sql table name for the ppp loan data

    loan_number: Mapped[str] = mapped_column(String, primary_key=True)
    date_approved: Mapped[Optional[Date]] = mapped_column(Date)
    borrower_name: Mapped[Optional[str]] = mapped_column(String)

    sba_office_code: Mapped[Optional[str]] = mapped_column(String)
    processing_method: Mapped[Optional[str]] = mapped_column(String)
    borrower_address: Mapped[Optional[str]] = mapped_column(String)
    borrower_city: Mapped[Optional[str]] = mapped_column(String)
    borrower_state: Mapped[Optional[str]] = mapped_column(String)
    borrower_zip: Mapped[Optional[str]] = mapped_column(String)
    loan_status_date: Mapped[Optional[Date]] = mapped_column(Date)
    loan_status: Mapped[Optional[str]] = mapped_column(String)
    term: Mapped[Optional[int]] = mapped_column(Integer)
    sba_guaranty_percentage: Mapped[Optional[float]] = mapped_column(Float)
    initial_approval_amount: Mapped[Optional[float]] = mapped_column(Float)
    current_approval_amount: Mapped[Optional[float]] = mapped_column(Float)
    undisbursed_amount: Mapped[Optional[float]] = mapped_column(Float)
    franchise_name: Mapped[Optional[str]] = mapped_column(String)

    servicing_lender_location_id: Mapped[Optional[str]] = mapped_column(String)
    servicing_lender_name: Mapped[Optional[str]] = mapped_column(String)
    servicing_lender_address: Mapped[Optional[str]] = mapped_column(String)
    servicing_lender_city: Mapped[Optional[str]] = mapped_column(String)
    servicing_lender_state: Mapped[Optional[str]] = mapped_column(String)
    servicing_lender_zip: Mapped[Optional[str]] = mapped_column(String)

    rural_urban_indicator: Mapped[Optional[str]] = mapped_column(String)
    hubzone_indicator: Mapped[Optional[str]] = mapped_column(String)
    lmi_indicator: Mapped[Optional[str]] = mapped_column(String)
    business_age_description: Mapped[Optional[str]] = mapped_column(String)
    project_city: Mapped[Optional[str]] = mapped_column(String)
    project_county_name: Mapped[Optional[str]] = mapped_column(String)
    project_state: Mapped[Optional[str]] = mapped_column(String)
    project_zip: Mapped[Optional[str]] = mapped_column(String)
    cd: Mapped[Optional[str]] = mapped_column(String)
    jobs_reported: Mapped[Optional[int]] = mapped_column(Integer)
    naics_code: Mapped[Optional[str]] = mapped_column(String)

    race: Mapped[Optional[str]] = mapped_column(String)
    ethnicity: Mapped[Optional[str]] = mapped_column(String)
    gender: Mapped[Optional[str]] = mapped_column(String)
    veteran: Mapped[Optional[str]] = mapped_column(String)
    non_profit: Mapped[Optional[str]] = mapped_column(String)

    utilities_proceed: Mapped[Optional[float]] = mapped_column(Float)
    payroll_proceed: Mapped[Optional[float]] = mapped_column(Float)
    mortgage_interest_proceed: Mapped[Optional[float]] = mapped_column(Float)
    rent_proceed: Mapped[Optional[float]] = mapped_column(Float)
    refinance_eidl_proceed: Mapped[Optional[float]] = mapped_column(Float)
    health_care_proceed: Mapped[Optional[float]] = mapped_column(Float)
    debt_interest_proceed: Mapped[Optional[float]] = mapped_column(Float)

    business_type: Mapped[Optional[str]] = mapped_column(String)
    originating_lender_location_id: Mapped[Optional[str]] = mapped_column(String)
    originating_lender: Mapped[Optional[str]] = mapped_column(String)
    originating_lender_city: Mapped[Optional[str]] = mapped_column(String)
    originating_lender_state: Mapped[Optional[str]] = mapped_column(String)

    forgiveness_amount: Mapped[Optional[float]] = mapped_column(Float)
    forgiveness_date: Mapped[Optional[Date]] = mapped_column(Date)

# get the db session
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session() as session:
        yield session

# fastapi app routes
"""
This endpoint just reutns a health check for the API.
"""
@app.get("/health")
async def health_check():
    return {"status": "ok"}
"""
This endpoint retrieves a list of PPP loans from the database.
"""
@app.get("/loans", response_model=List[PPPLoanDataSchema])
async def get_loans(limit: int = 10, session: AsyncSession = Depends(get_session)):
    stmt = select(PPPLoanData).limit(limit)
    result = await session.execute(stmt)
    return result.scalars().all()
"""
This endpoint allows you to search for PPP loans by borrower name. 
Returns a empty list if no loans are found under that borrower name.
Returns the list of loans if found.
"""
@app.get("/loans/search/by-borrower", response_model=List[PPPLoanDataSchema])
async def search_loans_by_borrower(
    borrower_name: str,
    session: AsyncSession = Depends(get_session),
):
    stmt = select(PPPLoanData).where(PPPLoanData.borrower_name.ilike(f"%{borrower_name}%"))
    result = await session.execute(stmt)
    return result.scalars().all()
"""
This endpoint allows you to search for PPP loans by loan number.
Raises an HTTPException if no loans are found under that loan number.
Returns a single loan if found.
"""
@app.get("/loans/search/by-loan-number", response_model=PPPLoanDataSchema)
async def search_loans_by_loan_number(
    loan_number: str,
    session: AsyncSession = Depends(get_session),
):
    stmt = select(PPPLoanData).where(PPPLoanData.loan_number == loan_number)
    result = await session.execute(stmt)
    loan = result.scalar_one_or_none()
    if loan is None:
        raise HTTPException(status_code=404, detail="Loan not found in the database.")
    return loan

"""
This endpoint allows you to search for PPP loans by date range.
Returns an empty list if no loans are found within that date range.
Returns the list of loans if found.
"""
@app.get("/loans/search/by-date-range", response_model=List[PPPLoanDataSchema])
async def search_loans_by_date_range(
    start_date: date,
    end_date: date,
    session: AsyncSession = Depends(get_session),
):
    stmt = select(PPPLoanData).where(
        PPPLoanData.date_approved >= start_date,
        PPPLoanData.date_approved <= end_date,
    )
    result = await session.execute(stmt)
    return result.scalars().all()

"""
This endpoint allows you to search for PPP loans by forgiveness amount.
Returns an empty list if no loans are found with a forgiveness amount greater than or equal to the specified amount.
Returns the list of loans if found.
"""
@app.get("/loans/search/by-forgiveness-amount", response_model=List[PPPLoanDataSchema])
async def search_loans_by_forgiveness_amount(
    min_forgiveness_amount: float,
    limti: int = 5,
    session: AsyncSession = Depends(get_session),
):
    stmt = ( select(PPPLoanData).where(
            PPPLoanData.forgiveness_amount != None,
            PPPLoanData.forgiveness_amount >= min_forgiveness_amount,
        )
        .limit(limti)
    )
    result = await session.execute(stmt)
    return result.scalars().all()

"""
This endpoint retrieves the top 10 borrowers with the highest forgiveness amounts.
Returns an empty list if no loans are found with a forgiveness amount.
Returns a list of loans with the highest forgiveness amounts.
"""
@app.get("/loans/top-borrowers", response_model=List[PPPLoanDataSchema])
async def get_top_borrowers(session: AsyncSession = Depends(get_session)):
    stmt = (
        select(PPPLoanData)
        .where(PPPLoanData.forgiveness_amount != None)
        .order_by(PPPLoanData.forgiveness_amount.desc())
        .limit(10)
    )
    result = await session.execute(stmt)
    return result.scalars().all()

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8001))
    uvicorn.run(app, host="0.0.0.0", port=port)