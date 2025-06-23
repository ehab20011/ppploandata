import datetime

from typing import List, Dict, Any, ClassVar, Optional
from pydantic import ConfigDict, Field, computed_field, field_validator
from utils import (
    ValidatedBaseModel,
    normalize,
    concatenate_fields
)

# -- PPP CONSTANTS AND MAPS --- #
DATE_FORMATS = [
    "%m/%d/%Y",   # 05/01/2020
    "%-m/%-d/%Y", # 6/1/2020
]

class PPPLoanDataSchema(ValidatedBaseModel):
    """Schema for PPP Loan Data."""
    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
    )

    delimiter: ClassVar[str] = ","
    columns: ClassVar[List[str]] = [
        "LoanNumber", "DateApproved", "SBAOfficeCode", "ProcessingMethod", "BorrowerName", "BorrowerAddress", "BorrowerCity",
        "BorrowerState", "BorrowerZip", "LoanStatusDate", "LoanStatus", "Term", "SBAGuarantyPercentage", "InitialApprovalAmount",
        "CurrentApprovalAmount", "UndisbursedAmount", "FranchiseName", "ServicingLenderLocationID", "ServicingLenderName",
        "ServicingLenderAddress", "ServicingLenderCity", "ServicingLenderState", "ServicingLenderZip", "RuralUrbanIndicator",
        "HubzoneIndicator", "LMIIndicator", "BusinessAgeDescription", "ProjectCity", "ProjectCountyName", "ProjectState",
        "ProjectZip", "CD", "JobsReported", "NAICSCode", "Race", "Ethnicity", "UTILITIES_PROCEED", "PAYROLL_PROCEED",
        "MORTGAGE_INTEREST_PROCEED", "RENT_PROCEED", "REFINANCE_EIDL_PROCEED", "HEALTH_CARE_PROCEED", "DEBT_INTEREST_PROCEED",
        "BusinessType", "OriginatingLenderLocationID", "OriginatingLender", "OriginatingLenderCity", "OriginatingLenderState",
        "Gender", "Veteran", "NonProfit", "ForgivenessAmount", "ForgivenessDate"
    ]

    loan_number: str = Field(..., alias="LoanNumber", description="Unique identifier for the loan")
    date_approved: datetime.datetime = Field(..., alias="DateApproved", description="Date the loan was approved")
    borrower_name: str = Field(..., alias="BorrowerName", description="Name of the business that received the loan")

    sba_office_code: Optional[str] = Field(None, alias="SBAOfficeCode", description="SBA office code")
    processing_method: Optional[str] = Field(None, alias="ProcessingMethod", description="Method used to process the loan")

    # -- Address Fields For Borrower -- #
    borrower_address: Optional[str] = Field(None, alias="BorrowerAddress", description="Address of the borrower")
    borrower_city: Optional[str] = Field(None, alias="BorrowerCity", description="City of the borrower")
    borrower_state: Optional[str] = Field(None, alias="BorrowerState", description="State of the borrower")
    borrower_zip: Optional[str] = Field(None, alias="BorrowerZip", description="ZIP code of the borrower")
    
    loan_status_date: Optional[datetime.datetime] = Field(None, alias="LoanStatusDate", description="Date of the loan status")
    loan_status: Optional[str] = Field(None, alias="LoanStatus", description="Current status of the loan")
    term: Optional[int] = Field(None, alias="Term", description="Term of the loan in months")
    sba_guaranty_percentage: Optional[float] = Field(None, alias="SBAGuarantyPercentage", description="Percentage of the loan guaranteed by the SBA")
    initial_approval_amount: Optional[float] = Field(None, alias="InitialApprovalAmount", description="Initial amount approved for the loan")
    current_approval_amount: Optional[float] = Field(None, alias="CurrentApprovalAmount", description="Current amount approved for the loan")
    undisbursed_amount: Optional[float] = Field(None, alias="UndisbursedAmount", description="Amount that has not been disbursed")
    franchise_name: Optional[str] = Field(None, alias="FranchiseName", description="Name of the franchise")

    # -- Servicing Lender Information -- # 
    servicing_lender_location_id: Optional[str] = Field(None, alias="ServicingLenderLocationID", description="Location ID of the servicing lender")
    servicing_lender_name: Optional[str] = Field(None, alias="ServicingLenderName", description="Name of the servicing lender")
    servicing_lender_address: Optional[str] = Field(None, alias="ServicingLenderAddress", description="Address of the servicing lender")
    servicing_lender_city: Optional[str] = Field(None, alias="ServicingLenderCity", description="City of the servicing lender")
    servicing_lender_state: Optional[str] = Field(None, alias="ServicingLenderState", description="State of the servicing lender")
    servicing_lender_zip: Optional[str] = Field(None, alias="ServicingLenderZip", description="ZIP code of the servicing lender")

    rural_urban_indicator: Optional[str] = Field(None, alias="RuralUrbanIndicator", description="Indicator for rural or urban area")
    hubzone_indicator: Optional[str] = Field(None, alias="HubzoneIndicator", description="HUBZone indicator")
    lmi_indicator: Optional[str] = Field(None, alias="LMIIndicator", description="Low-to-moderate income indicator")
    business_age_description: Optional[str] = Field(None, alias="BusinessAgeDescription", description="Description of the business age")
    project_city: Optional[str] = Field(None, alias="ProjectCity", description="City of the project")
    project_county_name: Optional[str] = Field(None, alias="ProjectCountyName", description="County name of the project")
    project_state: Optional[str] = Field(None, alias="ProjectState", description="State of the project")
    project_zip: Optional[str] = Field(None, alias="ProjectZip", description="ZIP code of the project")
    cd: Optional[str] = Field(None, alias="CD", description="Community Development indicator")
    jobs_reported: Optional[int] = Field(None, alias="JobsReported", description="Number of jobs reported")
    naics_code: Optional[str] = Field(None, alias="NAICSCode", description="NAICS code")

    # -- Demographic Information -- #
    race: Optional[str] = Field(None, alias="Race", description="Race of the borrower")
    ethnicity: Optional[str] = Field(None, alias="Ethnicity", description="Ethnicity of the borrower")
    gender: Optional[str] = Field(None, alias="Gender", description="Gender of the borrower")
    veteran: Optional[str] = Field(None, alias="Veteran", description="Veteran status of the borrower")
    non_profit: Optional[str] = Field(None, alias="NonProfit", description="Whether the borrower is a non-profit organization")

    # -- Loan Proceeds -- #
    utilities_proceed: Optional[float] = Field(None, alias="UTILITIES_PROCEED", description="Loan proceeds used for utilities")
    payroll_proceed: Optional[float] = Field(None, alias="PAYROLL_PROCEED", description="Loan proceeds used for payroll")
    mortgage_interest_proceed: Optional[float] = Field(None, alias="MORTGAGE_INTEREST_PROCEED", description="Loan proceeds used for mortgage interest")
    rent_proceed: Optional[float] = Field(None, alias="RENT_PROCEED", description="Loan proceeds used for rent")
    refinance_eidl_proceed: Optional[float] = Field(None, alias="REFINANCE_EIDL_PROCEED", description="Loan proceeds used to refinance EIDL")
    health_care_proceed: Optional[float] = Field(None, alias="HEALTH_CARE_PROCEED", description="Loan proceeds used for health care")
    debt_interest_proceed: Optional[float] = Field(None, alias="DEBT_INTEREST_PROCEED", description="Loan proceeds used for debt interest")

    # -- Business and Lender Info -- #
    business_type: Optional[str] = Field(None, alias="BusinessType", description="Type of business")
    originating_lender_location_id: Optional[str] = Field(None, alias="OriginatingLenderLocationID", description="Location ID of the originating lender")
    originating_lender: Optional[str] = Field(None, alias="OriginatingLender", description="Name of the originating lender")
    originating_lender_city: Optional[str] = Field(None, alias="OriginatingLenderCity", description="City of the originating lender")
    originating_lender_state: Optional[str] = Field(None, alias="OriginatingLenderState", description="State of the originating lender")

    # -- Forgiveness -- #
    forgiveness_amount: Optional[float] = Field(None, alias="ForgivenessAmount", description="Amount of loan forgiven")
    forgiveness_date: Optional[datetime.datetime] = Field(None, alias="ForgivenessDate", description="Date of loan forgiveness")

    # -- Field Validators -- #
    @field_validator("loan_number", mode="before")
    def validate_loan_number(cls, loan_number: str) -> str:
        if not loan_number or not loan_number.strip():
            raise ValueError("Loan number cannot be empty")
        return loan_number

    @field_validator("date_approved", mode="before")
    def validate_approval_date(cls, date_approved):
        #If the date is already a datetime object, return it as is
        if isinstance(date_approved, datetime.datetime):
            return date_approved
        if not date_approved or not str(date_approved).strip():
            raise ValueError("Date cannot be empty")
        date_approved = str(date_approved).strip()
        for format in DATE_FORMATS:
            try:
                return datetime.datetime.strptime(date_approved, format)
            except ValueError:
                continue
        raise ValueError(f"Unrecognised date format: {date_approved}. Expected formats: {DATE_FORMATS}")
    
    @field_validator("forgiveness_date", "loan_status_date", mode="before")
    def validate_forgiveness_and_status_date(cls, date_being_validated):
        #If the date is already a datetime object, return it as is
        if isinstance(date_being_validated, datetime.datetime):
            return date_being_validated
        if not date_being_validated or not str(date_being_validated).strip():
            return None
        date_being_validated = str(date_being_validated).strip()
        for format in DATE_FORMATS:
            try:
                return datetime.datetime.strptime(date_being_validated, format)
            except ValueError:
                continue
        raise ValueError(f"Unrecognised date format: {date_being_validated}. Expected formats: {DATE_FORMATS}")
    
    @field_validator("borrower_name", mode="before")
    def validate_borrower_name(cls, borrower_name: str) -> str:
        if not borrower_name or not borrower_name.strip() or not normalize(borrower_name):
            raise ValueError("Borrower name cannot be empty")
        return borrower_name

    # This Field Validator is used to parse optional float values
    # It will convert empty strings or None to None, and other values to float
    @field_validator(
        "utilities_proceed",
        "payroll_proceed",
        "mortgage_interest_proceed",
        "rent_proceed",
        "refinance_eidl_proceed",
        "health_care_proceed",
        "debt_interest_proceed",
        "sba_guaranty_percentage",
        "initial_approval_amount",
        "current_approval_amount",
        "undisbursed_amount",
        "forgiveness_amount",
        mode="before",
    )
    @classmethod
    def parse_optional_float(cls, v):
        if v in ("", None):
            return None
        return float(v)
    
    # -- Computed Fields -- #
    @computed_field
    def borrower_full_address(self) -> Optional[str]:
        """Returns the full address of the borrower."""
        return concatenate_fields(
            self,
            ["borrower_address", "borrower_city", "borrower_state", "borrower_zip"],
            ", "
        )

    @computed_field
    def servicing_lender_full_address(self) -> Optional[str]:
        """Returns the full address of the servicing lender."""
        return concatenate_fields(
            self,
            ["servicing_lender_address", "servicing_lender_city", "servicing_lender_state", "servicing_lender_zip"],
            ", "
        )

    @computed_field
    def originating_lender_full_address(self) -> Optional[str]:
        """Returns the full address of the originating lender."""
        return concatenate_fields(
            self,
            ["originating_lender", "originating_lender_city", "originating_lender_state"],
            ", "
        )
    
    @computed_field
    def shard_id(self) -> int:
        """Compute shard ID based on hash of loan number."""
        return abs(hash(self.loan_number))