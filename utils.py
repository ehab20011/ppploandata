import os
import io
import re
import csv
import time
import uuid
import logging
import psycopg2
import traceback

import datetime as dt
from enum import Enum
from dataclasses import dataclass
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    TYPE_CHECKING,
)

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.value_provider import ValueProvider
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    WorkerOptions,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    computed_field,
    model_validator,
)
from typing_extensions import Self

# type checking for circular imports
if TYPE_CHECKING:
    from models import PPPLoanDataSchema

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ErrorType(str, Enum):
    """Enumeration of error types for error reporting.

    Attributes:
        Parsing (str): Parsing-related errors.
        Processing (str): Processing-related errors.
        Validation (str): Validation-related errors.
        Blanket (str): General or unspecified errors.
    """

    Parsing = "parsing"
    Processing = "processing"
    Validation = "validation"
    Blanket = "blanket"

class DecodeCP1252(beam.DoFn):
    def process(self, element: bytes):
        yield element.decode("cp1252")
        
class SerializableBaseModel(BaseModel):
    """Base model with serialization capabilities for CSV output.

    Attributes:
        exclude_from_serialization (ClassVar[Set[str]]): Fields to exclude from serialization. Defaults to set().

    Methods:
        extract_element_values: Extracts field values as strings.
        to_csv_line: Converts the model to a CSV line.
        get_csv_header: Generates the CSV header from model fields.
    """

    model_config = ConfigDict(from_attributes=True)
    exclude_from_serialization: ClassVar[Set[str]] = set()

    def extract_element_values(
        self, columns_to_serialize: Optional[List[str]] = None
    ) -> List[str]:
        """Extracts field values for serialization.

        Returns:
            List[str]: List of string values for CSV serialization.
        """
        model_fields = self.model_fields
        if columns_to_serialize:
            columns = columns_to_serialize
        else:
            columns = [
                col
                for col in model_fields.keys()
                if col not in self.exclude_from_serialization
            ]

        element = self.model_dump()
        row_values = []

        for col in columns:
            raw_val = element.get(col, "")
            if col == "element" and isinstance(raw_val, dict):
                # Convert dictionary to a JSON string and escape it
                import json

                value_str = json.dumps(raw_val, default=str)
            elif isinstance(raw_val, Enum):
                value_str = raw_val.value
            elif isinstance(raw_val, (dt.datetime, dt.date)):
                value_str = raw_val.isoformat() if raw_val else ""
            elif raw_val is None:
                value_str = ""
            else:
                value_str = str(raw_val)
            row_values.append(value_str)

        if hasattr(self, "shard_id") and not columns_to_serialize:
            row_values.append(str(self.shard_id))

        return row_values

    def to_csv_line(self) -> str:
        """Converts the model to a CSV-formatted string.

        Returns:
            str: CSV line representation of the model.
        """
        output = io.StringIO()
        writer = csv.writer(
            output,
            quoting=csv.QUOTE_MINIMAL,
            delimiter=",",
            quotechar='"',
            lineterminator="\n",
        )
        writer.writerow(self.extract_element_values())
        return output.getvalue().strip()

    @classmethod
    def get_csv_header(cls) -> str:
        """Generates the CSV header from model fields.

        Returns:
            str: CSV header string.
        """
        model_fields = cls.model_fields
        columns = sorted(model_fields.keys())
        columns = [col for col in columns if col not in cls.exclude_from_serialization]
        output = io.StringIO()
        writer = csv.writer(
            output,
            quoting=csv.QUOTE_MINIMAL,
            delimiter=",",
            quotechar='"',
            lineterminator="\n",
        )
        writer.writerow(columns)
        return output.getvalue().strip()
    
class ValidatedBaseModel(SerializableBaseModel):
    """Base model with validation for required, combined, and normalized fields.

    Provides common validation logic for subclasses to enforce field requirements.

    Attributes:
        required_fields (ClassVar[List[str]]): Fields that must be non-empty. Defaults to [].
        combined_fields (ClassVar[Set[str]]): Fields where at least one must be non-empty. Defaults to set().
        normalized_fields (ClassVar[Set[str]]): Fields that must be valid after normalization. Defaults to set().
        normalized_combined_fields (ClassVar[Set[str]]): Fields where at least one must be valid after normalization. Defaults to set().

    Raises:
        ValueError: If validation fails for any field constraints.
    """

    # Class variables to be overridden by child classes
    required_fields: ClassVar[List[str]] = []
    combined_fields: ClassVar[Set[str]] = set()
    normalized_fields: ClassVar[Set[str]] = set()
    normalized_combined_fields: ClassVar[Set[str]] = set()

    @model_validator(mode="after")
    def check_required_fields(self) -> Self:
        for field in self.required_fields:
            if not bool(str(getattr(self, field, "")).strip()):
                raise ValueError(f"Required field '{field}' cannot be empty")
        return self

    @model_validator(mode="after")
    def check_combined_fields(self) -> Self:
        if self.combined_fields:
            if not any(
                bool(str(getattr(self, field, "")).strip())
                for field in self.combined_fields
            ):
                raise ValueError(
                    f"At least one of these fields must be non-empty: {self.combined_fields}"
                )
        return self

    @model_validator(mode="after")
    def check_normalized_fields(self) -> Self:
        for field in self.normalized_fields:
            value = str(getattr(self, field, ""))
            if not bool(normalize(value)):
                raise ValueError(f"Field '{field}' is invalid after normalization")
        return self

    @model_validator(mode="after")
    def check_normalized_combined_fields(self) -> Self:
        if self.normalized_combined_fields:
            if not any(
                bool(normalize(str(getattr(self, field, ""))))
                for field in self.normalized_combined_fields
            ):
                raise ValueError(
                    f"At least one of these fields must be valid after normalization: {self.normalized_combined_fields}"
                )
        return self
       
class ErrorSchema(SerializableBaseModel):
    """Schema for error reporting.

    Attributes:
        element (Union[str, Dict, Tuple, SOS schemas]): The data causing the error.
        error_type (ErrorType): Type of error.
        error_message (Optional[str]): Error description. Defaults to None.
        stack_trace (Optional[str]): Stack trace. Defaults to None.
        sos_id (Optional[uuid.UUID]): Related SOS ID. Defaults to None.

    Computed Fields:
        shard_id (int): Computed shard ID based on element hash.
    """

    model_config = ConfigDict(from_attributes=True)
    element: Union[
        str,
        Dict[str, Any],
        Tuple[str, Dict[str, List[Dict[str, Any]]]],
        "PPPLoanDataSchema",
    ]
    error_type: ErrorType
    error_message: Optional[str] = None
    stack_trace: Optional[str] = None
    sos_id: Optional[uuid.UUID] = None

    @computed_field
    def shard_id(self) -> int:
        """
        Compute shard_id based on the hash of element.
        Handles all possible element types.
        """
        try:
            from models import PPPLoanDataSchema  # imported only once code runs
        except ImportError:                       # this should never happen but fall back gracefully just incase
            PPPLoanDataSchema = None              

        elem = self.element

        # 1) Dictionaries
        if isinstance(elem, dict):
            return abs(hash(tuple(sorted(elem.keys()))))

        # 2) Pydantic PPP rows (only if class successfully imported)
        if PPPLoanDataSchema and isinstance(elem, PPPLoanDataSchema):
            if getattr(elem, "id", None) is not None:
                return abs(hash(str(elem.id)))
            return abs(hash(elem))

        # 3) Raw CSV line
        if isinstance(elem, str):
            return abs(hash(elem))

        # 4) CoGroupByKey output
        if isinstance(elem, tuple) and len(elem) == 2:
            return abs(hash(elem[0]))

        # 5) Fallback – stringify then hash
        try:
            return abs(hash(str(elem)))
        except Exception:
            return abs(hash(id(elem)))
            
class ConvertToPydantic(beam.DoFn):
    def __init__(self, model: BaseModel):
        self.model = model
        self.valid_counter = Metrics.counter(
            "Convert to Pydantic", f"{self.model.__name__} Pydantic valid"
        )
        self.error_counter = Metrics.counter(
            "Convert to Pydantic", f"{self.model.__name__} Pydantic errors"
        )

    def process(self, line):
        try:
            input_buffer = io.StringIO(line)
            reader = csv.reader(
                input_buffer,
                delimiter=self.model.delimiter
                if hasattr(self.model, "delimiter")
                else ",",
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
            )
            fields = next(reader)

            # Check if all fields in the model have aliases
            all_fields_have_aliases = all(
                field.alias is not None for field in self.model.model_fields.values()
            )

            if all_fields_have_aliases:
                # Alias-based logic (partial construction, no column length check)
                row_data = {
                    header: field_value.strip() or ""
                    for field_value, header in zip(fields, self.model.columns)
                }
                # Validate with Pydantic using aliases, allowing partial data
                validated_model = self.model.model_validate(row_data, strict=False)
                yield validated_model
                self.valid_counter.inc()
            else:
                # Name-based logic (requires matching column lengths)
                if len(fields) != len(self.model.columns):
                    raise ValueError(
                        f"Field count mismatch: expected {len(self.model.columns)}, got {len(fields)}"
                    )
                row_data = {
                    col: val.strip() or ""
                    for col, val in zip(self.model.columns, fields)
                }
                # Strict validation by field names
                validated_model = self.model.model_validate(row_data)
                yield validated_model
                self.valid_counter.inc()
        except Exception as e:
            logger.error(
                f"Error converting to Pydantic. \n Exception {e} \n Stacktrace: {traceback.format_exc()} \n Line: {line} \n Model: {self.model}"
            )
            self.error_counter.inc()
            yield beam.pvalue.TaggedOutput(
                "errors",
                ErrorSchema(
                    element=line,
                    error_type=ErrorType.Parsing,
                    error_message=str(e),
                    stack_trace=traceback.format_exc(),
                ),
            )
class WritePPPLoanDataToPostgresAndCSV(beam.PTransform):
    def __init__(self, pipeline_options):
        super().__init__()
        self.pipeline_options = pipeline_options

    def expand(self, pcoll):
        loan_columns = [
            "loan_number",
            "date_approved",
            "borrower_name",
            "sba_office_code",
            "processing_method",
            "borrower_address",
            "borrower_city",
            "borrower_state",
            "borrower_zip",
            "loan_status_date",
            "loan_status",
            "term",
            "sba_guaranty_percentage",
            "initial_approval_amount",
            "current_approval_amount",
            "undisbursed_amount",
            "franchise_name",
            "servicing_lender_location_id",
            "servicing_lender_name",
            "servicing_lender_address",
            "servicing_lender_city",
            "servicing_lender_state",
            "servicing_lender_zip",
            "rural_urban_indicator",
            "hubzone_indicator",
            "lmi_indicator",
            "business_age_description",
            "project_city",
            "project_county_name",
            "project_state",
            "project_zip",
            "cd",
            "jobs_reported",
            "naics_code",
            "race",
            "ethnicity",
            "utilities_proceed",
            "payroll_proceed",
            "mortgage_interest_proceed",
            "rent_proceed",
            "refinance_eidl_proceed",
            "health_care_proceed",
            "debt_interest_proceed",
            "business_type",
            "originating_lender_location_id",
            "originating_lender",
            "originating_lender_city",
            "originating_lender_state",
            "gender",
            "veteran",
            "non_profit",
            "forgiveness_amount",
            "forgiveness_date",
        ]

        error_columns = [
            "element",
            "error_type",
            "error_message",
            "stack_trace",
            "shard_id",
        ]

        loan_header = ",".join(loan_columns)
        error_header = ",".join(error_columns)

        # --- Main Data ---
        written_to_postgres = (
            pcoll["loans"]
            | "Shard PPP Loans"
            >> beam.Map(
                lambda x: (x.shard_id % self.pipeline_options.num_shards.get(), x)
            )
            | "GroupByShard PPP Loans" >> beam.GroupByKey()
            | "Write PPP Loans to Postgres"
            >> beam.ParDo(
                CopyToPostgresFn(
                    pipeline_options=self.pipeline_options,
                    table_name=self.pipeline_options.ppp_loan_table,
                    columns=loan_columns,
                )
            )
        )

        _ = (
            pcoll["loans"]
            | "Format PPP Loans CSV" >> beam.Map(lambda x: x.to_csv_line())
            | "Write PPP Loans CSV"
            >> beam.io.WriteToText(
                self.pipeline_options.ppp_loan_data_csv,
                shard_name_template="",
                header=loan_header,
            )
        )

        # --- Errors ---
        _ = (
            pcoll["errors"]
            | "Shard PPP Errors"
            >> beam.Map(
                lambda x: (x.shard_id % self.pipeline_options.num_shards.get(), x)
            )
            | "GroupByShard PPP Errors" >> beam.GroupByKey()
            | "Write PPP Errors to Postgres"
            >> beam.ParDo(
                CopyToPostgresFn(
                    pipeline_options=self.pipeline_options,
                    table_name=self.pipeline_options.ppp_loan_error_table,
                    columns=error_columns,
                )
            )
        )

        _ = (
            pcoll["errors"]
            | "Format PPP Errors CSV" >> beam.Map(lambda x: x.to_csv_line())
            | "Write PPP Errors CSV"
            >> beam.io.WriteToText(
                self.pipeline_options.ppp_loan_error_csv,
                shard_name_template="",
                header=error_header,
            )
        )

        return None

class ReadAndMapToPydanticSingle(beam.PTransform):
    def __init__(
        self,
        model: BaseModel,
        file_path: ValueProvider,
        replace_strings_with: Optional[List[str]] = None,
    ):
        """Initialize the transform with an optional list of strings to replace.

        Args:
            model (BaseModel): Pydantic model to parse data into.
            file_path (ValueProvider): File path to read from.
            replace_strings_with (Optional[List[str]]): If provided, replace all occurrences
                of these strings with an empty string. If None, no replacement is performed.
                Defaults to None.
        """
        self.model = model
        self.file_path = file_path
        self.replace_strings_with = replace_strings_with

    def expand(self, p):
        pipeline = (
            p
            | "Read CP1252 Bytes" >> beam.io.ReadFromText(
                file_pattern=self.file_path,
                coder=beam.coders.BytesCoder()
            )
            | "Decode CP1252 to String" >> beam.ParDo(DecodeCP1252())
        )

        # Conditionally add the string replacement step
        if self.replace_strings_with is not None:

            def replace_multiple(line: str) -> str:
                result = line
                for unwanted in self.replace_strings_with:
                    result = result.replace(unwanted, "")
                return result

            pipeline = pipeline | "Clean Unwanted Strings" >> beam.Map(replace_multiple)

        # Continue with conversion to Pydantic
        pipeline = pipeline | "Convert to Pydantic" >> beam.ParDo(
            ConvertToPydantic(self.model)
        ).with_outputs("errors", main="valid")

        return pipeline
class CopyToPostgresFn(beam.DoFn):
    """Writes data to PostgreSQL using COPY command.

    Args:
        pipeline_options (PipelineOptions): Pipeline configuration.
        table_name (str): Target table name.
        columns (List[str]): Column names for the table.
    """

    def __init__(
        self, pipeline_options: PipelineOptions, table_name: Any, columns: List[str]
    ) -> None:
        self.pipeline_options = pipeline_options
        self.table_name = table_name
        self.columns = columns

    def setup(self) -> None:
        """Initializes database connection."""
        self.db_host = self.pipeline_options.db_host.get()
        self.db_port = self.pipeline_options.db_port.get()
        self.db_name = self.pipeline_options.db_name.get()
        self.db_user = self.pipeline_options.db_user.get()
        self.db_password = self.pipeline_options.db_password.get()
        self.postgres_retry_delay = self.pipeline_options.postgres_retry_delay.get()
        self.postgres_num_retries = self.pipeline_options.postgres_num_retries.get()
        self.resolved_table_name = self.table_name.get()
        self.num_shards = self.pipeline_options.num_shards.get()
        self._connect()

    def _connect(self) -> None:
        self.conn = psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
        )
        self.cursor = self.conn.cursor()

    def _cleanup_shard(self, shard_id: str) -> None:
        self.cursor.execute(
            f"DELETE FROM {self.resolved_table_name} WHERE shard_id = %s", (shard_id,)
        )
        self.conn.commit()

    def process(self, element):
        """Writes a batch of elements to PostgreSQL.

        Args:
            element (Tuple[str, List]): Shard ID and list of elements.

        Yields:
            int: Number of rows written or 0 if empty.
        """
        _, elements = element
        temp_file_path = f"/tmp/{uuid.uuid4()}.csv"
        rows_written = 0

        try:
            with open(temp_file_path, "w", newline="") as temp_file:
                writer = csv.writer(
                    temp_file, quoting=csv.QUOTE_MINIMAL, delimiter=",", quotechar='"'
                )
                for element in elements:
                    logging.info(
                        f"Writing element to {self.resolved_table_name}: {element}"
                    )
                    row_values = element.extract_element_values(self.columns)
                    writer.writerow(row_values)
                    rows_written += 1

            if rows_written == 0:
                yield 0
                return

            attempts = 0
            while attempts <= self.postgres_num_retries:
                try:
                    with open(temp_file_path, "r") as temp_file:
                        copy_sql = f"""
                        COPY {self.resolved_table_name} ({', '.join(self.columns)})
                        FROM STDIN WITH (FORMAT CSV, DELIMITER ',', QUOTE '"')
                        """
                        self.cursor.copy_expert(copy_sql, temp_file)
                        self.conn.commit()
                    yield rows_written
                    break
                except Exception as insert_error:
                    self.conn.rollback()
                    attempts += 1
                    if attempts > self.postgres_num_retries:
                        raise RuntimeError(
                            f"Failed to COPY {rows_written} records into {self.resolved_table_name} "
                            f"after {attempts} attempts: {insert_error}"
                        )
                    time.sleep(self.postgres_retry_delay)
                    self._connect()
        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    def teardown(self) -> None:
        """Closes database connection."""
        self.cursor.close()
        self.conn.close()
################################################################################
#                                   HELPERS                                    #
################################################################################
def configure_baselayer_pipeline_options(opts: PipelineOptions) -> None:
    """Mutates `opts` in-place with our standard Dataflow settings."""
    setup_options = opts.view_as(SetupOptions)
    worker_options = opts.view_as(WorkerOptions)

    setup_options.save_main_session = True
    worker_options.disk_size_gb = 100
    worker_options.autoscaling_algorithm = "NONE"
    worker_options.machine_type = "n2-custom-32-65536"

    PROJECT_ID = opts.get_all_options()["project"]
    worker_options.disk_type = f"compute.googleapis.com/projects/{PROJECT_ID}/zones/us-central1/diskTypes/pd-ssd"
    worker_options.num_workers = 32 if PROJECT_ID == "osiris-app-staging" else 4

def normalize(name: Union[str, None]) -> Union[str, None]:
    """Normalizes a string by lowercasing and cleaning it.

    Args:
        name (Union[str, None]): String to normalize.

    Returns:
        Union[str, None]: Normalized string or None if empty/None.
    """
    # Handle None or empty string
    if name is None:
        return None
    name = name.strip()
    if not name:
        return None

    # Convert to lowercase
    normalized = name.lower()

    # Replace hyphens, ampersands, and underscores with spaces
    normalized = re.sub(r"[-&_]", " ", normalized)

    # Remove all non-word characters except spaces and apostrophes
    normalized = re.sub(r"[^\w\s\']", "", normalized)

    # Replace multiple spaces (whitespace) with a single space
    normalized = re.sub(r"\s+", " ", normalized)

    # Final trim
    normalized = normalized.strip()

    # Return None if the result is empty
    return normalized if normalized else None

def concatenate_fields(
    element: BaseModel, fields: List[str], join_key: str
) -> Optional[str]:
    """Concatenates fields from a model with a join key.

    Args:
        element (BaseModel): Model instance with fields.
        fields (List[str]): Field names to concatenate.
        join_key (str): String to join fields with.

    Returns:
        Optional[str]: Concatenated string or None if empty.
    """
    # Extract field values, handling enums, None and default cases
    join_key = ", "
    parts = []
    for field in fields:
        value = getattr(element, field, "")
        if isinstance(value, Enum):
            parts.append(value.value)
        elif value is None or not value.strip() or value.strip().upper() == "NONE":
            continue
        else:
            parts.append(str(value))

    # Join non-empty parts after stripping whitespace
    result = join_key.join(part.strip() for part in parts)
    return result if result else None
################################################################################
#                             BASE TEMPLATE OPTIONS                            #
################################################################################
@dataclass
class TemplateOption:
    name: str
    type: Type
    help: str

def construct_template_options(additional_args: Optional[List[TemplateOption]] = None):
    class TemplateOptions(PipelineOptions):
        """Custom options class for Dataflow classic template runtime parameters."""

        @classmethod
        def _add_argparse_args(cls, parser):
            if additional_args:
                for arg in additional_args:
                    parser.add_value_provider_argument(
                        "--" + arg.name,
                        type=arg.type,
                        help=arg.help,
                    )
            # Configuration
            parser.add_value_provider_argument(
                "--gcp_project_id", type=str, help="Google Cloud project ID"
            )
            # Database configuration
            parser.add_value_provider_argument(
                "--db_host", type=str, help="PostgreSQL database host address"
            )
            parser.add_value_provider_argument(
                "--db_port", type=str, help="PostgreSQL database port number"
            )
            parser.add_value_provider_argument(
                "--db_name", type=str, help="PostgreSQL database name"
            )
            parser.add_value_provider_argument(
                "--db_user", type=str, help="PostgreSQL database user (secret)"
            )
            parser.add_value_provider_argument(
                "--db_password", type=str, help="PostgreSQL database password (secret)"
            )
            parser.add_value_provider_argument(
                "--redis_host",
                type=str,
                default="10.62.17.4",
                help="Redis server host address for distributed semaphore",
            )
            parser.add_value_provider_argument(
                "--redis_port", type=int, default=6379, help="Redis server port number"
            )
            parser.add_value_provider_argument(
                "--redis_db", type=int, default=0, help="Redis database index"
            )
            parser.add_value_provider_argument(
                "--redis_lock_count",
                type=int,
                default=1,
                help="Number of locks to create in Redis",
            )
            parser.add_value_provider_argument(
                "--postgres_retry_delay",
                type=int,
                default=5,
                help="Delay in seconds between PostgreSQL COPY retry attempts",
            )
            parser.add_value_provider_argument(
                "--postgres_num_retries",
                type=int,
                default=0,
                help="Maximum number of retry attempts for PostgreSQL COPY operations",
            )
            parser.add_value_provider_argument(
                "--ppp_loan_data_input_path",
                type=str,
                help="Path to raw PPP loan data CSV",
            )

            # Output CSV paths
            parser.add_value_provider_argument(
                "--ppp_loan_data_csv",
                type=str,
                help="GCS path for processed ppp loan data CSV",
            )
            parser.add_value_provider_argument(
                "--ppp_loan_error_csv",
                type=str,
                help="GCS path for ppp loan data errors CSV",
            )

            # Postgres table names
            parser.add_value_provider_argument(
                "--ppp_loan_table",
                type=str,
                help="Postgres table for PPP loan data",
            )
            parser.add_value_provider_argument(
                "--ppp_loan_error_table",
                type=str,
                help="Postgres table for ppp loan errors",
            )

            # Sharding
            parser.add_value_provider_argument(
                "--num_shards",
                type=int,
                help="Number of shards for parallel COPY to Postgres",
            )

    return TemplateOptions

################################################################################
#                 Conditional Model Import                                     #
################################################################################
try:
    # late import avoids circular-import problems
    from models import PPPLoanDataSchema  # noqa: E402
except ImportError:
    PPPLoanDataSchema = None  # happens only during unit-tests that skip models

if PPPLoanDataSchema is not None:
    ErrorSchema.model_rebuild()