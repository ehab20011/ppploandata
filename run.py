import os
import sys
import configparser
import logging

import apache_beam as beam
from models import PPPLoanDataSchema
from typing import List, Optional

from sql import CreateTempTables

from utils import (
    ReadAndMapToPydanticSingle,
    TemplateOption,
    WritePPPLoanDataToPostgresAndCSV,
    normalize,
    construct_template_options,
    configure_baselayer_pipeline_options,
)

##########################################
#           LOCAL ONLY                   #
##########################################
logging.basicConfig(level=logging.INFO)
def config_to_argv(config_file: str) -> List[str]:
    if not os.path.exists(config_file):
        logging.warning(f"Config file {config_file} not found. Using default arguments.")
        return []

    config = configparser.ConfigParser()
    config.read(config_file)

    existing_arg_keys = {arg for arg in sys.argv if arg.startswith("--")}
    protected_keys: set[str] = set()

    argv: list[str] = []
    for section in config.sections():
        for key, value in config.items(section):
            arg_key = f"--{key}"
            if arg_key in protected_keys:
                logging.info(f"⚠️  Skipping protected key from config: {arg_key}")
                continue
            if arg_key in existing_arg_keys:
                logging.info(f"⚠️  Skipping duplicate cli arg: {arg_key}")
                continue
            logging.info(f"✅ Adding arg from config: {arg_key}={value}")
            argv.extend([arg_key, value])
    return argv
    return None
################################################################################
#                          PROCESSING FUNCTION                                 #
################################################################################
class ProcessPPPLoanDataDoFn(beam.DoFn):
    def process(self, loan: PPPLoanDataSchema):
        if not loan.loan_number or not loan.date_approved or not loan.borrower_name or not normalize(loan.borrower_name):
            return

        yield loan
################################################################################
#                              PIPELINE RUNNER                                 #
################################################################################
def run(argv: Optional[List[str]] = None) -> None:
    """
    Main pipeline execution for CALIFORNIA SOS data processing.
    """
    TemplateOptions = construct_template_options()
    pipeline_options = TemplateOptions(flags=argv)
    configure_baselayer_pipeline_options(pipeline_options)
    with beam.Pipeline(options=pipeline_options) as p:
        ########################################################################
        #                           PARSING STEP                               #
        ########################################################################
        ppp_loan_data = p | "Read Entities" >> ReadAndMapToPydanticSingle(
            model=PPPLoanDataSchema, file_path=pipeline_options.ppp_loan_data_input_path
        )
        ################################################################################
        #                           PROCESSING STEPS                                   #
        ################################################################################   
        processed_loan_data = (
            ppp_loan_data.valid
            | "Process PPP Loan Data" >> beam.ParDo(ProcessPPPLoanDataDoFn())
        )
        ################################################################################
        #                           PARSING ERRORS                                     #
        ################################################################################
        parsing_errors = [
            ppp_loan_data.errors,
        ] | "Flatten Errors" >> beam.Flatten()
        ################################################################################
        #                           OUTPUT STEPS                                       #
        ################################################################################
        (
            {
                "loans": processed_loan_data,
                "errors": parsing_errors,
            }
            | "Write to Postgres/CSV" >> WritePPPLoanDataToPostgresAndCSV(pipeline_options)
        )
        
def main():
    full_argv = sys.argv[1:] + config_to_argv("config.conf")

    if CreateTempTables().create_tables():
        run(full_argv)
    else:
        logging.error("Table creation failed; aborting pipeline run.")

if __name__ == "__main__":
    main()
