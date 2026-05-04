
import logging
import sys
from bronze_source_to_docker import run_source_to_docker, SourceToDockerFailed
from bronze_pipeline_orchestrator import run_bronze_pipeline, BronzePipelineFailed
from bronze_data_validation import run_bronze_validation, BronzeRowMismatch, BronzeKeyMismatch
from silver_pipeline_orchestrator import run_silver_pipeline, SilverPipelineFailed
from silver_data_validation import run_silver_validation, SilverValidationFailed
from gold_pipeline_orchestrator import run_gold_pipeline, GoldPipelineFailed
from gold_data_validation import run_gold_validation, GoldValidationFailed
from logging_config import setup_logging


def run_pipeline():
    try:
        run_source_to_docker()
        run_bronze_pipeline()
        run_bronze_validation()
        run_silver_pipeline()
        run_silver_validation()
        run_gold_pipeline()
        run_gold_validation()

        print("Pipeline completed successfully")

    except SourceToDockerFailed as e:
        logging.error(e)
        print(e)
        sys.exit(1)

    except BronzePipelineFailed as e:
        logging.error("BronzePipelineFailed: %s", e.failed_steps)
        print(f"BronzePipelineFailed: {e.failed_steps}")
        sys.exit(1)

    except BronzeRowMismatch as e:
        logging.error("BronzeRowMismatch: %s", e.row_comparison)
        print(f"BronzeRowMismatch: {e.row_comparison}")
        sys.exit(1)

    except BronzeKeyMismatch as e:
        logging.error("BronzeKeyMismatch: %s", e.key_comparison)
        print(f"BronzeKeyMismatch: {e.key_comparison}")
        sys.exit(1)

    except SilverPipelineFailed as e:
        logging.error("SilverPipelineFailed: %s", e.failed_steps)
        print(f"SilverPipelineFailed: {e.failed_steps}")
        sys.exit(1)

    except SilverValidationFailed as e:
        logging.error("SilverValidationFailed: %s", e.failed_checks)
        print(f"SilverValidationFailed: {e.failed_checks}")
        sys.exit(1)

    except GoldPipelineFailed as e:
        logging.error("GoldPipelineFailed: %s", e.failed_steps)
        print(f"GoldPipelineFailed: {e.failed_steps}")
        sys.exit(1)

    except GoldValidationFailed as e:
        logging.error("GoldValidationFailed: %s", e.failed_message)
        print(f"GoldValidationFailed: {e.failed_message}")
        sys.exit(1)

    except Exception as e:
        logging.error(e)
        print(f"Unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    setup_logging()
    run_pipeline()




