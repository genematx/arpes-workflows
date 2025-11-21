from prefect import task, flow, get_run_logger
from data_validation import data_validation_flow

@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")

@flow(log_prints=True)
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]
    data_validation_flow(uid)
    log_completion()
