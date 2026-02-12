import time

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret

from bluesky_tiled_plugins.writing.validator import validate
from tiled.client import from_profile


@task(retries=3, retry_delay_seconds=20)
def data_validation_task(uid, beamline_acronym="arpes"):
    logger = get_run_logger()

    api_key = Secret.load(f"tiled-{beamline_acronym}-api-key", _sync=True).get()
    tiled_client = from_profile("nsls2", api_key=api_key)

    logger.info(f"Connecting to Tiled client for beamline '{beamline_acronym}'")
    run_client = tiled_client[f"{beamline_acronym}/migration"][uid]

    logger.info(f"Validating uid {uid}")
    start_time = time.monotonic()
    validate(run_client, fix_errors=True, try_reading=True, raise_on_error=True)
    elapsed_time = time.monotonic() - start_time
    logger.info(f"Finished validating data; {elapsed_time = }")


@flow(log_prints=True)
def data_validation_flow(uid):
    data_validation_task(uid)
