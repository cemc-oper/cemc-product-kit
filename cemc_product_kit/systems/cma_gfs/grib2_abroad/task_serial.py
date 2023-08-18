from pathlib import Path
from typing import Union

from tqdm.auto import tqdm

from cemc_product_kit.logging import get_logger
from cemc_product_kit.utils import cal_run_time
from cemc_product_kit.systems.cma_gfs.grib2_abroad.common import get_parameters, get_message_bytes


logger = get_logger()


@cal_run_time
def make_grib2_abroad_by_serial(
        input_file_path: Union[Path, str],
        output_file_path: Union[Path, str]
):
    logger.info("program begin")
    parameters = get_parameters()

    with open(output_file_path, "wb") as f:
        for p in tqdm(parameters):
            b = get_message_bytes(input_file_path, p)
            f.write(b)
    logger.info("program done")
