"""
串行方式生成 grib2-split 数据

5 - 6 分钟
"""
from pathlib import Path
from typing import Union, Optional
from contextlib import ExitStack

from tqdm.auto import tqdm

from cemc_product_kit.logging import get_logger
from cemc_product_kit.utils import cal_run_time, get_message_count
from cemc_product_kit.systems.cma_meso.grib2_split.config import Config, Area
from cemc_product_kit.systems.cma_meso.grib2_split.common import get_message_bytes_list, get_output_file_name


logger = get_logger()


@cal_run_time
def split_grib2_by_serial(
        input_file_path: Union[Path, str],
        output_path: Union[Path, str],
        config: Config,
):
    logger.info("count...")
    total_count = get_message_count(input_file_path)
    logger.info("count..done")

    area_count = len(config.areas)

    file_paths = [Path(output_path, get_output_file_name(area)) for area in config.areas]

    logger.info("process...")
    with ExitStack() as stack:
        files = [stack.enter_context(open(file_path, "wb")) for file_path in file_paths]
        for i in tqdm(range(1, total_count+1)):
            message_bytes_list = get_message_bytes_list(
                input_file_path,
                count=i,
                areas=config.areas,
            )
            logger.info("writing to files...")
            for index in range(0, area_count):
                files[index].write(message_bytes_list[index])
            logger.info("writing to files...done")
            del message_bytes_list
    logger.info("process...done")
