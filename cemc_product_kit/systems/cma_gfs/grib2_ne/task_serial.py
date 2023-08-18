"""
串行方式生成 grib2-ne 数据

5 - 6 分钟
"""
from pathlib import Path
from typing import Union, Optional

from tqdm.auto import tqdm

from cemc_product_kit.logging import get_logger
from cemc_product_kit.systems.cma_gfs.grib2_ne.common import get_message_bytes
from cemc_product_kit.utils import cal_run_time, get_message_count


logger = get_logger()


@cal_run_time
def make_grib2_ne_by_serial(
        input_file_path: Union[Path, str],
        start_longitude: Union[float, int],
        end_longitude: Union[float, int],
        longitude_step: Optional[Union[float, int]],
        start_latitude: Union[float, int],
        end_latitude: Union[float, int],
        latitude_step: Optional[Union[float, int]],
        output_file_path: Union[Path, str]
):

    logger.info("count...")
    total_count = get_message_count(input_file_path)
    logger.info("count..done")

    logger.info("process...")
    with open(output_file_path, "wb") as f:
        for i in tqdm(range(1, total_count+1)):
            message_bytes = get_message_bytes(
                input_file_path,
                start_longitude=start_longitude,
                end_longitude=end_longitude,
                longitude_step=longitude_step,
                start_latitude=start_latitude,
                end_latitude=end_latitude,
                latitude_step=latitude_step,
                count=i,
            )
            f.write(message_bytes)
            del message_bytes
    logger.info("process...done")
