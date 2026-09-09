from typing import Union, Optional, List
from pathlib import Path

import eccodes
import numpy as np

from reki.format.grib.eccodes import load_message_from_file
from reki.format.grib.eccodes.operator import extract_region

from cemc_product_kit.systems.cma_meso.grib2_split.config import Area
from cemc_product_kit.logging import get_logger
from cemc_product_kit.operator import extract_region


logger = get_logger()


def get_message_bytes_list(
        file_path: Union[str, Path],
        count: int,
        areas: List[Area],
) -> List[bytes]:
    message = load_message_from_file(file_path, count=count)
    # logger.info(f"decoding {count}...")
    values = eccodes.codes_get_double_array(message, "values")
    # logger.info(f"decoding {count}...done")
    message_bytes_list = []
    for area in areas:
        # logger.info(f"area: {area.name}")
        # logger.info(f"cloning message...")
        area_message = eccodes.codes_clone(message)
        # logger.info(f"get message bytes...")
        message_bytes = get_message_bytes(
            area_message,
            start_longitude=area.start_longitude,
            end_longitude=area.end_longitude,
            longitude_step=area.longitude_step,
            start_latitude=area.start_latitude,
            end_latitude=area.end_latitude,
            latitude_step=area.latitude_step,
            values=values,
        )
        message_bytes_list.append(message_bytes)
    eccodes.codes_release(message)
    return message_bytes_list


def get_message_bytes(
        message,
        start_longitude: Union[float, int],
        end_longitude: Union[float, int],
        longitude_step: Optional[Union[float, int]],
        start_latitude: Union[float, int],
        end_latitude: Union[float, int],
        latitude_step: Optional[Union[float, int]],
        values: Optional[np.ndarray] = None,
) -> bytes:
    """
    从 GRIB2 文件中读取第 count 个要素场，裁剪区域，并返回新场的字节码。
    如果设置了 longitude_step 和 latitude_step，则使用最近邻插值生成数据。

    Parameters
    ----------
    file_path
    start_longitude
    end_longitude
    longitude_step
    start_latitude
    end_latitude
    latitude_step
    count
        要素场序号，从 1 开始，ecCodes GRIB Key count

    Returns
    -------
    bytes
        重新编码后的 GRIB 2 消息字节码
    """
    if latitude_step is None and longitude_step is None:
        message = extract_region(
            message,
            # 0, 180, 89.875, 0.125
            start_longitude,
            end_longitude,
            start_latitude,
            end_latitude,
            values=values,
        )
    elif latitude_step is not None and longitude_step is not None:
        # missing_value = MISSING_VALUE
        # message = interpolate_grid(
        #     message,
        #     latitude=np.arange(start_latitude, end_latitude + latitude_step, latitude_step),
        #     longitude=np.arange(start_longitude, end_longitude + longitude_step, longitude_step),
        #     bounds_error=False,
        #     fill_value=missing_value,
        #     scheme="linear"
        # )
        message = extract_region(
            message,
            # 0, 180, 89.875, 0.125
            start_longitude,
            end_longitude,
            start_latitude,
            end_latitude,
            longitude_step=longitude_step,
            latitude_step=latitude_step,
            values=values,
        )
    else:
        raise ValueError("longitude_step and latitude_step must be set together")

    # logger.info("encoding...")
    message_bytes = eccodes.codes_get_message(message)
    # logger.info("encoding...done")
    eccodes.codes_release(message)
    return message_bytes


def get_output_file_name(area: Area) -> str:
    return area.filename.format(name=area.name)