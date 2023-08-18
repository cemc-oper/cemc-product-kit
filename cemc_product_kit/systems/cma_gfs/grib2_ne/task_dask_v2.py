"""
并行方式生成 grib2-ne 数据

分批解码、编码、写文件，每次分发 32 个任务

是否对多计算节点有帮助？
"""

from pathlib import Path
from typing import Union, Optional

import dask
import numpy as np

from cemc_product_kit.logging import get_logger
from cemc_product_kit.utils import cal_run_time, get_message_count, create_dask_client
from cemc_product_kit.systems.cma_gfs.grib2_ne.common import get_message_bytes


logger = get_logger()


@cal_run_time
def make_grib2_ne_by_dask_v2(
        input_file_path: Union[Path, str],
        output_file_path: Union[Path, str],
        start_longitude: Union[float, int],
        end_longitude: Union[float, int],
        longitude_step: Optional[Union[float, int]],
        start_latitude: Union[float, int],
        end_latitude: Union[float, int],
        latitude_step: Optional[Union[float, int]],
        engine: str = "local",
        batch_size: int = 32,
):
    logger.info(f"create dask client with engine {engine}...")
    if engine == "local":
        client_kwargs = dict(threads_per_worker=1)
    else:
        client_kwargs = dict()
    client = create_dask_client(engine, client_kwargs=client_kwargs)
    logger.info("create dask client with engine {engine}...done")
    logger.info(f"client: {client}")

    logger.info("count...")
    total_count = get_message_count(input_file_path)
    logger.info("count..done")

    def get_object(x):
        return x

    with open(output_file_path, "wb") as f:
        for batch in np.array_split(np.arange(1, total_count+1), np.ceil(total_count/batch_size)):
            bytes_lazy = []
            for i in batch:
                fut = dask.delayed(get_message_bytes)(
                    input_file_path,
                    start_longitude, end_longitude, longitude_step,
                    start_latitude, end_latitude, latitude_step,
                    i
                )
                bytes_lazy.append(fut)
            b = dask.delayed(get_object)(bytes_lazy)
            b_future = b.persist()
            bytes_result = b_future.compute()
            del b_future

            for i, b in enumerate(bytes_result):
                logger.info(f"writing message...{i + batch[0]}/{total_count}")
                f.write(b)
                del b

    logger.info("shutdown client...")
    client.shutdown()
    logger.info("shutdown client...done")
    logger.info("close client...")
    client.close()
    logger.info("close client...done")
    logger.info("task is finished")
