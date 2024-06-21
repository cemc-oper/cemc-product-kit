"""
并行方式生成 grib2-ne 数据

1. 并行解码，抽取区域，编码
2. 主程序串行接收编码后的 GRIB 2 消息字节码，写入到文件中

需要传输 GRIB 2 字节码

耗时：1 分钟 (登录节点测试)
"""
from pathlib import Path
from typing import Union, Optional

from cemc_product_kit.logging import get_logger
from cemc_product_kit.systems.cma_gfs.grib2_ne.common import get_message_bytes
from cemc_product_kit.utils import cal_run_time, create_dask_client, get_message_count


logger = get_logger()


@cal_run_time
def make_grib2_ne_by_dask_v1(
        input_file_path: Union[Path, str],
        start_longitude: Union[float, int],
        end_longitude: Union[float, int],
        longitude_step: Optional[Union[float, int]],
        start_latitude: Union[float, int],
        end_latitude: Union[float, int],
        latitude_step: Optional[Union[float, int]],
        output_file_path: Union[Path, str],
        engine: str = "local",
        n_workers: Optional[int] = None,
):
    logger.info(f"create dask client with engine {engine}...")
    if engine == "local":
        client_kwargs = dict(threads_per_worker=1)
        if n_workers is not None:
            client_kwargs["n_workers"] = n_workers
    else:
        client_kwargs = dict()
    client = create_dask_client(engine, client_kwargs=client_kwargs)
    logger.info("create dask client with engine {engine}...done")
    logger.info(f"client: {client}")


    logger.info("count...")
    total_count = get_message_count(input_file_path)
    logger.info("count..done")

    # 一次性分发任务，Future 按顺序保存到列表中
    logger.info("submit jobs...")
    bytes_futures = []
    for i in range(1, total_count+1):
        f = client.submit(
            get_message_bytes,
            input_file_path,
            start_longitude, end_longitude, longitude_step,
            start_latitude, end_latitude, latitude_step,
            i
        )
        bytes_futures.append(f)
    logger.info("submit jobs...done")

    # 依次读取 Future 返回值，写入到文件中
    logger.info("receive results and write to file...")
    with open(output_file_path, "wb") as f:
        for i, fut in enumerate(bytes_futures):
            message_bytes = client.gather(fut)
            del fut
            logger.info(f"writing message...{i + 1}/{total_count}")
            f.write(message_bytes)
            del message_bytes
    logger.info("receive results and write to file...done")

    logger.info("shutdown client...")
    client.shutdown()
    logger.info("shutdown client...done")
    logger.info("close client...")
    client.close()
    logger.info("close client...done")
    logger.info("task is finished")
