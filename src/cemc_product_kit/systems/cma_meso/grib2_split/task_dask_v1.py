from pathlib import Path
from typing import Union, Optional
from contextlib import ExitStack

from reki.format.grib.eccodes import load_message_from_file

from cemc_product_kit.logging import get_logger
from cemc_product_kit.utils import cal_run_time, create_dask_client, get_message_count
from cemc_product_kit.systems.cma_meso.grib2_split.common import get_message_bytes_list, get_output_file_name
from cemc_product_kit.systems.cma_meso.grib2_split.config import Config, Area


logger = get_logger()


def get_message_bytes_from_file(file_path: Path, count: int) -> bytes:
    message = load_message_from_file(file_path, count=count)


@cal_run_time
def split_grib2_by_dask_v1(
        input_file_path: Union[Path, str],
        output_path: Union[Path, str],
        config: Config,
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

    areas = config.areas
    area_count = len(areas)

    logger.info("submit jobs...")
    bytes_futures = []
    for i in range(1, total_count+1):
        f = client.submit(
            get_message_bytes_list,
            input_file_path,
            i,
            areas,
        )
        bytes_futures.append(f)
    logger.info("submit jobs...done")

    file_paths = [Path(output_path, get_output_file_name(area)) for area in config.areas]

    logger.info("process...")
    with ExitStack() as stack:
        files = [stack.enter_context(open(file_path, "wb")) for file_path in file_paths]
        for i, fut in enumerate(bytes_futures):
            message_bytes_list = client.gather(fut)
            del fut
            logger.info(f"writing message...{i + 1}/{total_count}")
            for index in range(0, area_count):
                files[index].write(message_bytes_list[index])
            # logger.info("writing to files...done")
            del message_bytes_list
    logger.info("receive results and write to file...done")

    logger.info("shutdown client...")
    client.shutdown()
    logger.info("shutdown client...done")
    logger.info("close client...")
    client.close()
    logger.info("close client...done")
    logger.info("task is finished")
