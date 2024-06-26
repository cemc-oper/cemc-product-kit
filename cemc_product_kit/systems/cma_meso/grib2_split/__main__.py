from pathlib import Path
from typing import Optional

import typer

from cemc_product_kit.times import parse_time_options
from cemc_product_kit.systems.cma_meso.grib2_split.config import parse_config

app = typer.Typer()


data_type = "cma_meso_1km/grib2/orig"


@app.command()
def serial(
        config_file_path: Path = typer.Option(None),
        start_time: Optional[str] = None,
        forecast_time: Optional[str] = None,
        input_file_path: Optional[Path] = None,
        output_path: Optional[Path] = typer.Option(None)
):
    from reki.data_finder import find_local_file
    from cemc_product_kit.systems.cma_meso.grib2_split.task_serial import split_grib2_by_serial

    if input_file_path is None:
        start_time, forecast_time = parse_time_options(start_time, forecast_time)
        input_file_path = find_local_file(
            data_type,
            start_time=start_time,
            forecast_time=forecast_time
        )

    if input_file_path is None:
        print("input file path is empty. Please check options.")
        raise typer.Exit(code=2)

    config = parse_config(config_file_path)

    split_grib2_by_serial(
        input_file_path=input_file_path,
        output_path=output_path,
        config=config,
    )


@app.command()
def dask_v1(
        config_file_path: Path = typer.Option(None),
        start_time: Optional[str] = None,
        forecast_time: Optional[str] = None,
        input_file_path: Optional[Path] = None,
        output_path: Optional[Path] = typer.Option(None),
        engine: str = "local",
        n_workers: int = typer.Option(None),
):
    from reki.data_finder import find_local_file
    from cemc_product_kit.systems.cma_meso.grib2_split.task_dask_v1 import split_grib2_by_dask_v1

    if input_file_path is None:
        start_time, forecast_time = parse_time_options(start_time, forecast_time)
        input_file_path = find_local_file(
            data_type,
            start_time=start_time,
            forecast_time=forecast_time
        )

    if input_file_path is None:
        print("input file path is empty. Please check options.")
        raise typer.Exit(code=2)

    config = parse_config(config_file_path)

    split_grib2_by_dask_v1(
        input_file_path=input_file_path,
        output_path=output_path,
        config=config,
        engine=engine,
        n_workers=n_workers,
    )


def parse_grid(longitude: str, latitude: str):
    lon_tokens = longitude.split(":")
    if len(lon_tokens) == 2:
        start_longitude = float(lon_tokens[0])
        end_longitude = float(lon_tokens[1])
        longitude_step = None
    elif len(lon_tokens) == 3:
        start_longitude = float(lon_tokens[0])
        end_longitude = float(lon_tokens[1])
        longitude_step = float(lon_tokens[2])
    else:
        raise ValueError("longitude must be start_longitude:end_longitude or start_longitude:end_longitude:longitude_step")

    lat_tokens = latitude.split(":")
    if len(lat_tokens) == 2:
        start_latitude = float(lat_tokens[0])
        end_latitude = float(lat_tokens[1])
        latitude_step = None
    elif len(lat_tokens) == 3:
        start_latitude = float(lat_tokens[0])
        end_latitude = float(lat_tokens[1])
        latitude_step = float(lat_tokens[2])
    else:
        raise ValueError("latitude must be start_latitude:end_latitude or start_latitude:end_latitude:latitude_step")

    return start_longitude, end_longitude, longitude_step, start_latitude, end_latitude, latitude_step


if __name__ == "__main__":
    app()
