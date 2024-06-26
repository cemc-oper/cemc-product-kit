from pathlib import Path
from typing import Optional

import typer

from cemc_product_kit.times import parse_time_options

app = typer.Typer()


data_type = "grapes_gfs_gmf/grib2/orig"


@app.command()
def serial(
        start_time: Optional[str] = None,
        forecast_time: Optional[str] = None,
        input_file_path: Optional[Path] = None,
        longitude: str = typer.Option(None),
        latitude: str = typer.Option(None),
        output_file_path: Optional[Path] = typer.Option(None)
):
    from reki.data_finder import find_local_file
    from cemc_product_kit.systems.cma_gfs.grib2_ne.task_serial import make_grib2_ne_by_serial

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

    (
        start_longitude, end_longitude, longitude_step,
        start_latitude, end_latitude, latitude_step
    ) = parse_grid(longitude, latitude)

    make_grib2_ne_by_serial(
        input_file_path=input_file_path,
        start_longitude=start_longitude,
        end_longitude=end_longitude,
        longitude_step=longitude_step,
        start_latitude=start_latitude,
        end_latitude=end_latitude,
        latitude_step=latitude_step,
        output_file_path=output_file_path,
    )


@app.command()
def dask_v1(
        start_time: Optional[str] = None,
        forecast_time: Optional[str] = None,
        input_file_path: Optional[Path] = None,
        longitude: str = typer.Option(None),
        latitude: str = typer.Option(None),
        output_file_path: Optional[Path] = typer.Option(None),
        engine: str = "local",
        n_workers: int = typer.Option(None),
):
    from reki.data_finder import find_local_file
    from cemc_product_kit.systems.cma_gfs.grib2_ne.task_dask_v1 import make_grib2_ne_by_dask_v1

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

    (
        start_longitude, end_longitude, longitude_step,
        start_latitude, end_latitude, latitude_step,
    ) = parse_grid(longitude, latitude)

    make_grib2_ne_by_dask_v1(
        input_file_path=input_file_path,
        start_longitude=start_longitude,
        end_longitude=end_longitude,
        longitude_step=longitude_step,
        start_latitude=start_latitude,
        end_latitude=end_latitude,
        latitude_step=latitude_step,
        output_file_path=output_file_path,
        engine=engine,
        n_workers=n_workers,
    )


@app.command()
def dask_v2(
        start_time: Optional[str] = None,
        forecast_time: Optional[str] = None,
        output_file_path: Optional[Path] = typer.Option(None),
        engine: str = typer.Option("local"),
        batch_size: int = typer.Option(32),
):
    from cemc_product_kit.systems.cma_gfs.grib2_ne.task_dask_v2 import make_grib2_ne_by_dask_v2

    start_time, forecast_time = parse_time_options(start_time, forecast_time)

    make_grib2_ne_by_dask_v2(
        start_time=start_time,
        forecast_time=forecast_time,
        output_file_path=output_file_path,
        engine=engine,
        batch_size=batch_size,
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
