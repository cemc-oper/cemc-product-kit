from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path

import yaml


@dataclass
class Area:
    name: str
    filename: str
    start_latitude: float
    end_latitude: float
    start_longitude: float
    end_longitude: float
    latitude_step: Optional[float] = None
    longitude_step: Optional[float] = None


@dataclass
class Config:
    areas: List[Area]


def parse_config(config_file_path: Path) -> Config:
    with open(config_file_path, "r") as config_file:
        config_dict = yaml.safe_load(config_file)
        areas = []
        for area_item in config_dict["areas"]:
            areas.append(Area(**area_item))
        config = Config(areas=areas)
        return config
