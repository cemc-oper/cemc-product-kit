from typing import Union, Dict, Optional

import xarray as xr
import numpy as np
import eccodes

from reki.operator.area import extract_region as extract_region_field
from reki.format.grib.eccodes._xarray import create_data_array_from_message
from reki.format.grib.common import MISSING_VALUE


def extract_region(
        message,
        start_longitude: Union[float, int],
        end_longitude: Union[float, int],
        start_latitude: Union[float, int],
        end_latitude: Union[float, int],
        longitude_step: Optional[Union[float, int]] = None,
        latitude_step: Optional[Union[float, int]] = None,
        values: Optional[np.ndarray] = None,
):
    missing_value = MISSING_VALUE
    field = create_data_array_from_message(
        message,
        missing_value=missing_value,
        fill_missing_value=np.nan,
        values=values,
    )

    target_field = extract_region_field(
        field,
        start_longitude=start_longitude,
        end_longitude=end_longitude,
        start_latitude=start_latitude,
        end_latitude=end_latitude,
        longitude_step=longitude_step,
        latitude_step=latitude_step
    )

    eccodes.codes_set_double(message, 'longitudeOfFirstGridPointInDegrees', target_field.longitude.values[0])
    eccodes.codes_set_double(message, 'longitudeOfLastGridPointInDegrees', target_field.longitude.values[-1])
    eccodes.codes_set_double(message, 'iDirectionIncrementInDegrees', abs(target_field.longitude.values[0] - target_field.longitude.values[1]))
    # eccodes.codes_set_double(message, 'iDirectionIncrementInDegrees', self.grid.lon_step)
    eccodes.codes_set_long(message, 'Ni', len(target_field.longitude.values))

    eccodes.codes_set_double(message, 'latitudeOfFirstGridPointInDegrees', target_field.latitude.values[0])
    eccodes.codes_set_double(message, 'latitudeOfLastGridPointInDegrees', target_field.latitude.values[-1])
    eccodes.codes_set_double(message, 'jDirectionIncrementInDegrees', abs(target_field.latitude.values[0] - target_field.latitude.values[1]))
    # eccodes.codes_set_double(message, 'jDirectionIncrementInDegrees', self.grid.lat_step)
    eccodes.codes_set_long(message, 'Nj', len(target_field.latitude.values))

    values = target_field.values.flatten()

    np.place(values, np.isnan(values), missing_value)
    # values = np.nan_to_num(values, missing_value)

    eccodes.codes_set(message, 'missingValue', missing_value)
    num_missing = 0
    for i in range(len(values)):
        if values[i] == missing_value:
            num_missing += 1

    if num_missing > 0:
        eccodes.codes_set(message, 'bitmapPresent', 1)

    # close constant field feature.
    # eccodes.codes_set_long(message, "produceLargeConstantFields", 1)

    eccodes.codes_set_double_array(message, "values", values)

    del field
    del target_field

    return message
