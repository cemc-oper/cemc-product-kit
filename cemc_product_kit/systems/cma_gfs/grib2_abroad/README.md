# CMA-GFS 国外低分辨率数据

CMA-GFS/grib2-abroad

抽取要素 + 重新排序 + 插值

wgrib2 + gribpost.exe

原任务：50秒

## Usage

### Serial

```bash
python -m cemc_product_kit.systems.cma_gfs.grib2_abroad \
  serial \
  --start-time 2023081600 \
  --forecast-time 24h \
  --input-file-path /some/path/to/gmf.gra.2023081600024.grb2 \
  --output-file-path /some/path/to/abroad.2023081600024.grb2
```

### Parallel

In HPC with slurm

```bash
mpirun -np 64 \
  python -m cemc_product_kit.systems.cma_gfs.grib2_abroad \
  dask_v1 \
  --start-time 2023081600 \
  --forecast-time 24h \
  --input-file-path /some/path/to/gmf.gra.2023081600024.grb2 \
  --output-file-path /some/path/to/abroad.2023081600024.grb2 \
  --engine mpi
```