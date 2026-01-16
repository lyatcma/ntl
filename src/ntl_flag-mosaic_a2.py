import glob
import h5py
import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.merge import merge
from rasterio.io import MemoryFile
from collections import defaultdict

# ====== 参数 ======
data_dir = "D:/cmafiles/L/database/nighttime/VNP46A2_2024"
out_dir = "D:/cmafiles/L/database/nighttime/Precess/VNP46A2"
ntl_path = "/HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/DNB_BRDF-Corrected_NTL"
qf_path  = "/HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/Mandatory_Quality_Flag"
crs = "EPSG:4326"
valid_qf = {0}  # 质量码 = 0

# ===== tile边界 =====
tile_bounds = {
    "h28v06": (100, 20, 110, 30),  # (minlon, minlat, maxlon, maxlat)
    "h28v07": (100, 10, 110, 20),
    "h29v06": (110, 20, 120, 30),
    "h29v07": (110, 10, 120, 20),
}

# ===== 按日期分组 =====
files = sorted(glob.glob(f"{data_dir}/*.h5"))
daily = defaultdict(list)
for f in files:
    date = f.split(".")[1]  # AYYYYDDD
    daily[date].append(f)

# ===== 每日处理 =====
for date, file_list in daily.items():
    datasets = []

    for f in file_list:
        # 从文件名中提取 tile 名称，例如 h28v06
        tile = [t for t in tile_bounds.keys() if t in f][0]
        minlon, minlat, maxlon, maxlat = tile_bounds[tile]

        with h5py.File(f, "r") as h5:
            ntl = h5[ntl_path][:]
            qf  = h5[qf_path][:]

        mask = np.isin(qf, list(valid_qf))
        ntl_masked = np.where(mask, ntl, np.nan)

        # 计算 transform
        height, width = ntl_masked.shape
        resx = (maxlon - minlon) / width
        resy = (maxlat - minlat) / height
        transform = from_origin(minlon, maxlat, resx, resy)

        mem = MemoryFile()
        with mem.open(
            driver="GTiff",
            height=height,
            width=width,
            count=1,
            dtype=ntl_masked.dtype,
            crs=crs,
            transform=transform,
        ) as ds:
            ds.write(ntl_masked, 1)
        datasets.append(mem)

    # 拼接 mosaic
    srcs = [m.open() for m in datasets]
    mosaic, out_transform = merge(srcs)

    out_path = f"{out_dir}/VNP46A2_{date}_mosaic.tif"
    with rasterio.open(
        out_path,
        "w",
        driver="GTiff",
        height=mosaic.shape[1],
        width=mosaic.shape[2],
        count=1,
        dtype=mosaic.dtype,
        crs=crs,
        transform=out_transform,
    ) as dest:
        dest.write(mosaic[0], 1)

    for s in srcs:
        s.close()
    for m in datasets:
        m.close()

    print(f"Saved: {out_path}")