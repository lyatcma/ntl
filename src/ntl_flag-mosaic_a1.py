import glob
import h5py
import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.merge import merge
from rasterio.io import MemoryFile
from collections import defaultdict

# ====== 参数 ======
a1_dir = "D:/cmafiles/L/database/nighttime/VNP46A1_2024"
a2_dir = "D:/cmafiles/L/database/nighttime/VNP46A2_2024"
out_dir = "D:/cmafiles/L/database/nighttime/Precess/VNP46A1"

vza_path = "/HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/Sensor_Zenith"
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
a1_files = sorted(glob.glob(f"{a1_dir}/*.h5"))
a2_files = sorted(glob.glob(f"{a2_dir}/*.h5"))

a1_daily = defaultdict(list)
for f in a1_files:
    date = f.split(".")[1]  # AYYYYDDD
    a1_daily[date].append(f)

a2_lookup = defaultdict(dict)
for f in a2_files:
    date = f.split(".")[1]  # AYYYYDDD
    tile = next((t for t in tile_bounds.keys() if t in f), None)
    if tile:
        a2_lookup[date][tile] = f

# ===== 每日处理 =====
for date, file_list in a1_daily.items():
    datasets = []

    for f in file_list:
        # 从文件名中提取 tile 名称，例如 h28v06
        tile = [t for t in tile_bounds.keys() if t in f][0]
        a2_file = a2_lookup.get(date, {}).get(tile)
        if not a2_file:
            print(f"Skip {f}: missing VNP46A2 quality file for {date} {tile}")
            continue
        minlon, minlat, maxlon, maxlat = tile_bounds[tile]

        with h5py.File(f, "r") as h5:
            vza = h5[vza_path][:]
        with h5py.File(a2_file, "r") as h5:
            qf = h5[qf_path][:]

        mask = np.isin(qf, list(valid_qf))
        vza_masked = np.where(mask, vza, np.nan)

        # 计算 transform
        height, width = vza_masked.shape
        resx = (maxlon - minlon) / width
        resy = (maxlat - minlat) / height
        transform = from_origin(minlon, maxlat, resx, resy)

        mem = MemoryFile()
        with mem.open(
            driver="GTiff",
            height=height,
            width=width,
            count=1,
            dtype=vza_masked.dtype,
            crs=crs,
            transform=transform,
        ) as ds:
            ds.write(vza_masked, 1)
        datasets.append(mem)

    # 拼接 mosaic
    if not datasets:
        print(f"No tiles to merge for {date}")
        continue
    srcs = [m.open() for m in datasets]
    mosaic, out_transform = merge(srcs)

    out_path = f"{out_dir}/VNP46A1_{date}_mosaic.tif"
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