import glob
import os

import fiona
import numpy as np
import rasterio
from rasterio.mask import mask
from rasterio.merge import merge
from rasterio.warp import reproject, Resampling, transform_geom

# ====== 参数 ======
ntl_dir = "D:/cmafiles/L/database/nighttime/Precess/VNP46A2/"
out_dir = "D:/cmafiles/L/database/nighttime/Precess/Presult/VNP46A2"
study_area_shp = "D:/cmafiles/L/database/gis/中国专题图/省级数据/海南省/海南省.shp"
mcd12q1_files = [
    "D:/cmafiles/L/database/nighttime/LandCover/MCD12Q1.A2024001.h28v06.061.2025206072738.hdf",
    "D:/cmafiles/L/database/nighttime/LandCover/MCD12Q1.A2024001.h28v07.061.2025206060336.hdf",
]
landcover_class = 13  # IGBP: Urban and Built-up
subdataset_keys = ("LC_Type1", "Land_Cover_Type_1", "LC_Type_1")


def pick_subdataset(path: str) -> str:
    with rasterio.open(path) as src:
        for key in subdataset_keys:
            match = next((s for s in src.subdatasets if key in s), None)
            if match:
                return match
    raise RuntimeError(f"No landcover subdataset found in {path}")


def build_landcover_mosaic():
    srcs = []
    for path in mcd12q1_files:
        subdataset = pick_subdataset(path)
        srcs.append(rasterio.open(subdataset))
    mosaic, transform = merge(srcs)
    meta = srcs[0].meta.copy()
    meta.update(
        {
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": transform,
            "count": 1,
        }
    )
    for src in srcs:
        src.close()
    return mosaic[0], meta


def mask_ntl_with_builtup(landcover, landcover_meta):
    os.makedirs(out_dir, exist_ok=True)
    ntl_files = sorted(glob.glob(f"{ntl_dir}/*.tif"))
    with fiona.open(study_area_shp, "r") as shp:
        shapes = [feature["geometry"] for feature in shp]
        shp_crs = shp.crs_wkt or shp.crs
    for ntl_path in ntl_files:
        with rasterio.open(ntl_path) as ntl:
            ntl_data = ntl.read(1).astype("float32")
            landcover_resampled = np.zeros((ntl.height, ntl.width), dtype=landcover.dtype)
            reproject(
                source=landcover,
                destination=landcover_resampled,
                src_transform=landcover_meta["transform"],
                src_crs=landcover_meta["crs"],
                dst_transform=ntl.transform,
                dst_crs=ntl.crs,
                resampling=Resampling.nearest,
                src_nodata=landcover_meta.get("nodata"),
                dst_nodata=0,
            )

            builtup_mask = landcover_resampled == landcover_class
            masked = np.where(builtup_mask, ntl_data, np.nan)

            base = os.path.basename(ntl_path)
            date = base.split("_")[1]
            out_path = os.path.join(out_dir, f"VNP46A2_{date}_presult.tif")
            out_meta = ntl.meta.copy()
            out_meta.update({"dtype": "float32", "nodata": np.nan})
            with rasterio.open(out_path, "w", **out_meta) as dst:
                dst.write(masked, 1)

        with rasterio.open(out_path) as dst:
            if shp_crs and dst.crs and shp_crs != dst.crs:
                clip_shapes = [
                    transform_geom(shp_crs, dst.crs, geom) for geom in shapes
                ]
            else:
                clip_shapes = shapes
            clipped, clipped_transform = mask(dst, clip_shapes, crop=True)
            clipped_meta = dst.meta.copy()
            clipped_meta.update(
                {
                    "height": clipped.shape[1],
                    "width": clipped.shape[2],
                    "transform": clipped_transform,
                }
            )
        with rasterio.open(out_path, "w", **clipped_meta) as clipped_dst:
            clipped_dst.write(clipped)
        print(f"Saved: {out_path}")


if __name__ == "__main__":
    landcover_data, landcover_meta = build_landcover_mosaic()
    mask_ntl_with_builtup(landcover_data, landcover_meta)
