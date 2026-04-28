from __future__ import annotations

import json
import hashlib
import math
import os
import ssl
import time
from collections import Counter
from pathlib import Path
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import pandas as pd

try:
    import requests
except ImportError:
    requests = None


RESULT_DIR = Path(
    "C:/Users/liuy1/Desktop/0.文章/3.重点研发Chao/0.nightime/data/ntl/POI/RADIUS_M = 1000"
)
INPUT_CSV = RESULT_DIR / "window_anomaly_records.csv"
OUTPUT_CSV = RESULT_DIR / "window_anomaly_pixel_level_with_baidu_poi.csv"
POI_LONG_CSV = RESULT_DIR / "window_anomaly_baidu_poi_long.csv"
CHECKPOINT_CSV = RESULT_DIR / "window_anomaly_pixel_level_with_baidu_poi_checkpoint.csv"
CHECKPOINT_POI_LONG_CSV = RESULT_DIR / "window_anomaly_baidu_poi_long_checkpoint.csv"

BAIDU_AK = os.getenv("BAIDU_MAP_AK", "bmFoL8VVAn5jk1R1akp8sFuIkNrUlYx9")
BAIDU_SK = os.getenv("BAIDU_MAP_SK", "VLgVFnxHIhNr7YNBc9mqICGAcD8yq3a4")
BAIDU_HOST = "https://api.map.baidu.com"
PLACE_SEARCH_PATH = "/place/v2/search"
RADIUS_M = 1000
COORD_TYPE = 1  # 1 = WGS84; input pixel coords are WGS84 from VIIRS
PAGE_SIZE = 20
MAX_PAGES_PER_QUERY = 5
SLEEP_SECONDS = 0.15
MAX_REQUEST_RETRIES = 5
REQUEST_TIMEOUT = 30
SAVE_EVERY_N_ROWS = 10

# 关键词精化：避免短词误匹配（如"汽车"匹配"汽车旅馆"，"市场"匹配"人才市场"）
CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "餐饮住宿": ["美食", "餐饮", "中餐", "快餐", "火锅", "酒店", "宾馆", "民宿", "度假村", "客栈"],
    "旅游服务": ["景区", "旅游", "公园", "海滩", "风景", "古镇", "度假区"],
    "零售商业": ["购物中心", "超市", "商场", "商业街", "农贸市场", "批发市场", "便利店"],
    "工业生产": ["工厂", "工业园", "加工", "制造", "产业园", "物流园"],
    "教育": ["学校", "幼儿园", "培训", "大学", "中学", "小学", "教育"],
    "医疗": ["医院", "诊所", "药店", "卫生院", "医疗"],
    "公共管理": ["政府", "机关", "派出所", "街道办", "乡镇政府", "村委"],
    "居住": ["小区", "住宅", "村庄", "社区", "居委"],
    "交通服务": ["加油站", "汽车4S", "汽车修", "停车场", "公交站", "地铁站", "火车站", "机场"],
}

POI_QUERY_SPECS = [
    {"category": category, "query": "$".join(keywords)}
    for category, keywords in CATEGORY_KEYWORDS.items()
]

# 坐标转换常量
PI = math.pi
A = 6378245.0
EE = 0.00669342162296594323


# ── 工具函数 ──────────────────────────────────────────────

def request_json(url: str) -> dict:
    last_error = None
    for attempt in range(1, MAX_REQUEST_RETRIES + 1):
        try:
            if requests is not None:
                resp = requests.get(url, timeout=REQUEST_TIMEOUT,
                                    headers={"User-Agent": "Mozilla/5.0"})
                resp.raise_for_status()
                return resp.json()
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            ctx = ssl.create_default_context()
            with urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            last_error = exc
            wait = min(2 ** attempt, 30)
            print(f"Request failed ({attempt}/{MAX_REQUEST_RETRIES}): {exc}; retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Request failed after {MAX_REQUEST_RETRIES} retries: {last_error}")


def baidu_signed_url(path: str, params: dict) -> str:
    query = urlencode(params)
    raw = f"{path}?{query}{BAIDU_SK}"
    sn = hashlib.md5(quote(raw, safe="").encode("utf-8")).hexdigest()
    return f"{BAIDU_HOST}{path}?{query}&sn={sn}"


def baidu_place_search(lat: float, lon: float, query: str, page_num: int) -> dict:
    params = {
        "query": query,
        "location": f"{lat},{lon}",
        "radius": RADIUS_M,
        "radius_limit": "true",
        "coord_type": COORD_TYPE,
        "scope": 2,
        "page_size": PAGE_SIZE,
        "page_num": page_num,
        "output": "json",
        "ak": BAIDU_AK,
    }
    return request_json(baidu_signed_url(PLACE_SEARCH_PATH, params))


# ── 坐标转换：BD09 → GCJ02 → WGS84 ──────────────────────
# 百度地图API返回坐标为BD09，需两步转换才能与VIIRS WGS84栅格对齐

def out_of_china(lon: float, lat: float) -> bool:
    return not (73.66 < lon < 135.05 and 3.86 < lat < 53.55)


def _transform_lat(x: float, y: float) -> float:
    ret = (-100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y
           + 0.1 * x * y + 0.2 * math.sqrt(abs(x)))
    ret += (20.0 * math.sin(6.0 * x * PI) + 20.0 * math.sin(2.0 * x * PI)) * 2.0 / 3.0
    ret += (20.0 * math.sin(y * PI) + 40.0 * math.sin(y / 3.0 * PI)) * 2.0 / 3.0
    ret += (160.0 * math.sin(y / 12.0 * PI) + 320.0 * math.sin(y * PI / 30.0)) * 2.0 / 3.0
    return ret


def _transform_lon(x: float, y: float) -> float:
    ret = (300.0 + x + 2.0 * y + 0.1 * x * x
           + 0.1 * x * y + 0.1 * math.sqrt(abs(x)))
    ret += (20.0 * math.sin(6.0 * x * PI) + 20.0 * math.sin(2.0 * x * PI)) * 2.0 / 3.0
    ret += (20.0 * math.sin(x * PI) + 40.0 * math.sin(x / 3.0 * PI)) * 2.0 / 3.0
    ret += (150.0 * math.sin(x / 12.0 * PI) + 300.0 * math.sin(x / 30.0 * PI)) * 2.0 / 3.0
    return ret


def gcj02_to_wgs84(lon: float, lat: float) -> tuple[float, float]:
    if out_of_china(lon, lat):
        return lon, lat
    dlat = _transform_lat(lon - 105.0, lat - 35.0)
    dlon = _transform_lon(lon - 105.0, lat - 35.0)
    radlat = lat / 180.0 * PI
    magic = math.sin(radlat)
    magic = 1 - EE * magic * magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((A * (1 - EE)) / (magic * sqrtmagic) * PI)
    dlon = (dlon * 180.0) / (A / sqrtmagic * math.cos(radlat) * PI)
    return lon * 2 - (lon + dlon), lat * 2 - (lat + dlat)


def bd09_to_gcj02(bd_lon: float, bd_lat: float) -> tuple[float, float]:
    """百度BD09 → 国测局GCJ02"""
    x = bd_lon - 0.0065
    y = bd_lat - 0.006
    z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * PI * 3000 / 180)
    theta = math.atan2(y, x) - 0.000003 * math.cos(x * PI * 3000 / 180)
    return z * math.cos(theta), z * math.sin(theta)


def bd09_to_wgs84(bd_lon: float, bd_lat: float) -> tuple[float, float]:
    """百度BD09 → WGS84（两步：BD09→GCJ02→WGS84）"""
    gcj_lon, gcj_lat = bd09_to_gcj02(bd_lon, bd_lat)
    return gcj02_to_wgs84(gcj_lon, gcj_lat)


# ── POI 分类 ──────────────────────────────────────────────

def classify_poi(name: str | None, tag: str | None,
                 address: str | None, query_category: str) -> str:
    """按name/tag/address关键词匹配行业类别；匹配不到则用查询类别兜底"""
    text = " ".join(filter(None, [str(name or ""), str(tag or ""), str(address or "")]))
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return category
    return query_category


# ── 核心采集 ──────────────────────────────────────────────

def collect_pois(row: pd.Series) -> list[dict]:
    lat = float(row["lat"])
    lon = float(row["lon"])
    seen_uids: set[str] = set()
    pois: list[dict] = []

    for spec in POI_QUERY_SPECS:
        query_category = spec["category"]
        query = spec["query"]

        for page_num in range(MAX_PAGES_PER_QUERY):
            data = baidu_place_search(lat, lon, query, page_num)
            status = data.get("status")
            if status != 0:
                raise RuntimeError(
                    f"Baidu API error status={status}, "
                    f"message={data.get('message')}, row={row.name}"
                )

            results = data.get("results", [])
            if not results:
                break

            for poi in results:
                uid = poi.get("uid") or f"{poi.get('name')}|{poi.get('address')}"
                if uid in seen_uids:
                    continue
                seen_uids.add(uid)

                detail = poi.get("detail_info") or {}
                loc = poi.get("location") or {}
                bd_lon = loc.get("lng")
                bd_lat = loc.get("lat")

                # BD09 → WGS84（与VIIRS栅格坐标系对齐）
                wgs_lon, wgs_lat = (None, None)
                if bd_lon is not None and bd_lat is not None:
                    wgs_lon, wgs_lat = bd09_to_wgs84(float(bd_lon), float(bd_lat))

                pois.append({
                    "county":           row.get("county"),
                    "pixel_id":         row.get("pixel_id"),
                    "pixel_lon":        lon,
                    "pixel_lat":        lat,
                    "query_category":   query_category,
                    "query":            query,
                    "uid":              poi.get("uid"),
                    "name":             poi.get("name"),
                    "tag":              poi.get("tag"),
                    "address":          poi.get("address"),
                    "province":         poi.get("province"),
                    "city":             poi.get("city"),
                    "area":             poi.get("area"),
                    "poi_lon_bd09":     bd_lon,      # 原始百度坐标（保留备查）
                    "poi_lat_bd09":     bd_lat,
                    "poi_lon_wgs84":    wgs_lon,     # 转换后WGS84，可直接叠加NTL栅格
                    "poi_lat_wgs84":    wgs_lat,
                    "industry_category": classify_poi(
                        poi.get("name"), poi.get("tag"),
                        poi.get("address"), query_category
                    ),
                    "distance_m":       detail.get("distance"),
                    "type":             detail.get("type"),
                    "overall_rating":   detail.get("overall_rating"),
                    "comment_num":      detail.get("comment_num"),
                })

            if len(results) < PAGE_SIZE:
                break
            time.sleep(SLEEP_SECONDS)

    # 按距离升序排列
    pois.sort(
        key=lambda x: float(x["distance_m"])
        if x["distance_m"] not in [None, ""] else float("inf")
    )
    return pois


def summarize_pois(pois: list[dict], max_names: int = 10) -> dict:
    base: dict = {
        "poi_count_radius":         0,
        "nearest_poi_name":         None,
        "nearest_poi_tag":          None,
        "nearest_poi_category":     None,
        "nearest_poi_distance_m":   None,
        "nearest_poi_lon_wgs84":    None,
        "nearest_poi_lat_wgs84":    None,
        "poi_names_radius":         None,
        "poi_tags_radius":          None,
        "poi_categories_radius":    None,
        "dominant_category":        None,
    }
    # 各类别计数字段初始化为0
    for cat in CATEGORY_KEYWORDS:
        base[f"poi_count_{cat}"] = 0

    if not pois:
        return base

    nearest = pois[0]
    tags = sorted({str(p["tag"]) for p in pois if p.get("tag")})
    names = [str(p["name"]) for p in pois if p.get("name")]
    cat_counts = Counter(
        p["industry_category"] for p in pois if p.get("industry_category")
    )
    dominant = cat_counts.most_common(1)[0][0] if cat_counts else None

    summary = {
        "poi_count_radius":         len(pois),
        "nearest_poi_name":         nearest.get("name"),
        "nearest_poi_tag":          nearest.get("tag"),
        "nearest_poi_category":     nearest.get("industry_category"),
        "nearest_poi_distance_m":   nearest.get("distance_m"),
        "nearest_poi_lon_wgs84":    nearest.get("poi_lon_wgs84"),
        "nearest_poi_lat_wgs84":    nearest.get("poi_lat_wgs84"),
        "poi_names_radius":         " | ".join(names[:max_names]),
        "poi_tags_radius":          " | ".join(tags),
        "poi_categories_radius":    " | ".join(sorted(cat_counts.keys())),
        "dominant_category":        dominant,   # 主导功能类别，可直接用于功能区定性
    }
    for cat in CATEGORY_KEYWORDS:
        summary[f"poi_count_{cat}"] = cat_counts.get(cat, 0)

    return summary


# ── 断点续传 ──────────────────────────────────────────────

def row_key(row: pd.Series) -> str:
    return "|".join([
        str(row.get("county", "")),
        str(row.get("pixel_id", "")),
        str(row.get("lon", "")),
        str(row.get("lat", "")),
        str(row.name),
    ])


def save_outputs(source_df: pd.DataFrame, summaries: list[dict | None],
                 poi_rows: list[dict], final: bool = False) -> None:
    summary_df = pd.DataFrame([s if s is not None else {} for s in summaries])
    out = pd.concat([source_df.reset_index(drop=True),
                     summary_df.reset_index(drop=True)], axis=1)
    out["baidu_poi_done"] = [s is not None for s in summaries]

    out_path = OUTPUT_CSV if final else CHECKPOINT_CSV
    poi_path = POI_LONG_CSV if final else CHECKPOINT_POI_LONG_CSV

    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    pd.DataFrame(poi_rows).to_csv(poi_path, index=False, encoding="utf-8-sig")
    print(f"Saved: {out_path}")
    print(f"Saved: {poi_path}")


# ── 主流程 ────────────────────────────────────────────────

def main() -> None:
    if not BAIDU_AK:
        raise RuntimeError("Please set BAIDU_MAP_AK before running this script.")
    if not BAIDU_SK:
        raise RuntimeError("Please set BAIDU_MAP_SK before running this script.")

    df = pd.read_csv(INPUT_CSV, encoding="utf-8-sig")
    df["pixel_id"] = df["pixel_id"].astype(str)
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["baidu_row_key"] = [row_key(row) for _, row in df.iterrows()]

    summaries: list[dict | None] = [None] * len(df)
    poi_rows: list[dict] = []
    completed_keys: set[str] = set()

    # 恢复断点
    if CHECKPOINT_CSV.exists():
        ckpt = pd.read_csv(CHECKPOINT_CSV, encoding="utf-8-sig")
        if "baidu_row_key" in ckpt.columns and "baidu_poi_done" in ckpt.columns:
            done = ckpt[ckpt["baidu_poi_done"].fillna(False).astype(bool)].copy()
            completed_keys = set(done["baidu_row_key"].astype(str))
            summary_cols = [c for c in done.columns
                            if c in summarize_pois([]).keys()]
            for col in summary_cols:
                if col not in done.columns:
                    done[col] = None
            summary_by_key = done.set_index("baidu_row_key")[summary_cols].to_dict("index")
            for i, key in enumerate(df["baidu_row_key"].astype(str)):
                if key in summary_by_key:
                    summaries[i] = summary_by_key[key]

    if CHECKPOINT_POI_LONG_CSV.exists():
        poi_rows = pd.read_csv(CHECKPOINT_POI_LONG_CSV,
                               encoding="utf-8-sig").to_dict("records")

    try:
        for idx, row in df.iterrows():
            key = str(row["baidu_row_key"])
            if key in completed_keys:
                continue

            if pd.isna(row["lon"]) or pd.isna(row["lat"]):
                summaries[idx] = summarize_pois([])
                completed_keys.add(key)
                continue

            pois = collect_pois(row)
            summaries[idx] = summarize_pois(pois)
            poi_rows.extend(pois)
            completed_keys.add(key)

            if (idx + 1) % SAVE_EVERY_N_ROWS == 0:
                print(f"Processed {idx + 1}/{len(df)} rows")
                save_outputs(df, summaries, poi_rows, final=False)
            time.sleep(SLEEP_SECONDS)

    except KeyboardInterrupt:
        print("Interrupted. Saving checkpoint...")
        save_outputs(df, summaries, poi_rows, final=False)
        raise
    except Exception:
        print("Error. Saving checkpoint before raising...")
        save_outputs(df, summaries, poi_rows, final=False)
        raise

    save_outputs(df, summaries, poi_rows, final=True)


if __name__ == "__main__":
    main()