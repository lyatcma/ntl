import os
import re
import csv

# 参数
FOLDER = r"D:/cmafiles/L/database/nightime/ntl/raw/VNP46A1_2023"  # 你存放h5的文件夹
YEAR = 2023
TILES = ["h28v06", "h28v07", "h29v06", "h29v07"]
OUT_CSV = os.path.join(FOLDER, "missing_vnp46a1_2023.csv")

# 匹配
PAT = re.compile(
    r"^VNP46A1\.A(?P<year>\d{4})(?P<doy>\d{3})\.(?P<tile>h\d{2}v\d{2})\..*\.h5$",
    re.IGNORECASE
)

def scan_downloaded(folder: str):
    """返回已下载集合：(doy, tile)"""
    downloaded = set()
    for fn in os.listdir(folder):
        if not fn.lower().endswith(".h5"):
            continue
        m = PAT.match(fn)
        if not m:
            continue
        y = int(m.group("year"))
        if y != YEAR:
            continue
        doy = int(m.group("doy"))
        tile = m.group("tile").lower()
        downloaded.add((doy, tile))
    return downloaded

def main():
    downloaded = scan_downloaded(FOLDER)

    missing = []
    for doy in range(1, 366): 
        for tile in TILES:
            if (doy, tile) not in downloaded:
                missing.append({
                    "year": YEAR,
                    "doy": f"{doy:03d}",
                    "tile": tile,
                    "expected_pattern": f"VNP46A1.A{YEAR}{doy:03d}.{tile}.*.*.h5"
                })

    # 导出 CSV
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["year", "doy", "tile", "expected_pattern"])
        writer.writeheader()
        writer.writerows(missing)

    # 控制台摘要
    total_expected = 365 * len(TILES)
    print("==== SUMMARY ====")
    print(f"Folder: {FOLDER}")
    print(f"Expected: {total_expected} files (days × {len(TILES)} tiles)")
    print(f"Found:    {len(downloaded)} files (matched pattern)")
    print(f"Missing:  {len(missing)} files")
    print(f"CSV saved: {OUT_CSV}")

if __name__ == "__main__":
    main()