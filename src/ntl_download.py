import os
import re
import time
import requests
from urllib.parse import urljoin
from urllib.parse import urlparse

# ====== 参数 ======
TOKEN = "TOKEN"
OUTDIR = r"D:\cmafiles\L\database\nighttime\VNP46A2_2024"  # 下载目录
# =================================
BASE = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5200/VNP46A2/2024/"
TILES = {"h28v06", "h28v07", "h29v06", "h29v07"}
H5_RE = re.compile(r'h\d{2}v\d{2}.*\.h5$', re.IGNORECASE)

def list_h5_files(day_url: str, headers: dict):
    r = requests.get(day_url, headers=headers, timeout=60)
    r.raise_for_status()
    html = r.text
    # 从目录页抓所有 .h5 链接
    files = set(re.findall(r'href="([^"]+\.h5)"', html, flags=re.IGNORECASE))
    # 再过滤一下看起来像 h5 的
    return sorted([f for f in files if H5_RE.search(f)])

def is_target_tile(fname: str) -> bool:
    m = re.search(r"(h\d{2}v\d{2})", fname, re.IGNORECASE)
    return (m.group(1).lower() in TILES) if m else False

def safe_filename(fname: str) -> str:
    return fname

def download_one(url: str, out_path: str, headers: dict):
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return "skip_exists"

    tmp = out_path + ".part"
    with requests.get(url, headers=headers, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    os.replace(tmp, out_path)
    return "ok"

def main():
    os.makedirs(OUTDIR, exist_ok=True)

    headers = {"Authorization": f"Bearer {TOKEN}"}

    total_ok = total_skip = total_err = 0

    for d in range(1, 5):  # 2024 闰年
        day = f"{d:03d}"
        day_url = urljoin(BASE, day + "/")
        print(f"\n== Day {day} : {day_url}")

        try:
            files = list_h5_files(day_url, headers)
        except Exception as e:
            print(f"  [ERR] list failed: {e}")
            total_err += 1
            continue

        target = [f for f in files if is_target_tile(f)]
        print(f"  found {len(files)} h5, target {len(target)}")

        for fname in target:
            file_url = urljoin(day_url, fname)

            # 永远用 URL 的 path basename 当文件名（防止 fname 被写成完整URL）
            out_name = os.path.basename(urlparse(file_url).path)
            out_path = os.path.join(OUTDIR, out_name)

            try:
                status = download_one(file_url, out_path, headers)
                print(f"  [{status}] {out_name}")
            except Exception as e:
                print(f"  [ERR] {out_name}: {e}")
                time.sleep(1)

    print("\n==== DONE ====")
    print(f"ok: {total_ok}, skipped: {total_skip}, errors: {total_err}")
    print(f"saved to: {OUTDIR}")

if __name__ == "__main__":
    main()