import os
import re
import csv
import time
import requests
from urllib.parse import urljoin, urlparse

# ====== 你需要改的参数 ======
TOKEN = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6InJlYWxsaXV5IiwiZXhwIjoxNzc4MzcxNTMwLCJpYXQiOjE3NzMxODc1MzAsImlzcyI6Imh0dHBzOi8vdXJzLmVhcnRoZGF0YS5uYXNhLmdvdiIsImlkZW50aXR5X3Byb3ZpZGVyIjoiZWRsX29wcyIsImFjciI6ImVkbCIsImFzc3VyYW5jZV9sZXZlbCI6M30.foDx_jZe7GMFb3Ne6-Fs02iRJVZXb0EGDdgMy6hgDAGMULQCPoFRtgxiABfiW-TmTVYaMqprT1REpRTZHRO6KsROLvpM-5LjcvNrKeyK1pCA29a4p3wVeadfRagQuZaF_5Y7VZ8-mfMhgmhB_KqIRveRB4aVQwYxxo5QEPq65RE_bbNweGWEe8NLMgS43zo5O6gGhKUNRCTuXbA7qCC_9vLBIeCNZW07QgR19g7H5EH1VExBX4BovwbL-E5nTJuao0IIzWB4hNXHuLhpOkpsDtXZsD1aafAYezKCQu1NSZVMqGLypZEWVu16xscWXKtVBqFLDr1mEl6XIbND3owLsA"
BASE = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5200/VNP46A1/2023/"
FOLDER = r"D:/cmafiles/L/database/nightime/ntl/raw/VNP46A1_2023"  # 存放目录
CSV_FILE = os.path.join(FOLDER, "missing_vnp46a1_2023.csv")
# ============================

HEADERS = {"Authorization": f"Bearer {TOKEN}"}

H5_RE = re.compile(r'h\d{2}v\d{2}.*\.h5$', re.IGNORECASE)

def list_day_files(day_url: str):
    """列出某一天目录下的所有 h5 文件名"""
    r = requests.get(day_url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    html = r.text
    files = re.findall(r'href="([^"]+\.h5)"', html, flags=re.IGNORECASE)
    return [f for f in files if H5_RE.search(f)]

def download_one(url: str, out_path: str):
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return "skip_exists"

    tmp = out_path + ".part"
    with requests.get(url, headers=HEADERS, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    os.replace(tmp, out_path)
    return "ok"

def main():
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(CSV_FILE)

    os.makedirs(FOLDER, exist_ok=True)

    # 读 CSV
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"Missing records: {len(rows)}")

    ok = skip = err = 0

    for r in rows:
        doy = r["doy"]            # 已经是 001–366
        tile = r["tile"].lower()  # h28v06 等

        day_url = urljoin(BASE, doy + "/")
        print(f"\n== Day {doy}, tile {tile}")

        try:
            files = list_day_files(day_url)
        except Exception as e:
            print(f"  [ERR] list day failed: {e}")
            err += 1
            continue

        # 找到该 tile 的真实文件名
        matches = [f for f in files if tile in f.lower()]

        if not matches:
            print("  [ERR] no matching file on server")
            err += 1
            continue

        # 理论上只有 1 个
        fname = matches[0]
        file_url = urljoin(day_url, fname)

        out_name = os.path.basename(urlparse(file_url).path)
        out_path = os.path.join(FOLDER, out_name)

        try:
            status = download_one(file_url, out_path)
            print(f"  [{status}] {out_name}")
            if status == "ok":
                ok += 1
            else:
                skip += 1
        except Exception as e:
            print(f"  [ERR] download failed: {e}")
            err += 1
            time.sleep(1)

    print("\n==== RE-DOWNLOAD SUMMARY ====")
    print(f"ok: {ok}, skipped: {skip}, errors: {err}")
    print(f"folder: {FOLDER}")

if __name__ == "__main__":
    main()