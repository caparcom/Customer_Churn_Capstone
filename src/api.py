import time
import requests
import pandas as pd

URL = "https://nces.ed.gov/opengis/rest/services/K12_School_Locations/EDGE_GEOCODE_PUBLICSCH_1920/MapServer/0/query"

def get_json(session, params, timeout=(10, 90), retries=6):
    for attempt in range(1, retries + 1):
        try:
            r = session.get(URL, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            if attempt == retries:
                raise
            sleep_s = min(60, 2 ** attempt)
            print(f"[retry {attempt}/{retries}] {e} -> sleeping {sleep_s}s")
            time.sleep(sleep_s)

def download_layer_csv(out_path="nces_public_schools_2019_20.csv", chunk_size=300):
    with requests.Session() as session:
      
        ids_payload = get_json(session, {
            "where": "1=1",
            "returnIdsOnly": "true",
            "f": "json",
        })
        object_ids = ids_payload.get("objectIds", [])
        object_ids.sort()

        print(f"Found {len(object_ids)} records. Downloading in chunks of {chunk_size}...")

        rows = []
        for i in range(0, len(object_ids), chunk_size):
            chunk = object_ids[i:i + chunk_size]

            payload = get_json(session, {
                "objectIds": ",".join(map(str, chunk)),
                "outFields": "*",
                "returnGeometry": "false",
                "f": "json",
            })

            feats = payload.get("features", [])
            rows.extend([f["attributes"] for f in feats])

            if (i // chunk_size) % 10 == 0:  
                print(f"Fetched {min(i + chunk_size, len(object_ids))}/{len(object_ids)}")

            time.sleep(0.15) 

        df = pd.DataFrame(rows)
        df.to_csv(out_path, index=False)
        print(f"Saved {len(df)} rows -> {out_path}")

if __name__ == "__main__":
    download_layer_csv()
