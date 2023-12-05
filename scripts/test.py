import json
from pathlib import Path

path = Path("/home/anon/mrt_data/2023-11-01/parsed/")
path = path / "non_urlhttp%3A__data.ris.ripe.net_rrc25__2023.11_bview.20231101.json"

with path.open() as f:
    data = json.load(f)
    print(len(data))
