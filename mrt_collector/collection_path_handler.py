from pathlib import Path
from datetime import datetime

def handle_path(
    dl_time: datetime,
    input_path:str = "Use default"
) -> Path:
    
    root = Path.home()

    if input_path is not "Use default":
        root = parse_custom_path(input_path)

    return root / "mrt_data" / dated_dir(dl_time)


def parse_custom_path(input_path: str) -> Path:
    path = Path(input_path)
    
    if not path.is_dir():
        raise ValueError(f"Path is not a directory: {path}")
    
    return path


def dated_dir(dl_time: datetime) -> Path:
    return Path(dl_time.strftime("%Y_%m_%d_%H"))
