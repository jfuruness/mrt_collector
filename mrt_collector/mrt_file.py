from pathlib import Path
import os
import shutil
import time
from urllib.parse import quote
import warnings

import requests

from .sources import Source


class MRTFile:
    def __init__(
        self,
        url: str,
        source: Source,
        raw_dir: Path,
        parsed_dir: Path,
        prefixes_dir: Path,
        formatted_dir: Path,
    ) -> None:
        self.url: str = url
        self.source: Source = source
        self.raw_path: Path = raw_dir / self._url_to_fname(self.url)
        self.parsed_path_psv: Path = parsed_dir / self._url_to_fname(self.url, ext="psv")
        # NOTE: This isn't always filled with a file, but is sometimes useful
        self.parsed_path_json: Path = parsed_dir / self._url_to_fname(self.url, ext="json")
        # self.prefixes_path: Path = prefixes_dir
        # self.formatted_path: Path = formatted_dir

    def download_raw(self, retries: int = 3) -> None:
        """Downloads the raw file if you haven't already"""

        if self.downloaded:
            return

        # I tried using proper backoff strategies, such as:
        # https://stackoverflow.com/a/35504626/8903959
        # But this actually doesn't capture incomplete read
        # errors in URL lib. So I need to write my own.
        status_code = 0
        for i in range(retries):
            try:
                with requests.get(self.url, stream=True, timeout=60) as r:
                    status_code = r.status_code
                    if r.status_code == 200:
                        with self.raw_path.open("wb") as f:
                            shutil.copyfileobj(r.raw, f)  # type: ignore
                            return
            except Exception as e:
                if status_code == 404:
                    print(f"URL {self.url} failed due to 404 {i + 1}/{retries}")
                else:
                    print(f"URL {self.url} failed due to {e} {type(e)} {i + 1}/{retries}")
                if i == retries - 1:
                    raise

            time.sleep((i + 1) * 10)

        # Don't error, sometiems files fail due to not found errors (404)
        warnings.warn(
            f"status of {status_code} for {self.url}, download failed but didn't err"
        )

    def _url_to_fname(self, url: str, ext: str = "") -> str:
        """Converts a URL into a file name"""

        # precede with non_url so that bgp dump tools don't mistake it for one
        fname = "non_url" + quote(self.url).replace("/", "_")
        if ext:
            fname = fname.replace(".gz", ext).replace(".bz2", ext)
            # The base without the extension
            base_name = os.path.splitext(fname)[0]
            fname = f"{base_name}.{ext}"
        return fname

    @property
    def downloaded(self) -> bool:
        """Returns True if file downloaded else False"""

        return self.raw_path.exists()
