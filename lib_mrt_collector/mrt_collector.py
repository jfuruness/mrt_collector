from datetime import datetime
import logging
from multiprocessing import cpu_count
import os
import re
from urllib.parse import quote

from tqdm import tqdm

from lib_utils import helper_funcs, file_funcs

from .sources import Source
from .tools import BGPGrep

class MRTCollector:
    """This class downloads, parses, and stores MRT Rib dumps

    NOTE: this library uses https://git.doublefourteen.io/bgp/ubgpsuite
    The creator of bgpscanner moved on to this library since Isolario
    reached it's end of life. bgpscanner contained bugs that were never
    fixed
    """

    bgpgrep_location = "/usr/bin/bgpgrep"
    # In /ssd for speed
    base_mrt_path = "/ssd/mrts_raw"
    base_csv_path = "/ssd/csvs"

    def __init__(self, dl_procs=cpu_count() * 4, parse_procs=cpu_count()):
        self.dl_procs = dl_procs
        self.parse_procs = parse_procs
        # Make base folders
        for path in [self.base_mrt_path, self.base_csv_path]:
            if not os.path.exists(path):
                file_funcs.makedirs(path)
        # Make currently used folders
        now = str(datetime.now()).replace(" ", "")
        self.mrt_path = os.path.join(self.base_mrt_path, now)
        self.csv_path = os.path.join(self.base_csv_path, now)
        for path in [self.mrt_path, self.csv_path]:
            file_funcs.makedirs(path)
        # Install dependencies
        self._install_deps()

    def run(self,
            dl_time=None,
            IPv4=True,
            IPv6=False,
            sources=Source.sources.copy(),
            tool=BGPGrep):
        """Downloads and parses the latest RIB dumps from sources.

        First all downloading is done so as to efficiently multiprocess
        the parsing. This was found to have significant speedup.

        In depth explanation in readme
        """

        urls, mrt_paths, csv_paths = self._get_mrt_data(sources, dl_time)
        try:
            # Get downloaded instances of mrt files using multithreading
            self._download(urls, mrt_paths)
            # Parses files using multiprocessing in descending order by size
            self._parse_to_csvs(mrt_paths, csv_paths)
            input("Done. Press enter to iterate over csvs and split lines")
            for fname in tqdm(csv_paths, total=len(csv_paths)):
                with open(fname, "r") as f:
                    for l in f:
                        l.split("|")
            input("Check how long it takes to iterate over CSV files")
            input("Do post processing here")
        # So much space, always clean up
        finally:
            file_funcs.delete_paths([self.mrt_path, self.csv_path])

    def _get_mrt_data(self, sources: list, dl_time: datetime):
        """Gets MRT URLs for downloading, and their associated paths"""

        # Gets urls of all mrt files needed
        urls = self._get_mrt_urls(sources, dl_time)
        # File names. Encode URL, then replace slashes
        fnames = [quote(url).replace("/", "_") for url in urls]
        assert len(set(fnames)) == len(fnames), "file naming scheme is wrong"
        mrt_paths = [os.path.join(self.mrt_path, x) for x in fnames]
        csv_paths = [os.path.join(self.csv_path, x) for x in fnames]

        return urls, mrt_paths, csv_paths

    def _get_mrt_urls(self, sources, dl_time) -> list:
        """Gets caida and iso URLs, start and end should be epoch"""

        logging.info(f"Sources: {[x.__class__.__name__ for x in sources]}")
        if dl_time is None:
            dl_time = datetime.utcnow()
            # Make twos day before at 00am so that all collectors work
            # Some save every 2hrs. Some save every day. Some save once/day
            dl_time = dl_time.replace(day=dl_time.day - 2, hour=0, second=0)

        urls = list()
        for source in sources:
            urls.extend(source.get_urls(dl_time))
        return urls

    def _download(self, urls: list, paths: list) -> list:
        """Downloads MRT files in parallel"""

        with helper_funcs.Pool(processes=self.dl_procs) as pool:
            # Multiprocess call while displaying to a progress bar
            list(tqdm(pool.imap(file_funcs.download_file, urls, paths),
                      total=len(urls),
                      desc="Downloading MRTs"))

    def _parse_to_csvs(self, mrt_paths, csv_paths, tool):
        """Processes files in parallel and inserts into db"""

        # Make sure to install deps
        tool.install_deps()
        # Sort the paths to parse the largest first
        paths = self._sorted_paths(mrt_paths)
        with helper_funcs.Pool(processes=self.parse_procs) as pool:
            # Multiprocess call while displaying to a progress bar
            list(tqdm(pool.map(tool.parse, paths, csv_paths),
                      total=len(csv_paths),
                      desc="Parsing MRTs"))

    def _sorted_paths(self, mrt_paths):
        """Returns MRT paths in sorted order from largest to smallest"""

        sizes = [os.path.getsize(x) for x in mrt_paths]
        return [x[0] for x in sorted(zip(mrt_paths, sizes),
                                     reverse=True,
                                     key=lambda x: x[1])]
