from datetime import datetime
import logging
from multiprocessing import cpu_count
import os
import re
from urllib.parse import quote

from tqdm import tqdm

from lib_utils.file_funcs import makedirs, download_file, delete_paths
from lib_utils.helper_funcs import mp_call

from .mrt_file import MRTFile
from .po_metadata import POMetadata
from .sources import Source
from .tools import BGPGrep


class MRTCollector:
    """This class downloads, parses, and stores MRT Rib dumps

    NOTE: this library uses https://git.doublefourteen.io/bgp/ubgpsuite
    The creator of bgpscanner moved on to this library since Isolario
    reached it's end of life. bgpscanner contained bugs that were never
    fixed
    """

    def __init__(self,
                 # In /ssd for speed
                 base_mrt_path="/ssd/mrts_raw",
                 base_csv_path="/ssd/mrt_csvs",
                 base_parsed_path="/ssd/meta_csvs",
                 ):
        # Save paths. Done here for easy override
        self.base_mrt_path = base_mrt_path
        self.base_csv_path = base_csv_path
        self.base_parsed_path = base_parsed_path

    def run(self,
            dl_time=None,
            IPv4=True,
            IPv6=False,
            sources=Source.sources.copy(),
            tool=BGPGrep,
            dl_cpus=cpu_count() * 4,
            parse_cpus=cpu_count(),
            max_block_size=2000):
        """Downloads and parses the latest RIB dumps from sources.

        First all downloading is done so as to efficiently multiprocess
        the parsing. This was found to have significant speedup.

        In depth explanation in readme
        """

        mrt_files = self._init_mrt_files(dl_time, sources=sources)
        try:
            # Get downloaded instances of mrt files
            self._download_mrts(mrt_files, dl_cpus=dl_cpus)
            # Saves files to CSVs. csv_paths is sorted for largest first
            self._save_to_csvs(mrt_files, tool=tool, parse_cpus=parse_cpus)
            # Free up some disk space
            delete_paths([self.mrt_path])
            # Parse CSVs. Must be done sequentially for block/prefix/origin id
            self._parse_csvs(mrt_files, max_block_size)
        # So much space, always clean up
        finally:
            delete_paths(mrt_files[0].dirs)

    def _init_mrt_files(self, dl_time, sources=Source.sources.copy()):
        """Gets MRT files for downloading from URLs of sources"""

        logging.info(f"Sources: {[x.__class__.__name__ for x in sources]}")
        # Create default dl_time
        if dl_time is None:
            dl_time = datetime.utcnow()
            # Make twos day before at 00am so that all collectors work
            # Some save every 2hrs. Some save every day. Some save once/day
            dl_time = dl_time.replace(day=dl_time.day - 2, hour=0, second=0)

        paths = self._init_paths(dl_time)

        # Initialize MRT files from URLs of sources
        mrt_files = list()
        for source in sources:
            for url in source.get_urls(dl_time):
                mrt_files.append(MRTFile(url, source, *paths))

        return mrt_files

    def _init_paths(self, dl_time):
        """Gets paths for the current run"""

        dl_time = str(dl_time).replace(" ", "")
        # Get paths
        mrt_path = os.path.join(self.base_mrt_path, dl_time)
        csv_path = os.path.join(self.base_csv_path, dl_time)
        parsed_path = os.path.join(self.base_parsed_path, dl_time)
        paths = [mrt_path, csv_path, parsed_path]
        # Make base folders
        for path in paths:
            for i in range(1, len(path.split("/"))):
                subpath = "/".join(path.split("/")[:i + 1])
                if not os.path.exists(subpath):
                    makedirs(subpath)

        return paths

    def _download_mrts(self, mrts, dl_cpus=cpu_count() * 4):
        """Downloads MRT files from URLs into paths using multiprocessing"""

        mp_call(lambda x: x.download(), [mrts], "Downloading MRTs", cpus=dl_cpus)

    def _save_to_csvs(self, mrt_files, tool=BGPGrep, parse_cpus=cpu_count()):
        """Processes files in parallel and inserts into db"""

        # Make sure to install deps
        tool.install_deps()
        # Sort MRT and CSV paths to parse the largest first
        mp_call(tool.parse, [sorted(mrt_files)], "Extracting", cpus=parse_cpus)

    def _parse_csvs(self, mrt_files, max_block_size):
        """Parses all CSVs"""

        roa_checker = None
        meta_obj = POMetadata(roa_checker, max_block_size)
        from datetime import datetime
        print(datetime.now())
        for mrt_file in mrt_files:#tqdm(reversed(mrt_files), total=len(mrt_files), desc="MRT meta"):
             mrt_file.parse(meta_obj)
        print(datetime.now())
