from datetime import datetime
import logging
from multiprocessing import cpu_count
import os

from lib_utils import helper_funcs

from .sources import Source

class MRTCollector:
    """This class downloads, parses, and stores MRT Rib dumps

    NOTE: this library uses https://git.doublefourteen.io/bgp/ubgpsuite
    The creator of bgpscanner moved on to this library since Isolario
    reached it's end of life. bgpscanner contained bugs that were never
    fixed
    """

    def __init__(self, dl_threads=cpu_count() * 4, parse_procs=cpu_count()):
        self.dl_threads = dl_threads
        self.parse_procs = parse_procs
        self._install_deps()

    def _install_deps(self):
        if os.path.exists("/usr/bin/bgpgrep"):
            return

        logging.warning("Installing MRT Collector deps now")
        # Run separately to make errors easier to debug
        cmds = ["sudo apt-get install -y ninja-build meson",
                "sudo apt-get install -y libbz2-dev liblzma-dev doxygen"]
        helper_funcs.run_cmds(cmds),
        cmds = ["cd /tmp",
                "rm -rf ubgpsuite",
                "git clone https://git.doublefourteen.io/bgp/ubgpsuite.git",
                "cd ubgpsuite",
                "meson build",
                "cd build",
                "ninja"]
        helper_funcs.run_cmds(cmds)
            
    def run(self,
            dl_time=None,
            IPv4=True,
            IPv6=False,
            sources=Source.sources.copy()):
        """Downloads and parses the latest RIB dumps from sources.

        First all downloading is done so as to efficiently multiprocess
        the parsing. This was found to have significant speedup.

        In depth explanation in readme
        """

        # Gets urls of all mrt files needed
        urls = self._get_mrt_urls(sources, dl_time)
        logging.debug(f"Total files {len(urls)}")
        # Get downloaded instances of mrt files using multithreading
        mrt_files = self._multiprocess_download(urls)
        # Parses files using multiprocessing in descending order by size
        self._multiprocess_parse_dls(mrt_files)
        self._filter_and_clean_up_db(IPV4, IPV6)

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

    def _multiprocess_download(self, dl_threads: int, urls: list) -> list:
        """Downloads MRT files in parallel.

        In depth explanation at the top of the file, dl=download.
        """

        # Creates an mrt file for each url
        mrt_files = [MRT_File(self.path, self.csv_dir, url, i + 1)
                     for i, url in enumerate(urls)]

        with utils.progress_bar("Downloading MRTs, ", len(mrt_files)):
            # Creates a dl pool with 4xCPUs since it is I/O based
            with utils.Pool(dl_threads, 4, "download") as dl_pool:

                # Download files in parallel
                # Again verify is False because Isolario
                dl_pool.map(lambda f: utils.download_file(
                        f.url, f.path, f.num, len(urls),
                        f.num/5, progress_bar=True), mrt_files, verify=False)
        return mrt_files

    def _multiprocess_parse_dls(self,
                                p_threads: int,
                                mrt_files: list,
                                bgpscanner: bool):
        """Multiprocessingly(ooh cool verb, too bad it's not real)parse files.

        In depth explanation at the top of the file.
        dl=download, p=parse.
        """

        with utils.progress_bar("Parsing MRT Files,", len(mrt_files)):
            with utils.Pool(p_threads, 1, "parsing") as p_pool:
                # Runs the parsing of files in parallel, largest first
                p_pool.map(lambda f: f.parse_file(bgpscanner),
                           sorted(mrt_files, reverse=True))

    def _filter_and_clean_up_db(self,
                                IPV4: bool,
                                IPV6: bool,
                                delete_duplicates=False):
        """This function filters mrt data by IPV family and cleans up db

        First the database is connected. Then IPV4 and/or IPV6 data is
        removed. Aftwards the data is vaccuumed and analyzed to get
        statistics for the table for future queries, and a checkpoint is
        called so as not to lose RAM.
        """

        with MRT_Announcements_Table() as _ann_table:
            # First we filter by IPV4 and IPV6:
            _ann_table.filter_by_IPV_family(IPV4, IPV6)
            logging.info("vaccuming and checkpoint")
            # A checkpoint is run here so that RAM isn't lost
            _ann_table.cursor.execute("CHECKPOINT;")
            # VACUUM ANALYZE to clean up data and create statistics on table
            # This is needed for better index creation and queries later on
            _ann_table.cursor.execute("VACUUM ANALYZE;")
