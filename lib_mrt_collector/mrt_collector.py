import logging
from multiprocessing import cpu_count

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
        cmds = ["sudo apt-get install -y ninja meson",
                "sudo apt-get install -y libbz2-dev liblzma-dev doxygen",
                "cd tmp",
                "rm -rf ubgpsuite",
                "git clone https://git.doublefourteen.io/bgp/ubgpsuite.git",
                "cd ubgpsuite",
                "meson build",
                "cd build",
                "ninja"]
        helper_funcs.run_cmds(cmds)
            
    def run(self, IPv4=True, IPv6=False, sources=list(MRTSources)):
        """Downloads and parses the latest RIB dumps from sources.

        First all downloading is done so as to efficiently multiprocess
        the parsing. This was found to have significant speedup.

        In depth explanation in readme
        """

        # Gets urls of all mrt files needed
        urls = self._get_mrt_urls(sources)
        logging.debug(f"Total files {len(urls)}")
        # Get downloaded instances of mrt files using multithreading
        mrt_files = self._multiprocess_download(urls)
        # Parses files using multiprocessing in descending order by size
        self._multiprocess_parse_dls(mrt_files)
        self._filter_and_clean_up_db(IPV4, IPV6)

########################
### URL Helper Functions ###
########################

    def _get_mrt_urls(self, sources=list(MRTSources) -> list:
        """Gets caida and iso URLs, start and end should be epoch"""

        logging.info(f"Getting MRT urls for {[x.name for x in sources]}")
        urls = list()
        if MRTSources.RIPE in sources:
            urls += self._get_ripe_urls()
        if MRTSources.ROUTE_VIEWS in sources:
            urls += self._get_route_views_urls()
        if MRTSources.PCH in sources:
            urls += self._get_pch_urls()
        return urls

    def _get_ripe_urls(self):
        """Gets RIPE URLs for MRT RIB dumps"""

        tags = helper_funcs.get_tags(url, "a")
        input(tags)

    def _get_route_views_urls(self):
        """Gets Route Views URLs for MRT RIB dumps"""

        tags = helper_funcs.get_tags(url, "a")
        input(tags)

    def _get_pch_urls(self):
        """Gets Packet Clearing House URLs for MRT RIB dumps"""

        tags = helper_funcs.get_tags(url, "a")
        input(tags)

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

    def parse_files(self, **kwargs):
        warnings.warn(("MRT_Parser.parse_files is depreciated. "
                       "Use MRT_Parser.run instead"),
                      DeprecationWarning,
                      stacklevel=2)
        self.run(self, **kwargs)
