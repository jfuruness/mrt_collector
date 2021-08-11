from lib_utils import helper_funcs

from .tool import Tool


class BGPGrep(Tool):
    """Parses MRT RIB dumps using bgpgrep"""

    @staticmethod
    def install_deps():
        """Installs dependencies for bgpgrep"""

        if os.path.exists(self.bgpgrep_location):
            return

        logging.warning("Installing bgpgrep now")
        # Run separately to make errors easier to debug
        cmds = ["sudo apt-get install -y ninja-build meson",
                "sudo apt-get install -y libbz2-dev liblzma-dev doxygen"]
        helper_funcs.run_cmds(cmds),
        cmds = ["cd /tmp",
                "rm -rf ubgpsuite",
                "git clone https://git.doublefourteen.io/bgp/ubgpsuite.git",
                "cd ubgpsuite",
                "meson build --buildtype=release",
                "cd build",
                "ninja",
                f"sudo cp bgpgrep /usr/bin/bgpgrep"]
        helper_funcs.run_cmds(cmds)

    @staticmethod
    def parse(path, csv_path):
        """Parses MRT file in path to CSV path"""

        helper_funcs.run_cmds(f"bgpgrep {path} -o {csv_path}")
