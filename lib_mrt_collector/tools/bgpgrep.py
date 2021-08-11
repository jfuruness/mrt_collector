from lib_utils import helper_funcs

from .tool import Tool


class BGPGrep(Tool):
    """Parses MRT RIB dumps using bgpgrep"""

    install_path = "/usr/bin/bgpgrep"
    apt_deps = ["ninja-build", "meson", "libbz2-dev", "liblzma-dev", "doxygen"]

    @staticmethod
    def _install_deps():
        """Installs dependencies for bgpgrep. Apt deps installed in parent"""

        cmds = ["cd /tmp",
                "rm -rf ubgpsuite",
                "git clone https://git.doublefourteen.io/bgp/ubgpsuite.git",
                "cd ubgpsuite",
                "meson build --buildtype=release",
                "cd build",
                "ninja",
                f"sudo cp bgpgrep {BGPGrep.install_path}"]
        helper_funcs.run_cmds(cmds)

    @staticmethod
    def parse(path, csv_path):
        """Parses MRT file in path to CSV path"""

        helper_funcs.run_cmds(f"{BGPGrep.install_path} -o {csv_path} {path}")
