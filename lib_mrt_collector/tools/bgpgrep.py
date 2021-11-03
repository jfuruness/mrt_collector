from lib_utils.helper_funcs import run_cmds

from .tool import Tool


class BGPGrep(Tool):
    """Parses MRT RIB dumps using bgpgrep"""

    install_path = "/usr/bin/bgpgrep"
    apt_deps = ("ninja-build", "meson", "libbz2-dev", "liblzma-dev", "doxygen")

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
        run_cmds(cmds)

    @staticmethod
    def parse(mrt):
        """Parses MRT file in path to CSV path"""

        try:
            run_cmds([f"{BGPGrep.install_path} -o {mrt.dumped_path} {mrt.raw_path}"])
        except Exception as e:
            print("bgpgrep had a problem:")
            print(e)
            print("Fix this later. Malformed attributes raise errors - we should still keep these lines. Also remove this except statement")
