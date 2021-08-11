import logging
import os

class Tool:
    """Base class for a tool for parsing MRT RIB dumps"""

    tools = list()

    # https://stackoverflow.com/a/43057166/8903959
    def __init_subclass__(cls, **kwargs):
        """Overrides initializing subclasses"""

        super().__init_subclass__(**kwargs)
        for attr in ["_install_deps", "parse", "install_path", "apt_deps"]:
            assert hasattr(cls, attr), f"{cls.__name__} doesn't have {attr}"
        Tool.tools.append(cls)

    @classmethod
    def install_deps(cls):
        if os.path.exists(cls.install_path):
            return
        else:
            logging.warning("Installing deps now")
            # Install apt dependencies. One at a time for easy debugging
            for apt_dep in cls.apt_deps:
                helper_funcs.run_cmds(f"sudo apt-get install -y {apt_dep}")
            # Install tool from source
            cls._install_deps()
