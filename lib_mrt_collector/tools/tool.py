class Tool:
    """Base class for a tool for parsing MRT RIB dumps"""

    tools = list()

    # https://stackoverflow.com/a/43057166/8903959
    def __init_subclass__(cls, **kwargs):
        """Overrides initializing subclasses"""

        super().__init_subclass__(**kwargs)
        for attr in ["install_deps", "parse"]:
            assert hasattr(cls, attr)
        Tool.tools.append(cls)
