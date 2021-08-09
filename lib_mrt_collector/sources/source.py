class Source:
    """Base class for a source for MRT RIB dumps"""

    sources = set()

    # https://stackoverflow.com/a/43057166/8903959
    def __init_subclass__(cls, **kwargs):
        """Overrides initializing subclasses"""

        super().__init_subclass__(**kwargs)
        for attr in ["url", "value", "get_urls"]:
            Source.sources.add(cls)

        values = list(source.value for source in Source.sources)
        assert len(set(values)) == len(values), "Must have unique values"
