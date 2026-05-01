from pathlib import Path

from abc import ABC, abstractmethod

class ExportAnalyzer(ABC):
    _registry: dict[str, type["ExportAnalyzer"]] = {}

    def __init_subclass__(
        cls, 
        analyzer_id: str = None, 
        **kwargs
    ):
        """Add our analyzer to the registry for the orchestrator to access"""
        super().__init_subclass__(**kwargs)
        if analyzer_id is not None:
            if analyzer_id in ExportAnalyzer._registry:
                raise ValueError(f"Duplicate analyzer_id: {analyzer_id!r}")
            ExportAnalyzer._registry[analyzer_id] = cls

    @classmethod
    def from_id(
        cls, 
        analyzer_id: str, 
        base_dir: Path
    ) -> "ExportAnalyzer":
        """Access a specific analyzer via a string argument"""
        try:
            return cls._registry[analyzer_id](base_dir)
        except KeyError:
            raise ValueError(
                f"Unknown analyzer: {analyzer_id!r}. "
                f"Available: {sorted(cls._registry)}"
            )

    def __init__(
        self,
        base_dir: Path
    ) -> None:

        self.base_dir = base_dir / "analysis"
        self.uses_bgpy_graph = False

    def set_bgpy_graph(
        self,
        graph
    ) -> None:
        self.bgp_dag = graph

    def set_current_source(
        self,
        source: str
    ) -> None:
        self.current_source = source
    
    # not abstract because not always neccesary
    def post_process(
        self
    )->None:
        """Inherit and extend where needed"""
        pass

    @abstractmethod
    def analyze(
        self,
        row: dict[str]
    ) -> None:
        pass

    @abstractmethod
    def dump_json(
        self
    ) -> None:
        pass
