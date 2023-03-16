from abc import ABC, abstractmethod
from enum import Enum
from operator import attrgetter
from pathlib import Path
from typing import List, Optional, Dict

from benchmark import DATASETS_DIR
from benchmark.registry import ItemRegistry
from benchmark.tools import ToolID

SHUTDOWN_TIMEOUT = 20.0

DATA_SUBDIR_NAME = "data"
QUERIES_SUBDIR_NAME = "queries"
DEFAULT_QUERY_FILENAME = "program.txt"


class DatasetID(Enum):
    COMPANY_CONTROL = "company-control"
    DBPEDIA_PSC = "dbpedia-psc"
    DBPEDIA_STRONGLINK = "dbpedia-stronglink"
    DBPEDIA_STRONGLINK2 = "dbpedia-stronglink2"
    DOCTORS = "doctors"
    DOCTORS_FD = "doctors-fd"
    LUBM = "lubm"
    ONTOLOGY_256 = "ontology-256"
    RELATIONSHIP = "relationship"
    HAS_ANCESTOR = "has-ancestor"
    STB_128 = "stb-128"
    SYNTH_A = "synth-a"
    SYNTH_B = "synth-b"
    SYNTH_C = "synth-c"
    SYNTH_D = "synth-d"
    SYNTH_E = "synth-e"


ALL_DATASET_IDS = tuple(map(attrgetter("value"), DatasetID))


class Dataset(ABC):
    """Base class for datasets."""

    is_partitioned: bool
    is_program_partitioned: bool = False

    def __init__(self, dataset_id: DatasetID, path: Optional[Path] = None) -> None:
        self.__dataset_id = dataset_id
        self.__path = path if path is not None else DATASETS_DIR / self.dataset_id.value

    @classmethod
    def _check_cls_attribute(cls):
        mandatory_attributes = ["is_partitioned", "is_program_partitioned", "is_single_program"]
        for mandatory_attr in mandatory_attributes:
            assert hasattr(cls, mandatory_attr), f"dataset class does not specify attribute {mandatory_attributes}"

    @property
    def dataset_id(self) -> DatasetID:
        return self.__dataset_id

    @property
    def path(self) -> Path:
        return self.__path

    def get_dataset_paths(self, tool_id: ToolID) -> List[Path]:
        """
        Return the list of directories to consider as input datasets, for a certain tool.

        A path in the returned list is a directory containing files, each associated to some predicate

        The list denotes the fact that there might be different versions of the same dataset
          (e.g. different partitions). In that case, the name of the element pointed by the path
          determines the "identifier" of such version.

        The dataset partitions should be sorted in terms of difficulty (i.e. size) of the task.
        """
        if self.is_partitioned:
            return sorted((self.path / tool_id.get_dataset_type() / DATA_SUBDIR_NAME).iterdir())
        return [self.path / tool_id.get_dataset_type() / DATA_SUBDIR_NAME]

    def get_program_paths(self, tool_id: ToolID) -> List[Path]:
        """
        Return the list of directories to consider as input programs, for a certain tool and dataset.

        A path in the returned list is a directory containing files, each associated to some query.
        """
        return list((self.path / tool_id.value / QUERIES_SUBDIR_NAME).iterdir())

    @classmethod
    @abstractmethod
    def process_dataset(cls, original_dataset_path: Path, output_path: Path, force: bool = True):
        """
        Process a dataset in its original format.

        If force=False, a prompt is shown to the user whether the output path should be deleted.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def process_program(cls, original_program_path: Path, output_path: Path, force: bool = True):
        """
        Process a program in its original format.

        If force=False, a prompt is shown to the user whether the output path should be deleted.
        """
        raise NotImplementedError

    @classmethod
    def get_run_config(cls, tool_id: ToolID, dataset_path: Path) -> Dict:
        """Get running config."""
        return cls.get_vadalog_bind_strings(list(dataset_path.iterdir())) if "vadalog" in tool_id.value else {}

    @classmethod
    def get_vadalog_bind_strings(cls, paths: List[Path]) -> Dict:
        return {
            "binds": [
                f"{dataset_file.stem}:csv:{dataset_file.absolute()}"
                for dataset_file in paths
            ]
        }


class DatasetRegistry(ItemRegistry[DatasetID, Dataset]):
    """Dataset registry."""

    item_id_cls = DatasetID
