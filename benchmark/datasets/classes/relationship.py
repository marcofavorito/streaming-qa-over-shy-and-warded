from functools import partial
from operator import itemgetter
from pathlib import Path
from typing import List, Dict, Callable, Set

from benchmark import ORIGINAL_DATASETS_DIR, ORIGINAL_PROGRAMS_DIR
from benchmark.datasets.core import Dataset, DatasetID, DATA_SUBDIR_NAME
from benchmark.datasets.translate import write_lines_for_vadalog, transform_dataset_file_with_header
from benchmark.tools import ToolID
from benchmark.utils.base import remove_dir_or_fail


RELATIONSHIP_DATASET_PATH = ORIGINAL_DATASETS_DIR / "relationship.csv"
RELATIONSHIP_PROGRAM_PATH = ORIGINAL_PROGRAMS_DIR / "relationship.vada"


dataset_handlers: Dict[ToolID, Callable] = {
    ToolID.VADALOG: write_lines_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_NAIVE: write_lines_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE: write_lines_for_vadalog,
    ToolID.DLVE: transform_dataset_file_with_header,
}


def project_row(csv_line: str, indexes: Set[int]) -> str:
    return ",".join(
        map(
            itemgetter(1),
            filter(lambda x: x[0] in indexes, enumerate(csv_line.split(","))),
        )
    ) + "\n"


class RelationshipDataset(Dataset):
    # TODO add programs

    is_partitioned = False

    @classmethod
    def process_dataset(cls, original_dataset_path: Path, output_dir: Path, force: bool = True):
        dataset_name = DatasetID.RELATIONSHIP.value
        output_dataset_dir = output_dir / dataset_name

        indexes = [0, 1, 3]

        for tool in [ToolID.DLVE, ToolID.VADALOG]:
            dataset_handler = dataset_handlers[tool]
            tool_output_dataset_dir = output_dataset_dir / tool.value / DATA_SUBDIR_NAME
            remove_dir_or_fail(tool_output_dataset_dir, force)
            tool_output_dataset_dir.mkdir(parents=True, exist_ok=True)
            output_dataset_file = tool_output_dataset_dir / (dataset_name + ".data")
            dataset_handler(
                original_dataset_path,
                output_dataset_file,
                header=None,
                predicate_name="own",
                row_processor=partial(project_row, indexes=indexes),
            )

    @classmethod
    def process_program(cls, original_program_path: Path, output_path: Path, force: bool = True):
        pass
