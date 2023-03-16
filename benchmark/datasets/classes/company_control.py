import shutil
from pathlib import Path
from typing import List, Dict, Callable

from benchmark import ORIGINAL_DATASETS_DIR, ORIGINAL_PROGRAMS_DIR
from benchmark.datasets.core import Dataset, DatasetID, QUERIES_SUBDIR_NAME, DATA_SUBDIR_NAME, DEFAULT_QUERY_FILENAME
from benchmark.datasets.translate import quote_csv_line, max_digits, get_normalized_integer, \
    process_program_for_vadalog, process_program_for_dlve
from benchmark.tools import ToolID
from benchmark.utils.base import remove_dir_or_fail

COMPANY_CONTROL_DATASET = ORIGINAL_DATASETS_DIR / "company-control"
COMPANY_CONTROL_PROGRAM = ORIGINAL_PROGRAMS_DIR / "company_control.vada"


OWNERSHIP_DATASET_NAMES = [
    "ownerships_10.csv",
    "ownerships_100.csv",
    "ownerships_1000.csv",
    "ownerships_10000.csv",
    "ownerships_50000.csv",
    "ownerships_100000.csv",
]


def transform_ownership_dataset_file(input_file: Path, output_file: Path):
    with input_file.open() as input_file_object, output_file.open(
        mode="w"
    ) as output_file_object:
        atoms = map(
            lambda line: "own(" + quote_csv_line(line) + ").\n", input_file_object
        )
        output_file_object.writelines(atoms)


dataset_handlers: Dict[ToolID, Callable] = {
    ToolID.VADALOG: shutil.copy,
    ToolID.DLVE: transform_ownership_dataset_file,
}


program_handler: Dict[ToolID, Callable] = {
    ToolID.VADALOG: process_program_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_NAIVE: process_program_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE: process_program_for_vadalog,
    ToolID.VADALOG_RESUMPTION: process_program_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_NAIVE_RESUMPTION: process_program_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE_RESUMPTION: process_program_for_vadalog,
    ToolID.DLVE: process_program_for_dlve,
}


class CompanyControlDataset(Dataset):

    is_partitioned = True

    @classmethod
    def process_dataset(cls, original_dataset_path: Path, output_path: Path, force: bool = True):
        dataset_name = DatasetID.COMPANY_CONTROL.value
        output_dataset_dir = output_path / dataset_name

        dataset_max_digits = max_digits(OWNERSHIP_DATASET_NAMES)
        for tool in [ToolID.DLVE, ToolID.VADALOG]:
            dataset_handler = dataset_handlers[tool]
            tool_output_dataset_dir = output_dataset_dir / tool.value / DATA_SUBDIR_NAME
            remove_dir_or_fail(tool_output_dataset_dir, force)
            tool_output_dataset_dir.mkdir(parents=True, exist_ok=True)
            for ownership_dataset_name in OWNERSHIP_DATASET_NAMES:
                size = int(Path(ownership_dataset_name).stem.split("_")[1])
                normalized_partition_name = get_normalized_integer(size, dataset_max_digits)
                tool_output_partition_dir = (
                        tool_output_dataset_dir / normalized_partition_name
                )
                tool_output_partition_dir.mkdir()

                dataset_partition = original_dataset_path / ownership_dataset_name
                output_dataset_file = tool_output_partition_dir / "own.data"
                dataset_handler(dataset_partition, output_dataset_file)

    @classmethod
    def process_program(cls, original_program_path: Path, output_dir: Path, force: bool = True):
        for tool in ToolID:
            output_program_dir = output_dir / DatasetID.COMPANY_CONTROL.value / tool.value / QUERIES_SUBDIR_NAME
            remove_dir_or_fail(output_program_dir, force)
            output_program_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_program_dir / DEFAULT_QUERY_FILENAME
            output_content = program_handler[tool](original_program_path.read_text())
            output_file.write_text(output_content)
