import re
from pathlib import Path
from typing import List, Dict, Callable

from benchmark import ORIGINAL_DATASETS_DIR, ORIGINAL_PROGRAMS_DIR, CHASEBENCH_SCENARIOS
from benchmark.datasets.core import Dataset, DATA_SUBDIR_NAME, QUERIES_SUBDIR_NAME, DatasetID
from benchmark.datasets.translate import write_lines_for_vadalog, transform_dataset_file_with_header, \
    from_str_to_int_with_label, get_normalized_integer, process_program_for_vadalog, process_program_for_dlve, \
    process_program_for_vadalog_with_original_query
from benchmark.tools import ToolID
from benchmark.utils.base import remove_dir_or_fail

DOCTORS_DATASET_DIR = ORIGINAL_DATASETS_DIR / "doctors"
DOCTORS_FD_DATASET_DIR = ORIGINAL_DATASETS_DIR / "doctors-fd"

DOCTORS_PROGRAM_DIR = ORIGINAL_PROGRAMS_DIR / "doctors"
DOCTORS_FD_PROGRAM_DIR = ORIGINAL_PROGRAMS_DIR / "doctors-fd"

partition_names = [
    "1m",
    "10k",
    "100k",
    "500k",
]


dataset_handlers: Dict[ToolID, Callable] = {
    ToolID.VADALOG: write_lines_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_NAIVE: write_lines_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE: write_lines_for_vadalog,
    ToolID.DLVE: transform_dataset_file_with_header,
}


program_handler: Dict[ToolID, Callable] = {
    ToolID.VADALOG: process_program_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_NAIVE: process_program_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE: process_program_for_vadalog,
    ToolID.VADALOG_RESUMPTION: process_program_for_vadalog_with_original_query,
    ToolID.VADALOG_PARSIMONIOUS_NAIVE_RESUMPTION: process_program_for_vadalog_with_original_query,
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE_RESUMPTION: process_program_for_vadalog_with_original_query,
    ToolID.DLVE: process_program_for_dlve,
}


class DoctorsDataset(Dataset):

    is_partitioned = True

    @classmethod
    def process_dataset(cls, input_dir: Path, output_dir: Path, force: bool = True):
        dataset_name = input_dir.name
        output_dataset_dir = output_dir / dataset_name

        for tool in {ToolID.DLVE, ToolID.VADALOG}:
            dataset_handler = dataset_handlers[tool]
            tool_output_dataset_dir = output_dataset_dir / tool.value / DATA_SUBDIR_NAME
            remove_dir_or_fail(tool_output_dataset_dir, force)
            tool_output_dataset_dir.mkdir(parents=True, exist_ok=True)

            # get max digits number
            sizes = map(
                lambda p: from_str_to_int_with_label(p.name), input_dir.iterdir()
            )
            max_nb_digits = len(str(max(sizes)))

            for subdir in input_dir.iterdir():
                partition_name = subdir.name
                full_integer = from_str_to_int_with_label(partition_name)
                normalized_partition_name = get_normalized_integer(
                    full_integer, max_nb_digits
                )
                output_dataset_subdir = tool_output_dataset_dir / normalized_partition_name
                output_dataset_subdir.mkdir(parents=True)
                for dataset_file in subdir.iterdir():
                    output_dataset_file = output_dataset_subdir / (
                            dataset_file.stem + ".data"
                    )
                    dataset_handler(
                        dataset_file,
                        output_dataset_file,
                        header=None,
                        predicate_name=dataset_file.stem,
                    )

    @classmethod
    def process_program(cls, input_dir: Path, output_dir: Path, force: bool = True):
        for tool in ToolID:
            current_output_dir = output_dir / input_dir.name / tool.value / QUERIES_SUBDIR_NAME
            remove_dir_or_fail(current_output_dir, force)
            current_output_dir.mkdir(parents=True, exist_ok=True)
            # we consider only one partition since they are equivalent
            for program in sorted(input_dir.glob(f"program_10kq*.vada")):
                query_name = re.search("q[0-9]+", program.name).group(0)
                chasebench_query_file = CHASEBENCH_SCENARIOS / input_dir.name / "queries" / "10k" / (query_name + ".txt")
                output_content = program_handler[tool](program.read_text(), chasebench_query_file.read_text())
                output_file = current_output_dir / (query_name + ".txt")
                output_file.write_text(output_content)
