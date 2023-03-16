import re
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import List, Dict, Callable

from benchmark import ROOT_DIR, ORIGINAL_DATASETS_DIR, ORIGINAL_PROGRAMS_DIR, CHASEBENCH_SCENARIOS
from benchmark.datasets.core import Dataset, DATA_SUBDIR_NAME, QUERIES_SUBDIR_NAME, DatasetID
from benchmark.datasets.translate import write_lines_for_vadalog, transform_dataset_file_with_header, \
    from_str_to_int_with_label, get_normalized_integer, process_program_for_vadalog, process_program_for_dlve, \
    process_program_for_vadalog_with_original_query, process_program_for_vadalog_set_query
from benchmark.tools import ToolID
from benchmark.utils.base import remove_dir_or_fail


LUBM_DATASET_DIR = ORIGINAL_DATASETS_DIR / "lubm"
LUBM_PROGRAM_DIR = ORIGINAL_PROGRAMS_DIR / "lubm"


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
    ToolID.VADALOG_RESUMPTION: process_program_for_vadalog_set_query,
    ToolID.VADALOG_PARSIMONIOUS_NAIVE_RESUMPTION: process_program_for_vadalog_set_query,
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE_RESUMPTION: process_program_for_vadalog_set_query,
    ToolID.DLVE: process_program_for_dlve,
}

partition_names = [
    "01k",
    "001",
    "010",
    "100",
]


def job(output_dataset_subdir, tool_id, dataset_file):
    dataset_handler = dataset_handlers[tool_id]
    print(f"Processing dataset file {dataset_file} for tool {tool_id}")
    output_dataset_file = output_dataset_subdir / (dataset_file.stem + ".data")
    dataset_handler(
        dataset_file,
        output_dataset_file,
        header=None,
        predicate_name=dataset_file.stem,
    )


class LUBMDataset(Dataset):

    is_partitioned = True

    @classmethod
    def process_dataset(cls, input_dir: Path, output_dir: Path, force: bool = True):
        dataset_name = input_dir.name
        output_dataset_dir = output_dir / dataset_name

        for tool in [ToolID.DLVE, ToolID.VADALOG]:
            tool_output_dataset_dir = output_dataset_dir / tool.value / DATA_SUBDIR_NAME
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
                remove_dir_or_fail(output_dataset_subdir, force)
                output_dataset_subdir.mkdir(parents=True, exist_ok=True)
                job_i = partial(job, output_dataset_subdir, tool)
                with Pool() as pool:
                    pool.map(job_i, subdir.iterdir())

    @classmethod
    def process_program(cls, input_dir: Path, output_dir: Path, force: bool = True):
        program = input_dir / "program_01k.vada"
        input_content = program.read_text()
        query_names = re.findall('@output\("(q[0-9]+)"\).', input_content)
        input_content = re.sub("%?@.*", "", input_content)
        for tool in ToolID:
            current_output_dir = output_dir / input_dir.name / tool.value / QUERIES_SUBDIR_NAME
            remove_dir_or_fail(current_output_dir, force)
            current_output_dir.mkdir(parents=True, exist_ok=True)
            for query_name in query_names:
                output_content = input_content + "\n" + f'@output("{query_name}").'
                output_content = program_handler[tool](output_content, query_name)
                output_file = current_output_dir / (query_name+ ".txt")
                output_file.write_text(output_content)
