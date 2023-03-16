import re
from pathlib import Path
from typing import Dict, Callable

from benchmark import ORIGINAL_DATASETS_DIR, ORIGINAL_PROGRAMS_DIR
from benchmark.datasets.core import Dataset, DatasetID, DATA_SUBDIR_NAME, QUERIES_SUBDIR_NAME
from benchmark.datasets.translate import write_lines_for_vadalog, transform_dataset_file_with_header, \
    get_normalized_integer, process_program_for_vadalog, process_program_for_dlve, \
    process_program_for_vadalog_set_query
from benchmark.tools import ToolID
from benchmark.utils.base import remove_dir_or_fail

HAS_ANCESTOR_DATASET = ORIGINAL_DATASETS_DIR / "person.csv"
HAS_ANCESTOR_PROGRAM = ORIGINAL_PROGRAMS_DIR / "hasancestor.vada"

dataset_handlers: Dict[ToolID, Callable] = {
    ToolID.VADALOG: write_lines_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_NAIVE: write_lines_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE: write_lines_for_vadalog,
    ToolID.VADALOG_RESUMPTION: write_lines_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_NAIVE_RESUMPTION: write_lines_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE_RESUMPTION: write_lines_for_vadalog,
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

MAX_NB_ANCESTORS = 20


class HasAncestorDataset(Dataset):

    is_partitioned = False
    is_program_partitioned = True

    @classmethod
    def process_dataset(cls, input_path: Path, output_dir: Path, force: bool = True):
        dataset_name = DatasetID.HAS_ANCESTOR.value
        person_dataset_path = input_path
        output_dataset_dir = output_dir / dataset_name

        for tool in ToolID:
            dataset_handler = dataset_handlers[tool]
            tool_output_dataset_dir = output_dataset_dir / tool.value / DATA_SUBDIR_NAME
            remove_dir_or_fail(tool_output_dataset_dir, force)
            tool_output_dataset_dir.mkdir(parents=True, exist_ok=True)
            output_dataset_file = tool_output_dataset_dir / "person.data"
            dataset_handler(
                person_dataset_path,
                output_dataset_file,
                predicate_name="person",
            )

    @classmethod
    def process_program(cls, original_program_path: Path, output_dir: Path, force: bool = True):
        max_digits = len(str(MAX_NB_ANCESTORS))
        for tool in ToolID:
            output_program_dir = output_dir / DatasetID.HAS_ANCESTOR.value / tool.value / QUERIES_SUBDIR_NAME
            remove_dir_or_fail(output_program_dir, force)
            output_program_dir.mkdir(parents=True, exist_ok=True)
            for size in range(2, MAX_NB_ANCESTORS + 1):
                query_name = "q" + get_normalized_integer(size, max_digits)
                original_program = original_program_path.read_text()
                original_program = re.sub("^q.*", "", original_program, flags=re.MULTILINE)
                original_program += "\n" + cls.generate_hasanchestor_query(size)

                output_content = program_handler[tool](original_program, "q")
                output_file = output_program_dir / (query_name + ".txt")
                output_file.write_text(output_content)

    @classmethod
    def generate_hasanchestor_query(cls, nb_ancestors: int) -> str:
        result = f"q(X0) :- person(X0),hasAncestor(X0,X1),"
        for i in range(1, nb_ancestors):
            first, second = f"X{i}", f"X{i+1}"
            result = result + f"hasAncestor({first}, {second}),"
        return result[:-1] + "."
