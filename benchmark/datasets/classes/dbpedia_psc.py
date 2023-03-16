from functools import partial
from pathlib import Path
from typing import List, Dict, Callable

from benchmark import ORIGINAL_DATASETS_DIR, ORIGINAL_PROGRAMS_DIR
from benchmark.datasets.core import Dataset, DATA_SUBDIR_NAME, QUERIES_SUBDIR_NAME, DatasetID, DEFAULT_QUERY_FILENAME
from benchmark.datasets.translate import write_lines_for_vadalog, transform_dataset_file_with_header, \
    get_normalized_integer, normalize, normalize_person_dataset_row, process_program_for_vadalog, \
    process_program_for_dlve
from benchmark.tools import ToolID
from benchmark.utils.base import remove_dir_or_fail

DBPEDIA_DATASET_DIR = ORIGINAL_DATASETS_DIR / "dbpedia"
DBPEDIA_PSC_PROGRAM = ORIGINAL_PROGRAMS_DIR / "psc.vada"


SIZES = [1000, 10000, 100000, 500000, 1000000]



dataset_handlers: Dict[ToolID, Callable] = {
    ToolID.VADALOG: write_lines_for_vadalog,
    ToolID.DLVE: transform_dataset_file_with_header,
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


class DBPediaPscDataset(Dataset):

    is_partitioned = True

    @classmethod
    def process_dataset(cls, input_dir: Path, output_dir: Path, force: bool = True):
        dataset_name = DatasetID.DBPEDIA_PSC.value
        full_person_dataset_path = input_dir / "persons_1m.csv"
        companies_kp_dataset_path = input_dir / "dbpedia_companies_kp.csv"
        control_dataset_path = input_dir / "dbpedia_company_control.csv"

        output_dataset_dir = output_dir / dataset_name

        dataset_max_digits = len(str(SIZES[-1]))

        custom_normalize = partial(normalize, nb_https=2)

        for tool in {ToolID.DLVE, ToolID.VADALOG}:
            dataset_handler = dataset_handlers[tool]
            tool_output_dataset_dir = output_dataset_dir / tool.value / DATA_SUBDIR_NAME
            remove_dir_or_fail(tool_output_dataset_dir, force)
            tool_output_dataset_dir.mkdir(parents=True, exist_ok=True)

            for size in SIZES:
                # copy persons
                normalized_partition_name = get_normalized_integer(size, dataset_max_digits)
                tool_output_partition_dir = (
                        tool_output_dataset_dir / normalized_partition_name
                )
                tool_output_partition_dir.mkdir()

                output_dataset_file = tool_output_partition_dir / "person.data"
                dataset_handler(
                    full_person_dataset_path,
                    output_dataset_file,
                    header=None,
                    predicate_name="person",
                    row_processor=normalize_person_dataset_row,
                    skip_lines=3,
                    size=size,
                )

                # copy company_control
                dataset_handler(
                    control_dataset_path,
                    tool_output_partition_dir / "control.data",
                    header=None,
                    predicate_name="control",
                    row_processor=custom_normalize,
                    skip_lines=1,
                    size=size,
                )
                # copy companies_kp
                dataset_handler(
                    companies_kp_dataset_path,
                    tool_output_partition_dir / "keyPerson.data",
                    header=None,
                    predicate_name="keyPerson",
                    row_processor=custom_normalize,
                    skip_lines=1,
                    size=size,
                )

    @classmethod
    def process_program(cls, original_program_path: Path, output_dir: Path, force: bool = True):
        for tool in ToolID:
            output_program_dir = output_dir / DatasetID.DBPEDIA_PSC.value / tool.value / QUERIES_SUBDIR_NAME
            remove_dir_or_fail(output_program_dir, force)
            output_program_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_program_dir / DEFAULT_QUERY_FILENAME
            output_content = program_handler[tool](original_program_path.read_text())
            output_file.write_text(output_content)
