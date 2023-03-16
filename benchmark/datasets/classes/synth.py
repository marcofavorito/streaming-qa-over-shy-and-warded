import re
from abc import ABC
from pathlib import Path
from typing import List, Dict, Callable

from benchmark import ORIGINAL_DATASETS_DIR, ORIGINAL_PROGRAMS_DIR
from benchmark.datasets.core import Dataset, DATA_SUBDIR_NAME, QUERIES_SUBDIR_NAME, DEFAULT_QUERY_FILENAME, DatasetID
from benchmark.datasets.translate import write_lines_for_vadalog, transform_dataset_file, process_program_for_vadalog
from benchmark.tools import ToolID
from benchmark.utils.base import remove_dir_or_fail

SYNTH_DATASET_DIR = ORIGINAL_DATASETS_DIR / "prot"
SYNTH_PROGRAM_DIR = ORIGINAL_PROGRAMS_DIR / "prot"
SYNTH_A_DATASET_DIR = SYNTH_DATASET_DIR / "protA"
SYNTH_B_DATASET_DIR = SYNTH_DATASET_DIR / "protB"
SYNTH_C_DATASET_DIR = SYNTH_DATASET_DIR / "protC"
SYNTH_D_DATASET_DIR = SYNTH_DATASET_DIR / "protD"
SYNTH_E_DATASET_DIR = SYNTH_DATASET_DIR / "protE"
SYNTH_A_PROGRAM_DIR = SYNTH_A_DATASET_DIR / "protA.vada"
SYNTH_B_PROGRAM_DIR = SYNTH_B_DATASET_DIR / "protB.vada"
SYNTH_C_PROGRAM_DIR = SYNTH_C_DATASET_DIR / "protC.vada"
SYNTH_D_PROGRAM_DIR = SYNTH_D_DATASET_DIR / "protD.vada"
SYNTH_E_PROGRAM_DIR = SYNTH_E_DATASET_DIR / "protE.vada"
EDB_DATASET_DIRNAME = "inputCsv"


def process_synth_line_for_dlve(line: str) -> str:
    head, body = line.split(":-")
    variable_names = ["HARMLESS_[0-9]+", "HARMFUL_[0-9]+"]
    variable_regex = "(" + "|".join(variable_names) + ")"

    head_variables = set(re.findall(variable_regex, head))
    body_variables = set(re.findall(variable_regex, body))
    existentially_quantified_vars = head_variables.difference(body_variables)

    if existentially_quantified_vars:
        dlve_exist_prefix = (
            f"#exists{{{','.join(sorted(existentially_quantified_vars))}}}"
        )
        line = dlve_exist_prefix + line
    return line


def process_synth_program_for_dlve(input_file: str) -> str:
    input_file = re.sub("%.*\n", "", input_file)
    input_file = re.sub("@.*\n", "", input_file)

    lines = input_file.splitlines(keepends=False)
    lines = [line for line in lines if line]
    new_lines = map(process_synth_line_for_dlve, lines)
    output = "\n".join(new_lines)

    # generate query.
    output_rules = re.findall("(.*out_[0-9]+)\((.*)\) :-", output)
    # take the first (we only need the number of arguments)
    output_rule = output_rules[0]
    output_predicate = output_rule[0]
    output_variables = output_rule[1].split(",")
    output += "\n" + f"{output_predicate}({','.join(output_variables)})?"
    return output


dataset_handlers: Dict[ToolID, Callable] = {
    ToolID.VADALOG: write_lines_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_NAIVE: write_lines_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE: write_lines_for_vadalog,
    ToolID.DLVE: transform_dataset_file,
}

program_handler: Dict[ToolID, Callable] = {
    ToolID.VADALOG: process_program_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_NAIVE: process_program_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE: process_program_for_vadalog,
    ToolID.VADALOG_RESUMPTION: process_program_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_NAIVE_RESUMPTION: process_program_for_vadalog,
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE_RESUMPTION: process_program_for_vadalog,
    ToolID.DLVE: process_synth_program_for_dlve,
}


class SynthDataset(Dataset, ABC):

    is_partitioned = False
    _synth_dataset_name: str

    @classmethod
    def _get_datset_id(cls) -> str:
        dataset_id = DatasetID(f"synth-{cls._synth_dataset_name[-1].lower()}")
        return dataset_id.value

    @classmethod
    def process_dataset(cls, input_dir: Path, output_dir: Path, force: bool = True):
        input_dataset_dir = SYNTH_DATASET_DIR / cls._synth_dataset_name
        edb_dataset_dir = input_dataset_dir / EDB_DATASET_DIRNAME
        output_dataset_dir = output_dir / cls._get_datset_id()

        for tool in [ToolID.DLVE, ToolID.VADALOG]:
            dataset_handler = dataset_handlers[tool]
            tool_output_dataset_dir = output_dataset_dir / tool.value / DATA_SUBDIR_NAME
            remove_dir_or_fail(tool_output_dataset_dir, force)
            tool_output_dataset_dir.mkdir(parents=True, exist_ok=True)
            for edb_dataset_path in edb_dataset_dir.iterdir():
                new_filename = edb_dataset_path.stem.replace("_csv", "")
                output_dataset_file = tool_output_dataset_dir / (new_filename + ".data")
                dataset_handler(
                    edb_dataset_path,
                    output_dataset_file,
                    predicate_name=new_filename,
                )

    @classmethod
    def process_program(cls, input_dir: Path, output_dir: Path, force: bool = True):
        for tool in ToolID:
            output_program_dir = output_dir / cls._get_datset_id() / tool.value / QUERIES_SUBDIR_NAME
            remove_dir_or_fail(output_program_dir, force)
            output_program_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_program_dir / DEFAULT_QUERY_FILENAME
            output_content = program_handler[tool](input_dir.read_text())
            output_file.write_text(output_content)


class SynthADataset(SynthDataset):

    _synth_dataset_name = "protA"


class SynthBDataset(SynthDataset):
    _synth_dataset_name = "protB"


class SynthCDataset(SynthDataset):
    _synth_dataset_name = "protC"


class SynthDDataset(SynthDataset):
    _synth_dataset_name = "protD"


class SynthEDataset(SynthDataset):
    _synth_dataset_name = "protE"
