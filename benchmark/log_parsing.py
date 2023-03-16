from pathlib import Path

import pandas as pd

from benchmark.utils.base import itersubdir, TSV_FILENAME


def load_results(output_dir: Path):
    df_all = pd.DataFrame([])
    for dataset_dir in itersubdir(output_dir):
        for tool_dir in itersubdir(dataset_dir):
            output_file = tool_dir / TSV_FILENAME
            df = pd.read_csv(output_file, sep="\t")
            # todo remove temporary fix
            df["tool"] = tool_dir.name
            df_all = pd.concat([df_all, df])
    return df_all


def load_results_single_dataset(dataset_output_dir: Path):
    df_dataset = pd.DataFrame([])
    for tool_dir in itersubdir(dataset_output_dir):
        output_file = tool_dir / TSV_FILENAME
        df = pd.read_csv(output_file, sep="\t")
        # todo remove temporary fix
        df["tool"] = tool_dir.name
        df_dataset = pd.concat([df_dataset, df])
    return df_dataset
