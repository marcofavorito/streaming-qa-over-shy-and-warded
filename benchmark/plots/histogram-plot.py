#!/usr/bin/env python3
from pathlib import Path

import click
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from benchmark import DATASETS_DIR
from benchmark.datasets import dataset_registry, DatasetID
from benchmark.experiments.core import Status
from benchmark.log_parsing import load_results, load_results_single_dataset
from benchmark.plots.base import setup_matplotlib, COLORS, MARKERS, DLV, LLUNATIC, RDFOX, TOOL_NAMES
from benchmark.tools import ToolID
from benchmark.utils.base import remove_dir_or_fail, itersubdir, human_format

setup_matplotlib()


MARKER_CONFIGS = dict(
    markersize=8.0, markeredgewidth=0.2, markeredgecolor=(0.0, 0.0, 0.0, 0.9)
)


TIMEOUT = 600


TOOL_ORDER = [
    ToolID.VADALOG.value,
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE.value,
    # ToolID.DLVE.value,
    DLV,
    RDFOX,
    LLUNATIC
]

CHASEBENCH_TIMES = {
    DatasetID.STB_128.value: {
        DLV: 48.0,
        RDFOX: 28.0,
        LLUNATIC: 52.0
    },
    DatasetID.ONTOLOGY_256.value: {
        DLV: 118.0,
        RDFOX: 83.0,
        LLUNATIC: 367.0
    }
}


def get_query_average(df: pd.DataFrame):
    times = []
    by_query = dict(list(df.groupby("program")))
    for query_name, query_df in by_query.items():
        statuses = query_df["status"].unique()
        assert set(statuses) == {Status.SUCCESS.value}
        mean_time = query_df["time_end2end"].mean()
        times.append(mean_time)
    return np.mean(times)


@click.command("histogram-plot")
@click.option("--stb-128-dir", type=click.Path(exists=True, file_okay=False, dir_okay=True), required=True)
@click.option("--ontology-256-dir", type=click.Path(exists=True, file_okay=False, dir_okay=True), required=True)
@click.option("--output-dir", type=click.Path(file_okay=False, dir_okay=True), default="output")
@click.option("--timeout", type=int, default=TIMEOUT)
def histogram_plot(stb_128_dir: str, ontology_256_dir: str, output_dir: str, timeout: int):
    stb_128_dir = Path(stb_128_dir)
    ontology_256_dir = Path(ontology_256_dir)
    output_dir = Path(output_dir)
    if output_dir.exists():
        remove_dir_or_fail(output_dir, True)
    output_dir.mkdir(parents=True)

    df_stb128 = load_results_single_dataset(stb_128_dir)
    df_ont256 = load_results_single_dataset(ontology_256_dir)

    x = np.arange(len(TOOL_ORDER))
    width = 0.15

    figs, axs = plt.subplots()

    stb128_times = []
    ont256_times = []

    for tool_idx, tool in enumerate(TOOL_ORDER):
        if tool in {RDFOX, LLUNATIC, DLV}:
            ont256_time = CHASEBENCH_TIMES[DatasetID.ONTOLOGY_256.value][tool]
            stb128_time = CHASEBENCH_TIMES[DatasetID.STB_128.value][tool]
        else:
            ont256_time = get_query_average(df_ont256[df_ont256["tool"] == tool])
            stb128_time = get_query_average(df_stb128[df_stb128["tool"] == tool])

        print(f"Results for tool: {tool}: STB-128={stb128_time}, ONT-256={ont256_time}")
        stb128_times.append(stb128_time)
        ont256_times.append(ont256_time)

    # hatch = tool.HATCH
    group_shift = [0.0] * 2
    stb128_bars = axs.bar(
        x - 0.6 * width,
        stb128_times,
        width,
        label="STB-128",
        color="white",
        linewidth=0.5,
    )
    for b in stb128_bars:
        b.set_edgecolor("darkorchid")
        b.set_hatch("XXXXXX")
    ont256_bars = axs.bar(
        x + 0.6 * width,
        ont256_times,
        width,
        label="ONT-256",
        color="white",
        linewidth=0.5,
    )
    for b in ont256_bars:
        b.set_edgecolor("seagreen")
        b.set_hatch("XXXXXXXXXXXX")

    output_file = output_dir / "ibench"
    lgd = plt.legend(loc="upper center", bbox_to_anchor=(0.5, 1.0), ncol=3)
    plt.xticks(x, labels=list(map(lambda t: TOOL_NAMES[t], TOOL_ORDER)))
    plt.ylabel("Time (seconds)")
    plt.xlabel("Tools")
    plt.title("ChaseBench: STB-128 and ONT-256 scenarios")
    plt.savefig(output_file.with_suffix(".pdf"), bbox_inches="tight")
    plt.savefig(output_file.with_suffix(".svg"), bbox_inches="tight")
    plt.clf()


if __name__ == "__main__":
    histogram_plot()
