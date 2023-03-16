#!/usr/bin/env python3
from pathlib import Path
from typing import List

import click
import matplotlib.pyplot as plt
import numpy as np

from benchmark import DATASETS_DIR
from benchmark.datasets import dataset_registry
from benchmark.datasets.core import ALL_DATASET_IDS
from benchmark.experiments.core import Status
from benchmark.log_parsing import load_results
from benchmark.plots.base import setup_matplotlib, COLORS, MARKERS, TOOL_NAMES, DATASET_NAMES
from benchmark.utils.base import remove_dir_or_fail, itersubdir, human_format

setup_matplotlib()


MARKER_CONFIGS = dict(
    markersize=8.0, markeredgewidth=0.2, markeredgecolor=(0.0, 0.0, 0.0, 0.9)
)


TIMEOUT = 600


def get_partitions(dataset_name: str):
    dataset_dir = DATASETS_DIR / dataset_name
    tool_dir = dataset_dir / "dlve"
    return sorted([p.name for p in itersubdir(tool_dir / "data")])


@click.command("scalability-plot")
@click.argument(
    "results-dir", type=click.Path(exists=True, file_okay=False, dir_okay=True)
)
@click.option("--output-dir", type=click.Path(file_okay=False, dir_okay=True), default="output")
@click.option(
    "--dataset",
    type=click.Choice(ALL_DATASET_IDS),
    required=True,
    multiple=True
)
@click.option("--timeout", type=int, default=TIMEOUT)
def scalability_plot(results_dir: str, output_dir: str, dataset: List[str], timeout: int):
    results_dir = Path(results_dir)
    output_dir = Path(output_dir)
    if output_dir.exists():
        remove_dir_or_fail(output_dir, True)
    output_dir.mkdir(parents=True)

    allowed_datasets = set(dataset)

    df = load_results(results_dir)

    for dataset, dataset_df in df.groupby("name"):
        dataset_obj = dataset_registry.make(dataset)
        if dataset not in allowed_datasets:
            print(f"dataset {dataset} not chosen; skipping...")
            continue
        if not dataset_obj.is_partitioned:
            print("Ignoring dataset since it not partitioned ", dataset)
            continue
        partitions = get_partitions(dataset)
        max_nb_rows = len(partitions)
        x_axis = [int(p) for p in partitions]

        if dataset == "lubm":
            xlabels = [human_format(int(partition) * 1000) for partition in sorted(partitions)]
        else:
            xlabels = [human_format(int(partition)) for partition in sorted(partitions)]

        for tool, tool_df in dataset_df.groupby("tool"):
            if "naive" in tool: continue
            partition_to_times = {}
            by_partition = dict(list(tool_df.groupby("partition")))
            for partition in partitions:
                if int(partition) not in by_partition:
                    partition_to_times[partition] = [timeout]
                else:
                    partition_df = by_partition[int(partition)]
                    partition_times = []
                    for program, program_df in partition_df.groupby("program"):
                        statuses = program_df["status"].unique()
                        if Status.TIMEOUT.value in statuses:
                            median_time = timeout
                        else:
                            assert statuses == [Status.SUCCESS.value]
                            median_time = program_df[program_df["status"] == Status.SUCCESS.value]["time_end2end"].median()
                        partition_times.append(median_time)
                    partition_to_times[partition] = partition_times

            times = [min(np.mean(partition_to_times[k]), timeout) for k in sorted(partition_to_times.keys())]
            times = times + ([timeout] * (max_nb_rows - len(times)))
            plt.plot(
                x_axis,
                times,
                label=TOOL_NAMES[tool],
                color=COLORS[tool],
                linestyle="-",
                # linewidth=line_width(dataset_name, label),
                marker=MARKERS[tool],
                **MARKER_CONFIGS
            )

        output_file = output_dir / dataset
        # plt.hlines(y=timeout, xmin=x_axis[0], xmax=x_axis[-1], linestyle=":", color="black")
        # plt.yscale("log")
        plt.xscale("log")
        plt.xticks(x_axis, labels=xlabels)
        plt.xlim(x_axis[0], x_axis[-1])
        lgd = plt.legend(loc="upper center")
        plt.xlabel("Source instance size")
        plt.ylabel("Time (seconds)")
        plt.title(DATASET_NAMES[dataset])
        plt.grid()
        plt.savefig(output_file.with_suffix(".pdf"), bbox_inches="tight")
        plt.savefig(output_file.with_suffix(".svg"), bbox_inches="tight")
        plt.clf()


if __name__ == "__main__":
    scalability_plot()
