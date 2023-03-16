#!/usr/bin/env python3
from pathlib import Path

import click
import matplotlib.pyplot as plt

from benchmark import DATASETS_DIR
from benchmark.datasets import DatasetID
from benchmark.experiments.core import Status
from benchmark.log_parsing import load_results
from benchmark.plots.base import setup_matplotlib, COLORS, MARKERS

setup_matplotlib()


MARKER_CONFIGS = dict(
    markersize=6.0, markeredgewidth=0.2, markeredgecolor=(0.0, 0.0, 0.0, 0.9)
)


TIMEOUT = 600


def get_queries():
    dataset_dir = DATASETS_DIR / DatasetID.HAS_ANCESTOR.value
    tool_dir = dataset_dir / "dlve"
    return sorted([p.stem for p in (tool_dir / "queries").iterdir()])


@click.command("has-ancestor-plot")
@click.argument(
    "results-dir", type=click.Path(exists=True, file_okay=False, dir_okay=True)
)
@click.option("--output-dir", type=click.Path(file_okay=False, dir_okay=True), default="output")
@click.option("--timeout", type=int, default=TIMEOUT)
def has_ancestor_plot(results_dir: str, output_dir: str, timeout: int):
    results_dir = Path(results_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = load_results(results_dir)
    assert df["name"].unique() == "has-ancestor"
    queries = get_queries()

    max_nb_rows = len(queries)
    x_axis = list(range(max_nb_rows))
    xlabels = list(map(lambda x: str(x + 2), x_axis))

    for tool, tool_df in df.groupby("tool"):
        times = []
        by_query = dict(list(tool_df.groupby("program")))
        for query in sorted(queries):
            if query not in by_query:
                times.append(timeout)
            else:
                query_df = by_query[query]
                statuses = query_df["status"].unique()
                if Status.TIMEOUT.value in statuses:
                    mean_time = timeout
                else:
                    assert statuses == [Status.SUCCESS.value]
                    mean_time = query_df[query_df["status"] == Status.SUCCESS.value]["time_end2end"].mean()
                times.append(mean_time)
        plt.plot(
            x_axis,
            times,
            label=tool,
            color=COLORS[tool],
            linestyle="-",
            # linewidth=line_width(dataset_name, label),
            marker=MARKERS[tool],
            **MARKER_CONFIGS
        )

    output_file = output_dir / "has-ancestor"
    plt.hlines(y=timeout, xmin=x_axis[0], xmax=x_axis[-1], linestyle=":", color="black")
    plt.yscale("log")
    plt.xticks(x_axis, labels=xlabels)
    plt.xlim(x_axis[0], x_axis[-1])
    lgd = plt.legend(loc="upper center", bbox_to_anchor=(0.5, 1.2), ncols=4)
    plt.xlabel("Source instance size")
    plt.ylabel("Time (seconds)")
    plt.title("has-ancestor")
    plt.grid()
    plt.savefig(output_file.with_suffix(".pdf"), bbox_inches="tight")
    plt.savefig(output_file.with_suffix(".svg"), bbox_inches="tight")
    plt.clf()


if __name__ == "__main__":
    has_ancestor_plot()
