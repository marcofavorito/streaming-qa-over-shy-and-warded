from typing import Dict

import matplotlib

from benchmark.datasets import DatasetID
from benchmark.tools import ToolID

DLV = "dlv"
RDFOX = "rdfox"
LLUNATIC = "llunatic"


def setup_matplotlib():
    matplotlib.rcParams["ps.useafm"] = True
    matplotlib.rcParams["pdf.use14corefonts"] = True
    matplotlib.rcParams["text.usetex"] = True
    matplotlib.rcParams["hatch.linewidth"] = 0.1
    font = {"size": 12}
    matplotlib.rc("font", **font)


COLORS: Dict[str, str] = {
    "vadalog": "dodgerblue",
    "vadalog-parsimonious-naive": "violet",
    "vadalog-parsimonious-aggregate": "darkorange",
    "vadalog-resumption": "blue",
    "vadalog-parsimonious-naive-resumption": "purple",
    "vadalog-parsimonious-aggregate-resumption": "red",
    "dlve": "green",
    "dlv": "lightgreen",
    "rdfox": "gold",
    "llunatic": "grey",
}

MARKERS: Dict[str, str] = {
    "vadalog": "*",
    "vadalog-parsimonious-naive": ".",
    "vadalog-parsimonious-aggregate": "D",
    "vadalog-resumption": "h",
    "vadalog-parsimonious-naive-resumption": "o",
    "vadalog-parsimonious-aggregate-resumption": "s",
    "dlve": "X",
    "rdfox": "triangleleft",
    "llunatic": "triangleright",
}


TOOL_NAMES: Dict[str, str] = {
    ToolID.VADALOG.value: "Vadalog-I",
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE.value: "Vadalog-P",
    ToolID.VADALOG_RESUMPTION.value: "Vadalog-IR",
    ToolID.VADALOG_PARSIMONIOUS_AGGREGATE_RESUMPTION.value: "Vadalog-PR",
    ToolID.DLVE.value: "DLV$^\exists$",
    RDFOX: "RDFox",
    DLV: "DLV",
    LLUNATIC: "Llunatic",
}

DATASET_NAMES: Dict[str, str] = {
    DatasetID.DBPEDIA_STRONGLINK2.value: "DBPedia Strong Link",
    DatasetID.DOCTORS.value: "Doctors",
    DatasetID.DOCTORS_FD.value: "Doctors-FD",
    DatasetID.LUBM.value: "LUBM",
}
