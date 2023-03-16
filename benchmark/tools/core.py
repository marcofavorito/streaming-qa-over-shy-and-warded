import contextlib
import datetime
import logging
from abc import ABC, abstractmethod
from enum import Enum
from operator import attrgetter
from pathlib import Path
from typing import Dict, List, Optional

from benchmark.experiments.core import Status, Result, run_cli
from benchmark.registry import ItemRegistry
from benchmark.utils.base import ensure_dict


class ToolID(Enum):
    VADALOG = "vadalog"
    VADALOG_PARSIMONIOUS_NAIVE = "vadalog-parsimonious-naive"
    VADALOG_PARSIMONIOUS_AGGREGATE = "vadalog-parsimonious-aggregate"
    VADALOG_RESUMPTION = "vadalog-resumption"
    VADALOG_PARSIMONIOUS_NAIVE_RESUMPTION = "vadalog-parsimonious-naive-resumption"
    VADALOG_PARSIMONIOUS_AGGREGATE_RESUMPTION = "vadalog-parsimonious-aggregate-resumption"
    DLVE = "dlve"

    def get_dataset_type(self) -> str:
        """
        Get the dataset type for a tool."""
        return self.VADALOG.value if "vadalog" in self.value else self.value


ALL_TOOL_IDS = tuple(map(attrgetter("value"), ToolID))


class Tool(ABC):
    """Interface for tools."""

    def __init__(self, tool_id: ToolID, binary_path: str):
        """
        Initialize the tool.

        :param binary_path: the binary path
        """
        self.__tool_id = tool_id
        self._binary_path = binary_path

    @property
    def tool_id(self) -> ToolID:
        return self.__tool_id

    @property
    def binary_path(self) -> str:
        """Get the binary path."""
        return self._binary_path

    def run(
        self,
        program: Path,
        datasets: List[Path],
        run_config: Optional[Dict] = None,
        timeout: float = 20.0,
        cwd: Optional[str] = None,
        name: Optional[str] = None,
        working_dir: Optional[str] = None,
    ) -> Result:
        """
        Apply the tool to a file.

        :param program: the program
        :param run_config: the list of datasets
        :param run_config: configuration for the tool run
        :param timeout: the timeout in seconds
        :param cwd: the current working directory
        :param name: the experiment name
        :param working_dir: the working dir
        :return: the planning result
        """
        run_config = ensure_dict(run_config)
        args = self.get_cli_args(program, datasets, run_config, working_dir)
        logging.info("Running command: %s", " ".join(map(str, args)))
        stdout_file = Path(working_dir) / "stdout.txt"
        stderr_file = Path(working_dir) / "stderr.txt"
        timestamp = datetime.datetime.now()
        returncode, total, timed_out, interrupted = run_cli(args, timeout, cwd, logging, stdout_file, stderr_file)

        result = self.collect_statistics(stdout_file.read_text())
        result.name = name
        result.tool = self.tool_id.value
        result.timestamp = timestamp
        result.command = args

        # in case time end2end not set by the tool, set from command
        if result.time_end2end is None:
            result.time_end2end = total

        if interrupted:
            result.status = Status.INTERRUPTED
        elif timed_out:
            result.status = Status.TIMEOUT
        elif result.status is None or returncode != 0:
            result.status = Status.ERROR

        return result

    @abstractmethod
    def collect_statistics(self, output: str) -> Result:
        """
        Collect statistics.

        :param output: the output from where to extract statistics.
        :return: statistics
        """

    @abstractmethod
    def get_cli_args(
        self,
        program: Path,
        datasets: List[Path],
        run_config: Dict,
        working_dir: Optional[str] = None,
    ) -> List[str]:
        """Get CLI arguments."""

    def start_session(self, working_dir: Path) -> None:
        """Start session."""

    def end_session(self) -> None:
        """End session."""

    @contextlib.contextmanager
    def session(self, working_dir: Path):
        self.start_session(working_dir)
        yield
        self.end_session()


class ToolRegistry(ItemRegistry[ToolID, Tool]):
    """Tool registry."""

    item_id_cls = ToolID
