import argparse
import dataclasses
import json
import logging
import os
import signal
import subprocess
import time
from json import JSONDecodeError
from pathlib import Path
from typing import Dict, List, Mapping, Optional

import requests

from benchmark import ROOT_DIR
from benchmark.experiments.core import Status, Result
from benchmark.tools.core import Tool, ToolID
from benchmark.utils.base import from_dict_to_key_equal_value
from benchmark.utils.jvm import JVMConfig, _get_max_default_heap_size_mb

DEFAULT_JAVA_HOME = (
    Path(os.getenv("HOME")) / ".sdkman" / "candidates" / "java" / "current"
)
DEFAULT_VADALOG_ROOT = ROOT_DIR / "third_party" / "vadalog-engine"
DEFAULT_VADALOG_SERVER_TIMEOUT = 30.0
DEFAULT_VADALOG_URL = "http://localhost:8080"
VADALOG_WRAPPER_PATH = ROOT_DIR / "bin" / "vadalog-wrapper"

DEFAULT_JAVA_CONFIG = dict(
    maximum_heap_size=_get_max_default_heap_size_mb(),
    initial_heap_size=None
)


class VadalogTool(Tool):
    """Implement the Vadalog tool wrapper."""

    NAME = "Vadalog"

    def __init__(self, tool_id: ToolID, binary_path: str, properties: Optional[Mapping] = None, java_config: Optional[Mapping] = None) -> None:
        super().__init__(tool_id, binary_path)
        self.properties = properties if properties else dict()
        java_config = java_config if java_config else DEFAULT_JAVA_CONFIG
        self.jvm_config = JVMConfig(**java_config)

        self.vadalog_server: Optional[_VadalogServer] = None

    def collect_statistics(self, output: str) -> Result:
        try:
            json_output = json.loads(output)
            result_set = json_output["resultSet"]
            result_sets = list(result_set.items())
            if len(result_sets) == 0:
                nb_values = 0
            else:
                nb_values = len(result_sets[0][1])
            return Result(status=Status.SUCCESS, nb_atoms=nb_values)
        except json.JSONDecodeError:
            return Result(status=Status.ERROR)

    def get_cli_args(
        self,
        program: Path,
        datasets: List[Path],
        run_config: Dict,
        working_dir: Optional[str] = None,
    ) -> List[str]:
        bind_parameters: List[str] = run_config["binds"]
        args = [self.binary_path, "--program", program]
        if len(bind_parameters) > 0:
            args += ["--bind", *bind_parameters]
        if working_dir is not None:
            args += ["--working-dir", working_dir]
        if self.properties:
            args += [
                "--set",
                *from_dict_to_key_equal_value(
                    tuple(self.properties.items()), separator=" "
                ).split(" "),
            ]
        return args

    def start_session(self, working_dir: Path) -> None:
        if self.vadalog_server is not None:
            return
        self.vadalog_server = _VadalogServer(working_dir, jvm_config=self.jvm_config)
        self.vadalog_server.start()

    def end_session(self) -> None:
        if not self.vadalog_server.is_running:
            return
        self.vadalog_server.stop()


class _VadalogServer:
    def __init__(
        self,
        working_dir: Path,
        java_home: Path = DEFAULT_JAVA_HOME,
        vadalog_root: Path = DEFAULT_VADALOG_ROOT,
        vadalog_timeout: float = DEFAULT_VADALOG_SERVER_TIMEOUT,
        jvm_config: Optional[JVMConfig] = None,
    ):
        self.working_dir = working_dir
        self.java_home = java_home
        self.vadalog_root = vadalog_root
        self.vadalog_server: Optional[subprocess.Popen] = None
        self.vadalog_timeout = vadalog_timeout
        self.jvm_config = jvm_config if jvm_config is not None else JVMConfig()

    @property
    def java_bin(self) -> Path:
        """Get the java binary."""
        return self.java_home / "bin" / "java"

    @property
    def is_running(self) -> bool:
        return self.vadalog_server is not None

    def start(self):
        if self.is_running:
            return
        logging.info("Starting Vadalog engine server...")
        cmd = [str(self.java_bin), *self.jvm_config.to_cli_config(), "-jar", "target/VadaEngine-1.14.0.jar"]
        logging.info("Running command: %s", " ".join(cmd))

        vadalog_server_output_file = self.working_dir / "vadalog-output.log"
        with vadalog_server_output_file.open(mode="w") as fout:
            self.vadalog_server = subprocess.Popen(
                cmd,
                cwd=str(self.vadalog_root),
                stdout=fout,
                stderr=fout,
            )
            logging.info("Wait until Vadalog server is healthy...")
            try:
                self.wait_until_up()
                logging.info("Vadalog is ready!")
            except TimeoutError:
                self._stop()
                raise

    def stop(self):
        if not self.is_running:
            return
        self._stop()

    def _stop(self):
        logging.info("Stopping Vadalog engine server...")
        self.vadalog_server.terminate()
        try:
            self.vadalog_server.wait(timeout=self.vadalog_timeout)
        except subprocess.TimeoutExpired:
            if self.vadalog_server.poll() is None:
                os.kill(self.vadalog_server.pid, signal.SIGKILL)
        finally:
            self.vadalog_server = None
        logging.info("Stopping completed.")

    def wait_until_up(self, timeout: float = 1.0, attempts=20):
        for i in range(attempts):
            try:
                response = requests.get(DEFAULT_VADALOG_URL)
                response.json()
                return
            except (requests.ConnectionError, JSONDecodeError):
                time.sleep(timeout)
        raise TimeoutError("Vadalog engine does not respond")


@dataclasses.dataclass(frozen=True)
class Bind:
    predicate_name: str
    dataset_format: str
    dataset_path: Path

    def to_input_statement(self) -> str:
        return f'@input("{self.predicate_name}").'

    def to_vadalog_statement(self) -> str:
        return f'@bind("{self.predicate_name}", "{self.dataset_format}", "{self.dataset_path.parent}", "{self.dataset_path.name}").'


def parse_bind_type(arg: str) -> Bind:
    """
    Argparse validator for bind parameters.

    A bind parameter has the form:

        predicate_name:data_format:dataset_path

    e.g.:

        own:csv:/path/to/company_control/relationships.csv

    :param arg: the argument.
    :return: the predicate name, the format, the dataset path.
    """
    tokens = arg.split(":")
    if len(tokens) != 3:
        raise argparse.ArgumentTypeError(
            f"expected 3 tokens, got {len(tokens)}: {tokens}"
        )
    predicate_name, dataset_format, dataset_path = tokens
    dataset_path = Path(dataset_path).absolute()
    if not dataset_path.is_file():
        raise argparse.ArgumentTypeError(
            f"the dataset path provided is not a file: {dataset_path}"
        )
    return Bind(predicate_name, dataset_format, dataset_path)
