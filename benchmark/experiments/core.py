import datetime
import logging
import os
import signal
import subprocess
import sys
import time
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict, Any

import psutil

SHUTDOWN_TIMEOUT = 20.0


class Status(Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    INTERRUPTED = "interrupted"
    ERROR = "error"


@dataclass()  # frozen=True
class Result:
    name: Optional[str] = None
    tool: Optional[str] = None
    timestamp: Optional[datetime.datetime] = None
    run_id: Optional[int] = None
    partition: Optional[str] = None
    program: Optional[str] = None
    command: Optional[List[str]] = None
    time_end2end: Optional[float] = None
    status: Optional[Status] = None
    nb_atoms: Optional[int] = None

    @staticmethod
    def headers() -> str:
        return "name\t" "tool\t" "timestamp\t" "run_id\t" "partition\t" "program\t" "status\t" "time_end2end\t" "nb_atoms\t" "command"

    def json(self) -> Dict[str, Any]:
        """To json."""
        return dict(
            name=self.name,
            tool=self.tool,
            timestamp=self.timestamp,
            run_id=self.run_id,
            partition=self.partition,
            program=self.program,
            status=self.status.value,
            time_end2end=self.time_end2end,
            nb_atoms=self.nb_atoms,
            command=self.command_str,
        )

    @property
    def command_str(self) -> str:
        return ' '.join(map(str, self.command)) if self.command is not None else "None"

    def __str__(self):
        """To string."""
        time_end2end_str = (
            f"{self.time_end2end:10.6f}" if self.time_end2end is not None else "None"
        )
        return (
            f"{self.name}\t"
            f"{self.tool}\t"
            f"{self.timestamp}\t"
            f"{self.run_id}\t"
            f"{self.partition}\t"
            f"{self.program}\t"
            f"{self.status.value}\t"
            f"{time_end2end_str}\t"
            f"{self.nb_atoms}\t"
            f"{self.command_str}"
        )

    def to_rows(self) -> str:
        """Print results by rows."""
        return (
            f"name={self.name}\n"
            f"tool={self.tool}\n"
            f"timestamp={self.timestamp}\n"
            f"run_id={self.run_id}\n"
            f"partition={self.partition}\n"
            f"program={self.program}\n"
            f"status={self.status}\n"
            f"time_end2end={self.time_end2end}\n"
            f"nb_atoms={self.nb_atoms}\n"
            f"command={self.command_str}"
        )


def save_data(data: List[Result], output: Path) -> None:
    """Save data to a file."""
    content = ""
    content += Result.headers() + "\n"
    for result in data:
        content += str(result) + "\n"
    output.write_text(content)


def run_cli(cmd, timeout: float, cwd, logger: logging.Logger, stdout_file: Path, stderr_file: Path):
    start = time.perf_counter()
    timed_out = False
    interrupted = False
    cmd_args = list(map(str, cmd))
    command_str = " ".join(cmd_args)
    logger.info("Calling command: %s", command_str)

    with stdout_file.open(mode="w") as stdout_fp, \
         stderr_file.open(mode="w") as stderr_fp:
        proc = subprocess.Popen(cmd_args,
                                cwd=cwd,
                                encoding="utf-8",
                                stdout=stdout_fp,
                                stderr=stderr_fp,
                                preexec_fn=os.setsid,
                                )
        logger.info(f"Created process with PID: %s", proc.pid)
        try:
            proc.communicate(timeout=timeout)
            logger.info(f"command succeeded: %s", command_str)
        except subprocess.TimeoutExpired:
            logger.error(f"command timed out: %s", command_str)
            timed_out = True
        except KeyboardInterrupt:
            logger.error(f"keyboard interrupt received...")
            interrupted = True
        finally:
            end = time.perf_counter()
            terminate_process(proc, logger)
        total = end - start
        proc.communicate(timeout=0.1)
        logger.info(f"Return code of PID %s: %s", proc.pid, proc.returncode)
        return proc.returncode, total, timed_out, interrupted


def terminate_process(proc: subprocess.Popen, logger: logging.Logger):
    logger.info("Sending SIGINT event to process %s...", proc.pid)
    proc.send_signal(get_sigint_crossplatform())
    with suppress(subprocess.TimeoutExpired):
        proc.communicate(timeout=5)
    if proc.poll() is None:
        logger.info("Timeout expired for SIGINT; send SIGTERM signal...")
        proc.terminate()
        if proc.poll() is None:
            logger.info("Timeout expired for SIGTERM; send killing signal...")
            proc.kill()

    logger.info("Killing child processes...")
    for any_process in psutil.process_iter():
        if any_process.ppid() == proc.pid:
            child_process = any_process
            logger.info(
                f"Killing child process with PID %s (PPID: %s)",
                child_process,
                any_process.ppid(),
            )
            terminate_process(proc, logger)


def get_sigint_crossplatform():
    return signal.CTRL_C_EVENT if sys.platform == "win32" else signal.SIGINT
