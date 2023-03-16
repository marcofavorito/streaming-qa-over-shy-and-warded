import logging
from pathlib import Path
from typing import Dict, List, Optional

from benchmark.experiments.core import Result
from benchmark.tools import tool_registry
from benchmark.utils.base import ensure_dict, remove_dir_or_fail


def run_engine(
    name: str,
    program: Path,
    datasets: List[Path],
    timeout: float,
    tool_id: str,
    tool_config: Optional[Dict] = None,
    run_config: Optional[Dict] = None,
    working_dir: Optional[Path] = None,
    force: bool = False,
) -> Result:
    tool_config = ensure_dict(tool_config)
    run_config = ensure_dict(run_config)
    if working_dir is not None:
        remove_dir_or_fail(working_dir, force)
        working_dir.mkdir(parents=True)

    tool = tool_registry.make(tool_id, **tool_config)
    logging.debug(f"name={name}")
    logging.debug(f"program={program}")
    logging.debug(f"datasets={datasets}")
    logging.debug(f"timeout={timeout}")
    logging.debug(f"tool={tool_id}")
    logging.debug(f"tool_config={tool_config}")
    logging.debug(f"run_config={run_config}")
    logging.debug(f"working_dir={working_dir}")

    with tool.session(working_dir=working_dir):
        try:
            result = tool.run(
                program,
                datasets,
                run_config=run_config,
                timeout=timeout,
                name=name,
                working_dir=working_dir,
            )
            return result
        except KeyboardInterrupt:
            logging.info("Interrupted!")
            raise
        except Exception as e:
            logging.exception(e)
            raise
