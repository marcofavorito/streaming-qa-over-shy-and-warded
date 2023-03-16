import dataclasses
from math import floor, log2, ceil
from typing import Optional, List

import psutil

from benchmark.utils.base import argmin

_total_mem_gb: float = psutil.virtual_memory().total // 10 ** 9
_max_mem_mb: int = _total_mem_gb * 1000
_min_mem_mb: int = 256


def _get_max_default_heap_size_mb():
    upper = int(pow(2, ceil(log2(_total_mem_gb))))
    lower = int(pow(2, floor(log2(_total_mem_gb))))
    closest_idx = argmin([abs(upper - _min_mem_mb), abs(lower - _min_mem_mb)])
    closest = upper if closest_idx == 0 else lower
    return int(closest / 2) * 1000


@dataclasses.dataclass(frozen=True)
class JVMConfig:
    initial_heap_size: Optional[int] = None
    maximum_heap_size: Optional[int] = None

    def __post_init__(self):
        if self.initial_heap_size:
            assert _min_mem_mb < self.initial_heap_size <= _max_mem_mb
        if self.maximum_heap_size:
            assert _min_mem_mb < self.maximum_heap_size <= _max_mem_mb
        if self.initial_heap_size and self.maximum_heap_size:
            assert self.initial_heap_size <= self.maximum_heap_size

    def to_cli_config(self) -> List[str]:
        args = []
        if self.initial_heap_size is not None:
            args.append(f"-Xms{self.initial_heap_size}m")
        if self.maximum_heap_size is not None:
            args.append(f"-Xmx{self.maximum_heap_size}m")
        return args
