"""The single VNext sending pipeline.

The package deliberately has no dependency on the legacy task worker,
result-journal state machine, or campaign-specific DuckDB send transactions.
Each module exposes a small contract that can be exercised without starting
the FastAPI application.
"""

from .partition import (
    DEFAULT_VIRTUAL_PARTITIONS,
    PartitionMap,
    PartitionMapError,
    normalize_msisdn,
    partition_for_msisdn,
)
from .spool import (
    SpoolCapacityError,
    SpoolCorruptionError,
    SpoolExpiredError,
    SpoolRecord,
    TaskSpool,
)

__all__ = [
    "DEFAULT_VIRTUAL_PARTITIONS",
    "PartitionMap",
    "PartitionMapError",
    "SpoolCapacityError",
    "SpoolCorruptionError",
    "SpoolExpiredError",
    "SpoolRecord",
    "TaskSpool",
    "normalize_msisdn",
    "partition_for_msisdn",
]
