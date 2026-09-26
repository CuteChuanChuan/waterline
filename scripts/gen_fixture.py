# /// script
# dependencies = ["pyiceberg[sql-sqlite,pyarrow,pyiceberg-core]"]
# ///
"""Generate a small local Iceberg table with several snapshots.

Usage: uv run scripts/gen_fixture.py [WAREHOUSE_DIR]

Output is non-deterministic (UUIDs, snapshot ids, timestamps). Tests must
read committed fixtures, never regenerate them.

Iceberg bakes absolute paths into metadata.json and manifest Avro files, so
the default warehouse is a fixed, machine-independent path. Tests can later
remap this prefix to the committed fixture directory.
"""

import shutil
import sys
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import LongType, NestedField, StringType, TimestampType

# Not resolved: on macOS /tmp -> /private/tmp, which would leak into paths.
DEFAULT_WAREHOUSE = Path("/tmp/waterline-fixtures")

root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else DEFAULT_WAREHOUSE
root.mkdir(parents=True, exist_ok=True)

catalog = SqlCatalog(
    "local",
    uri=f"sqlite:///{root}/catalog.db",
    warehouse=f"file://{root}",
)
catalog.create_namespace_if_not_exists("demo")

schema = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "event", StringType(), required=False),
    NestedField(3, "ts", TimestampType(), required=False),
)
spec = PartitionSpec(PartitionField(3, 1000, DayTransform(), "ts_day"))

# drop_table only removes the catalog entry; wipe stale files from earlier runs.
if catalog.table_exists("demo.events"):
    catalog.drop_table("demo.events")
shutil.rmtree(root / "demo" / "events", ignore_errors=True)
table = catalog.create_table("demo.events", schema=schema, partition_spec=spec)


def batch(start: int, n: int, day: int) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(range(start, start + n), pa.int64()),
            "event": pa.array([f"e{i % 5}" for i in range(n)]),
            "ts": pa.array(
                [f"2026-09-{day:02d}T{i % 24:02d}:00:00" for i in range(n)]
            ).cast(pa.timestamp("us")),
        }
    )


# Many small appends -> many snapshots and small files (good lint material).
for i in range(5):
    table.append(batch(i * 100, 100, day=20 + i % 3))
table.delete(delete_filter="id < 50")
table.overwrite(batch(10_000, 50, day=25))

print(table.metadata_location)
