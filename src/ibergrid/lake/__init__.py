"""The lake library: Iceberg tables on the REST catalog, written by PyIceberg, read by DuckDB.

ADR 0002 explains the split: ``catalog``, ``tables`` and ``wap`` own every write; ``duck`` owns
analytical reads and metadata queries; ``maintenance`` expires snapshots and compacts files.
"""
