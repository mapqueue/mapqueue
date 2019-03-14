from .base import Key, Map, Optional, millis_dt
from google.cloud.bigtable.client import Client
from google.cloud.bigtable.row_filters import CellsColumnLimitFilter, TimestampRangeFilter, TimestampRange, RowFilterChain

FAMILY = 'f'


class BigTableMap(Map):

    def open(self):
        self._table = Client(
            project=self._config.project,
            credentials=self._config.credentials,
            admin=True
        ).instance(self._config.instance).table(self._config.table)
        # maybe add batcher later as an optimization?
        # self._buffer = self._table.mutations_batcher()
        # ensure table and column are created
        if not self._table.exists():
            self._table.create()
        columns = self._table.list_column_families().keys()
        if FAMILY not in columns:
            self._table.column_family(FAMILY).create()
        return self

    def close(self):
        """does bigtable client require cleanup?"""
        # maybe add batcher later as an optimization?
        # self._buffer.flush()
        return self

    def _put(self, key: Key, value: bytes) -> Key:
        row = self._table.row(row_key=key.uuid.bytes_le)
        row.set_cell(
            column_family_id=FAMILY,
            column=key.kind.encode('utf-8'),
            value=value,
            timestamp=millis_dt(key.time)
        )
        self._table.mutate_rows([row])
        return key

    def _get(self, key: Key) -> Optional[bytes]:
        return self._table.read_row(
            row_key=key.uuid.bytes_le,
            filter_=RowFilterChain(
                filters=[
                    TimestampRangeFilter(range_=TimestampRange(
                        end=millis_dt(key.time+1))),
                    CellsColumnLimitFilter(num_cells=1)
                ]
            )
        ).cell_value(column_family_id=FAMILY, column=key.kind.encode('utf-8'))
