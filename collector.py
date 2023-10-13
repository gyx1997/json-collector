import os
import sys
from typing import Callable, List, Dict, Any, Type, Iterable
import threading
from fields import *
import sqlite3
import time
import hashlib
import random


class JsonDataCollector(object):
    """
    Collect sequence of Json-Like objects to structured table data.
    """

    def __init__(self,
                 collection_name: str,
                 fields: List[Field],
                 in_memory: bool = True,
                 unique_keys: List[int] = None,
                 sorted_keys: List[List[int]] = None,
                 error_handler: Callable[[Exception, Any], Any] = None,
                 ignore_duplicates: bool = False,
                 append: bool = False,
                 batch_size: int = 4096):
        """
        Initialize a DataCollector object.

        Args:
            collection_name: The table name of this collection. If `in_memory=False`, it will
                             also be used as filename of that SQLite database.
            error_handler: A callable that handle the exceptions raised. The first argument
                           is the exception raised, and the second argument is the raw record
                           that causes this exception. If `None` is set, the record will be
                           simply skipped when causing an exception.
            in_memory: Use in-memory mode for Sqlite database if True. Otherwise, use file.
            sorted_keys: A list of integer list that represents the keys to be indexed. Example:
                         specifying `sorted_keys=[[1, 2], [0]]` would create two indexes; the first
                         one is on column (field) 1 and 2, while the second one is on column 0. To reduce
                         the overhead of insert operation, only create indexes on necessary fields.
            unique_keys: A list of integers which are the indices of columns that used for identifying
                         duplicate data. Example: `unique_keys=[0, 2, 4]` means two records would be
                         considered as the same if their fields 0, 2, and 4 are the same.
            batch_size: The number of records to store in memory temporarily before committing
                        by SQLite. For non-memory SQLite storage, a relatively large value of
                        `batch_size` would increase the inserting performance significantly.
                        However, data loss may be caused if terminated unexpectedly before commit
                        operation.
            ignore_duplicates: Ignore duplicate objects (detected by unique_keys) if True. Accept duplicate
                               objects if False.
            append: The new objects will be appended on the existing SQLite database file if True; Otherwise,
                    records stored in the old file would be truncated before inserting new records by this
                    JsonDataCollector instance.
            fields: A list of `Field` objects that includes the fields which should be captured from raw data.
        """
        self._sqlite: sqlite3.Connection | None = None
        self._in_memory = in_memory
        self._fields = fields
        self._unique_keys = unique_keys if unique_keys is not None else []
        self._sorted_keys = sorted_keys if sorted_keys is not None else []
        self._collection_name = collection_name
        self._skip_duplicate = ignore_duplicates
        self._append = append
        self._hash_keys = dict()
        self._inc_id = 1
        self._batch_size = batch_size
        self._error_handler = error_handler

        self.__init_sqlite()

    def __del__(self):
        """ Release the resource of sqlite connection. """
        if self._sqlite is not None:
            self._sqlite.commit()
            self._sqlite.close()

    @property
    def name(self):
        """
        The name of this data collection.
        """
        return self._collection_name

    @property
    def fields(self):
        """
        The fields for the data collected.
        """
        return self._fields

    @property
    def append_on_exist(self):
        """
        Append data on existing SQLite database if True, otherwise create a new one.
        """
        return self._append

    @property
    def sqlite(self):
        """
        The backend sqlite object.
        """
        return self._sqlite

    def add(self, r: List[dict] | dict) -> None:
        """
        Add a list of records into Sqlite backend.

        Args:
            r: A list of record (dict-like), or a dict-like object.
        """
        r_list = r if isinstance(r, list) else list(r)
        for record in r_list:
            try:
                r_parsed = {f.name: f.parse(record) for f in self.fields}
                self.__insert(r_parsed)
            except Exception as e:
                if self._error_handler is not None:
                    self._error_handler(e, record)

    def query(self, sql: str = None, parameters: Any = None) -> List[Dict[str, Any]]:
        """
        Get a set of collected objects with given sql query.

        Args:
            sql: The SQL query string to be executed. Parameters can use character `?` for binding.
                 For more details see `https://docs.python.org/3/library/sqlite3.html`. If None is set,
                 all objects will be returned by using a `SELECT *` query.
            parameters: The parameters to be bound in the SQL query string.
        """
        return self.__get_objects(sql=sql,
                                  params=parameters,
                                  row_factory=lambda x, y: {
                                      k: v for k, v in zip([col[0] for col in x.description], y)
                                  })[1]

    def query_as_csv(self, sql: str = None, parameters: Any = None, file_name: str = None,
                     delimiter: str = ',') -> str | None:
        """
        Get a csv string with given sql query.

        Args:
            sql: The SQL query string to be executed. Parameters can use character `?` for binding.
                 For more details see `https://docs.python.org/3/library/sqlite3.html`. If None is set,
                 all objects will be returned by using a `SELECT *` query.
            parameters: The parameters to be bound in the SQL query string.
            file_name: The filename of csv file. Returns None if `file_name` is correctly set; returns
                       a string if `file_name=None`.
            delimiter: The delimiter used in csv file.
        """

        def __parse_val(val: Any) -> str:
            if isinstance(val, str):
                # Escape special characters
                val = val.replace("\"", "\"\"")
                return f"\"{val}\""
            else:
                return str(val)

        columns, objects = self.__get_objects(sql, parameters, None)
        # Header
        csv_lines = [delimiter.join(columns)]
        # Rows
        for obj in objects:
            csv_lines.append(delimiter.join(list(__parse_val(val) for val in obj)))
        csv_str = os.linesep.join(csv_lines)
        if file_name is not None and len(file_name) > 0:
            with open(file_name, "w+", encoding="utf-8") as fd:
                fd.write(csv_str)
                fd.flush()
            return None
        else:
            return csv_str

    def __get_objects(self, sql: str, params: Any, row_factory: Any) -> tuple[tuple, list[Any]]:
        """ Get objects from given sql query. """
        sql = sql if isinstance(sql, str) and len(sql) > 0 else f"SELECT * FROM {self._collection_name}"
        cursor = self._sqlite.cursor()
        cursor.row_factory = row_factory
        cursor = cursor.execute(sql, params if params is not None else tuple())
        res_columns = tuple(col[0] for col in cursor.description)
        res_objects = cursor.fetchall()
        cursor.close()
        return res_columns, res_objects

    def __init_sqlite(self):
        """ Initialize the Sqlite3 backend storage """
        # Create SQLite database
        if self._in_memory:
            self._sqlite = sqlite3.connect(":memory:")
        else:
            self._sqlite = sqlite3.connect(f"{self._collection_name}.sqlite")
        if self._append is False:
            self._sqlite.execute(f"DROP TABLE IF EXISTS {self._collection_name}")
        # Create __columns__
        stmt_fields = ",".join(tuple(f"`{field.name}` {field.type}" for field in self.fields))
        sql_collection = (f"CREATE TABLE IF NOT EXISTS {self._collection_name} "
                          f"({stmt_fields}, __id INTEGER)")
        self._sqlite.execute(sql_collection)
        self._sqlite.execute(f"CREATE INDEX IF NOT EXISTS index_id ON {self._collection_name} (__id)")
        if len(self._unique_keys) > 0:
            unique_key_columns = ",".join([self.fields[uniq_key_id].name for uniq_key_id in self._unique_keys])
            self._sqlite.execute(f"CREATE INDEX IF NOT EXISTS idx_id "
                                 f"ON {self._collection_name} ({unique_key_columns})")
        self._sqlite.commit()
        # Create indexes for sorted_keys.
        if len(self._sorted_keys) > 0:
            for sorted_key_pair in self._sorted_keys:
                sorted_key_index_name = "idx_sk_" + ("_".join(list(str(x) for x in sorted_key_pair)))
                sorted_key_columns = ",".join([self.fields[sorted_key_id].name for sorted_key_id in sorted_key_pair])
                if self._sqlite.execute(f"SELECT COUNT(1) FROM sqlite_master "
                                        f"WHERE type='index' "
                                        f"AND tbl_name='{self._collection_name}' "
                                        f"AND name='{sorted_key_index_name}'").fetchone()[0] == 0:
                    self._sqlite.execute(f"CREATE INDEX {sorted_key_index_name} "
                                         f"ON {self._collection_name} ({sorted_key_columns})")
        self._sqlite.commit()
        # Prepare in-memory hash table for fast checking duplicates.
        self._hash_keys = dict()
        if self._append is True:
            cursor_existed = self._sqlite.execute(f"SELECT * FROM {self._collection_name}")
            for rec in cursor_existed:
                rec_hash = self.__hash_unique_key(rec)
                # storage as [field_1, field_2, ..., field_n, #id]
                # Here we only select value defined in unique_keys.
                rec_keys = tuple(rec[x] for x in self._unique_keys)
                if self._hash_keys.get(rec_hash, None) is None:
                    self._hash_keys[rec_hash] = [rec_keys]
                else:
                    self._hash_keys[rec_hash].append(rec_keys)
        # Make the auto-inc id continuous.
        if self._append is True:
            cursor_count = self._sqlite.execute(f"SELECT COUNT(1) FROM {self._collection_name}")
            existing_count = cursor_count.fetchone()[0]
            self._inc_id = existing_count + 1
        else:
            self._inc_id = 1

    def __insert(self, r):
        """ Insert a single record to the result set. """
        r_hash = self.__hash_unique_key(r)
        if self._skip_duplicate is False or self.__has_duplicate(r, r_hash) is False:
            r_id = self._inc_id
            # Insert into Sqlite backend.
            columns = ",".join(list(f"`{f.name}`" for f in self.fields))
            placeholders = ','.join(list('?' for _ in self.fields))
            values = list(r[x.name] for x in self.fields)
            # Assume the following SQL query will not result in syntax error
            # (as there must be len(columns) >= 1).
            self._sqlite.execute(
                f"INSERT INTO {self._collection_name} (__id, {columns}) VALUES ({r_id}, {placeholders})",
                values
            )
            # Update hash table
            r_uniq_keys = tuple(r[self.fields[key_id].name] for key_id in self._unique_keys)
            if self._hash_keys.get(r_hash, None) is None:
                self._hash_keys[r_hash] = [r_uniq_keys]
            else:
                self._hash_keys[r_hash].append(r_uniq_keys)
            # Increase the auto-increment pointer
            self._inc_id = self._inc_id + 1
        if self._inc_id % self._batch_size == 0:
            self._sqlite.commit()

    def __has_duplicate(self, r, r_hash):
        """ Are there any records with the same content as the given record? """
        bucket = self._hash_keys.get(r_hash, None)
        if bucket is None:
            return False
        r_uniq_keys = tuple(r[self.fields[key_id].name] for key_id in self._unique_keys)
        for r_compared in bucket:
            if self.__tuple_eq(r_compared, r_uniq_keys) is True:
                return True
        return False

    def __tuple_eq(self, x: tuple, y: tuple):
        """ Does tuple x equal to record y? """
        for i in range(0, len(x)):
            if x[i] != y[i]:
                return False
        return True

    def __hash_unique_key(self, record):
        """ Get the hash value of given record. """
        if len(self._unique_keys) == 0:
            raise RuntimeError("No unique key specified when hashing")
        md5 = hashlib.md5()
        for uniq_key_id in self._unique_keys:
            if isinstance(record, dict):
                md5.update(str(record[self.fields[uniq_key_id].name]).encode())
            elif isinstance(record, tuple):
                md5.update(str(record[uniq_key_id]).encode())
        return md5.hexdigest()
