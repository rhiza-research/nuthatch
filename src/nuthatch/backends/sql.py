import os
import uuid
import hashlib
import sqlalchemy
import pandas as pd
import dask.dataframe as dd
from nuthatch.backend import DatabaseBackend, register_backend

import logging
logger = logging.getLogger(__name__)

@register_backend
class SQLBackend(DatabaseBackend):
    """
    SQL backend for caching tabular data in a SQL database.

    This backend supports dask and pandas dataframes.
    """

    backend_name = "sql"

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs={}):
        super().__init__(cacheable_config, cache_key, namespace, args, backend_kwargs)

        if backend_kwargs.get('hash_table_name', False):
            self.table_name = self._hashed_table_name(cache_key)
        else:
            self.table_name = cache_key

        self.write_index = backend_kwargs.get('write_index', False)
        self.chunk_size = backend_kwargs.get('chunk_size', 10000)

        if namespace:
            self.table_name = namespace + '.' + self.table_name

            if not self.write_connection.dialect.has_schema(self.write_connection, namespace):
                self.write_connection.execute(sqlalchemy.schema.CreateSchema(namespace))

    def upsert(self, data, upsert_keys=None):
        primary_keys = upsert_keys

        if self.exists():
            if primary_keys is None or not isinstance(primary_keys, list):
                raise ValueError("Upsert may only be performed with primary keys specified as a list.")

            with self.connection.begin() as conn:
                logger.info("SQL cache exists for upsert.")
                # If it already exists...

                # Extract the primary key columns for SQL constraint
                for key in primary_keys:
                    if key not in data.columns:
                        raise ValueError("Dataframe MUST contain all primary keys as columns")

                # Write a temporary table
                temp_table_name = f"temp_{uuid.uuid4().hex[:6]}"

                if isinstance(data, pd.DataFrame):
                    data.to_sql(temp_table_name, self.write_connection, index=self.write_index)
                elif isinstance(data, dd.DataFrame):
                    data.to_sql(temp_table_name, uri=self.write_connection.url.render_as_string(hide_password=False), index=self.write_index, parallel=True, chunksize=self.chunk_size)
                else:
                    raise RuntimeError("SQL backend only support pandas and dask dataframes")

                index_sql_txt = ", ".join([f'"{i}"' for i in primary_keys])
                columns = list(data.columns)
                headers = primary_keys + list(set(columns) - set(primary_keys))
                headers_sql_txt = ", ".join(
                    [f'"{i}"' for i in headers]
                )  # index1, index2, ..., column 1, col2, ...

                # col1 = exluded.col1, col2=excluded.col2
                # Excluded statement updates values of rows where primary keys conflict
                update_column_stmt = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])

                # For the ON CONFLICT clause, postgres requires that the columns have unique constraint
                # To add if not exists must drop if exists and then add. In a transaction this is consistent

                # Constraint IDs need to be globally unique to not conflict in the database
                constraint_id =  hashlib.md5(self.cache_key.encode()).hexdigest()[:10]
                query_pk = f"""
                ALTER TABLE "{self.table_name}" DROP CONSTRAINT IF EXISTS unique_constraint_for_{constraint_id} CASCADE;
                """

                logger.info("Adding a unique to contraint to table if it doesn't exist.")
                conn.exec_driver_sql(query_pk)

                query_pk = f"""
                ALTER TABLE "{self.table_name}" ADD CONSTRAINT unique_constraint_for_{constraint_id}
                UNIQUE ({index_sql_txt});
                """
                conn.exec_driver_sql(query_pk)

                # Compose and execute upsert query
                query_upsert = f"""INSERT INTO "{self.table_name}" ({headers_sql_txt})
                                   SELECT {headers_sql_txt} FROM "{temp_table_name}"
                                   ON CONFLICT ({index_sql_txt}) DO UPDATE
                                   SET {update_column_stmt};
                                   """
                logger.info("Upserting.")
                conn.exec_driver_sql(query_upsert)
                conn.exec_driver_sql(f"DROP TABLE {temp_table_name}")
                return self.read(engine=type(data))
        else:
            return self.write(data)


    def write(self, data):
        try:
            if isinstance(data, pd.DataFrame):
                data.to_sql(self.table_name, self.write_connection, if_exists='replace', index=self.write_index)
                return data
            elif isinstance(data, dd.DataFrame):
                data.to_sql(self.table_name, self.connection.url.render_as_string(hide_password=False), if_exists='replace', index=self.write_index, parallel=True, chunksize=self.chunk_size)
                return self.read(engine=dd.DataFrame)
            else:
                raise RuntimeError("SQL Backend can only write pandas and dask dataframes.")
        except sqlalchemy.exc.InterfaceError:
            raise RuntimeError("Error connecting to database.")


    def read(self, engine=None):
        if engine == 'pandas' or engine == pd.DataFrame or engine =='dask' or engine == dd.DataFrame or engine is None:
            try:
                data = pd.read_sql_query(f'select * from "{self.table_name}"', con=self.connection)
                if engine == 'dask' or engine == dd.DataFrame:
                    return dd.from_pandas(data)
                else:
                    return data
            except sqlalchemy.exc.InterfaceError:
                raise RuntimeError("Error connecting to database.")
        else:
            raise RuntimeError("SQL backend only supports pandas and dask engines.")


    def exists(self):
        try:
            insp = sqlalchemy.inspect(self.connection)
            return insp.has_table(self.table_name)
        except sqlalchemy.exc.InterfaceError:
            raise RuntimeError("Error connecting to database.")


    def get_uri(self):
        return os.path.join(self.connection.url.render_as_string(), self.table_name)


    def delete(self):
        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table(self.table_name, metadata)
        table.drop(self.connection, checkfirst=True)

    def _hashed_table_name(self, table_name):
        """Return a qualified postgres table name."""
        return hashlib.md5(table_name.encode()).hexdigest()

