from nuthatch.backend import DatabaseBackend, register_backend
from os.path import join
import hashlib
import sqlalchemy
import uuid
import pandas as pd
import dask.dataframe as dd

def hashed_table_name(table_name):
    """Return a qualified postgres table name."""
    return hashlib.md5(table_name.encode()).hexdigest()

@register_backend
class SQLBackend(DatabaseBackend):
    """
    SQL backend for caching tabular data in a SQL database.

    This backend supports dask and pandas dataframes.
    """

    backend_name = "sql"

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs):
        super().__init__(cacheable_config, cache_key, namespace, args, backend_kwargs)

        if backend_kwargs and 'hash_table_name' in backend_kwargs:
            self.table_name = hashed_table_name(cache_key)
        else:
            self.table_name = cache_key

        if namespace:
            self.table_name = namespace + '.' + self.table_name

            if not self.write_engine.dialect.has_schema(self.write_engine, namespace):
                self.write_engine.execute(sqlalchemy.schema.CreateSchema(namespace))



    def write(self, data, upsert=False, primary_keys=None):
        if upsert and self.exists():
            if primary_keys is None or not isinstance(primary_keys, list):
                raise ValueError("Upsert may only be performed with primary keys specified as a list.")

            if not isinstance(data, dd.DataFrame):
                raise RuntimeError("Upsert is only supported by dask dataframes for parquet")

            with self.engine.begin() as conn:
                print("SQL cache exists for upsert.")
                # If it already exists...

                # Extract the primary key columns for SQL constraint
                for key in primary_keys:
                    if key not in data.columns:
                        raise ValueError("Dataframe MUST contain all primary keys as columns")

                # Write a temporary table
                temp_table_name = f"temp_{uuid.uuid4().hex[:6]}"

                if isinstance(data, pd.DataFrame):
                    data.to_sql(temp_table_name, self.engine, index=False)
                elif isinstance(data, dd.DataFrame):
                    data.to_sql(temp_table_name, uri=self.engine.url.render_as_string(hide_password=False), index=False, parallel=True, chunksize=10000)
                else:
                    raise RuntimeError("Did not return dataframe type.")

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

                print("Adding a unique to contraint to table if it doesn't exist.")
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
                print("Upserting.")
                conn.exec_driver_sql(query_upsert)
                conn.exec_driver_sql(f"DROP TABLE {temp_table_name}")
        else:
            try:
                if isinstance(data, pd.DataFrame):
                    data.to_sql(self.table_name, self.write_engine, if_exists='replace', index=False)
                    return data
                elif isinstance(data, dd.DataFrame):
                    data.to_sql(self.table_name, self.engine.url.render_as_string(hide_password=False), if_exists='replace', index=False, parallel=True, chunksize=10000)
                else:
                    raise RuntimeError("Did not return dataframe type.")

                # Also log the table name in the tables table
                pd_name = {'table_name': [self.cache_key], 'table_key': [self.table_name], 'created_at': [pd.Timestamp.now()]}
                pd_name = pd.DataFrame(pd_name)
                pd_name.to_sql('cache_tables', self.write_engine, if_exists='append')
            except sqlalchemy.exc.InterfaceError:
                raise RuntimeError("Error connecting to database.")

    def read(self, engine=None):
        if engine == 'pandas' or engine == pd.DataFrame or engine =='dask' or engine == dd.DataFrame or engine is None:
            try:
                data = pd.read_sql_query(f'select * from "{self.table_name}"', con=self.engine)
                if engine == 'dask' or engine == dd.DataFrame:
                    return dd.from_pandas(data)
                else:
                    return data
            except sqlalchemy.exc.InterfaceError:
                raise RuntimeError("Error connecting to database.")
        else:
            raise RuntimeError("SQL backend only supports pandas engine.")

    def exists(self):
        try:
            insp = sqlalchemy.inspect(self.engine)
            return insp.has_table(self.table_name)
        except sqlalchemy.exc.InterfaceError:
            raise RuntimeError("Error connecting to database.")

    def get_file_path(self):
        return join(self.engine.url.render_as_string(), self.table_name)

    def delete(self):
        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table(self.table_name, metadata)
        table.drop(self.engine, checkfirst=True)