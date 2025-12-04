from nuthatch.processor import NuthatchProcessor
import dateparser
import datetime
import pandas as pd
import dask.dataframe as dd
import xarray as xr


class timeseries(NuthatchProcessor):
    """
    Processor for timeseries data.

    This processor is used to slice a timeseries dataset based on the start and end times.

    It also validates the timeseries data to ensure it has data within the start and end times.

    It supports xarray datasets and pandas/dask dataframes.
    """

    def __init__(self, timeseries='time', **kwargs):
        """
        Initialize the timeseries processor.

        Args:
            func: The function to wrap.
            timeseries: The name of the timeseries dimension.
        """
        super().__init__(**kwargs)
        self.timeseries = timeseries

    def post_process(self, ds):
        start_time = self.start_time
        end_time = self.end_time

        if isinstance(ds, xr.Dataset):
            match_time = [t for t in self.timeseries if t in ds.dims]
            if len(match_time) == 0:
                raise RuntimeError(f"Timeseries must have a dimension named {self.timeseries} for slicing.")

            time_col = match_time[0]
            ds = ds.sel({time_col: slice(start_time, end_time)})
        elif isinstance(ds, pd.DataFrame) or isinstance(ds, dd.DataFrame):
            match_time = [t for t in self.timeseries if t in ds.columns]

            if len(match_time) == 0:
                raise RuntimeError(f"Timeseries must have a dimension named {self.timeseries} for slicing.")

            time_col = match_time[0]

            try:
                if start_time is not None:
                    ds = ds[ds[time_col] >= start_time]
                if end_time is not None:
                    ds = ds[ds[time_col] <= end_time]

            except TypeError as e:
                if "Invalid comparison" not in str(e):
                    raise e

                time_col_tz = ds[time_col].dt.tz.compute()

                if start_time is not None:
                    start_time = pd.Timestamp(start_time)
                    if start_time.tz is None:
                        start_time = start_time.tz_localize(time_col_tz)
                    else:
                        start_time = start_time.tz_convert(time_col_tz)
                    ds = ds[ds[time_col] >= start_time]

                if end_time is not None:
                    end_time = pd.Timestamp(end_time)
                    if end_time.tz is None:
                        end_time = end_time.tz_localize(time_col_tz)
                    else:
                        end_time = end_time.tz_convert(time_col_tz)
                    ds = ds[ds[time_col] <= end_time]
        else:
            raise RuntimeError(f"Cannot filter timeseries for data type {type(ds)}")

        return ds

    def process_arguments(self, params, args, kwargs):
        # Validate time series params
        self.start_time = None
        self.end_time = None

        # Convert to a list if not
        self.timeseries = self.timeseries if isinstance(self.timeseries, list) else [self.timeseries]

        if 'start_time' not in params or 'end_time' not in params:
            raise ValueError(
                "Time series functions must have the parameters 'start_time' and 'end_time'")
        else:
            keys = [item for item in params]
            try:
                self.start_time = args[keys.index('start_time')]
                self.end_time = args[keys.index('end_time')]
            except IndexError:
                raise ValueError("'start_time' and 'end_time' must be passed as positional arguments, not "
                                 "keyword arguments")

        return args, kwargs

    def validate(self, ds):
        start_time = self.start_time
        end_time = self.end_time
        if isinstance(ds, xr.Dataset):
            # Check to see if the dataset extends roughly the full time series set
            match_time = [t for t in self.timeseries if t in ds.dims]
            if len(match_time) == 0:
                raise RuntimeError("Timeseries array functions must return "
                                   "a time dimension for slicing. "
                                   "This could be an invalid cache. "
                                   "Try running with recompute=True to reset the cache.")
            else:
                time_col = match_time[0]

                # Assign start and end times if None are passed
                st = dateparser.parse(start_time) if start_time is not None \
                    else pd.Timestamp(ds[time_col].min().values)
                et = dateparser.parse(end_time) if end_time is not None \
                    else pd.Timestamp(ds[time_col].max().values)

                # Check if within 1 year at least
                if (pd.Timestamp(ds[time_col].min().values) <
                    st + datetime.timedelta(days=365) and
                        pd.Timestamp(ds[time_col].max().values) >
                        et - datetime.timedelta(days=365)):
                    return True
                else:
                    print("""WARNING: The cached array does not have data within
                          1 year of your start or end time. Triggering recompute.
                          If you do not want to recompute the result set
                          `validate_data=False`""")
                    return False
        else:
            raise RuntimeError(f"Cannot validate timeseries for data type {type(ds)}")
