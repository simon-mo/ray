from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from itertools import chain
from io import BytesIO
import os
import re

import pandas as pd

from . import get_npartitions
from .dataframe import ray, DataFrame
from .utils import _partition_pandas_dataframe


# Parquet
def _get_parquet_dataset_from_path(path):
    import pyarrow.parquet as pq

    if path.startswith('s3://'):
        import s3fs
        s3 = s3fs.S3FileSystem()
        p = pq.ParquetDataset(path.replace('s3://', ''), filesystem=s3)
    else:
        p = pq.ParquetDataset(path)

    return p


def read_parquet(path, engine='auto', columns=None, **kwargs):
    """Load a parquet object from the file path, returning a DataFrame.
    Ray DataFrame only supports pyarrow engine for now.

    Args:
        path: The filepath of the parquet file.
            We currently supports:
            - Local path
            - s3
        engine: Ray only support pyarrow reader.
                This argument doesn't do anything for now.
        kwargs: Pass into parquet's read function.
    """
    p = _get_parquet_dataset_from_path(path)

    if not columns:
        idx_regex = re.compile('__index_level_\d+__')
        columns = [
            name for name in p.schema.names if not idx_regex.match(name)
        ]

    column_obj_ids = [
        read_parquet_column.remote(path, [col], kwargs) for col in columns
    ]
    block_ids = ray.get(column_obj_ids)

    # Each block will have RangeIndex index and RangeIndex column
    # Block_ids will be column major.
    # See block_to_pd_df function below for more explaination.
    return block_ids


def block_to_pd_df(blocks):
    """
    Assume columns major blocks as list of lists. 
        blocks = [col_1_list, col_2_list, ...]
        col_1_list = [ObjectID(col_1_row_partition_1), ...]
    """
    columns = [
        pd.concat(ray.get(blocks[i])).reset_index(drop=True)
        for i in range(len(blocks))
    ]
    return pd.concat(columns, axis=1)


@ray.remote
def read_parquet_column(path, columns, kwargs):
    p = _get_parquet_dataset_from_path(path)

    full_column = p.read(columns=columns, **kwargs).to_pandas()
    return _partition_pandas_dataframe(
        full_column, num_partitions=get_npartitions())


# CSV
def _compute_offset(fn, npartitions):
    """
    Calculate the currect bytes offsets for a csv file.
    Return a list of (start, end) tuple where the end == \n or EOF.
    """
    total_bytes = os.path.getsize(fn)
    chunksize = total_bytes // npartitions
    if chunksize == 0:
        chunksize = 1

    bio = open(fn, 'rb')

    offsets = []
    start = 0
    while start <= total_bytes:
        bio.seek(chunksize, 1)  # Move forward {chunksize} bytes
        extend_line = bio.readline()  # Move after the next \n
        total_offset = chunksize + len(extend_line)
        # The position of the \n we just crossed.
        new_line_cursor = start + total_offset - 1
        offsets.append((start, new_line_cursor))
        start = new_line_cursor + 1

    bio.close()
    return offsets


def _get_firstline(file_path):
    bio = open(file_path, 'rb')
    first = bio.readline()
    bio.close()
    return first


def _infer_column(first_line):
    return pd.read_csv(BytesIO(first_line)).columns


@ray.remote
def _read_csv_with_offset(fn, start, end, header=b'', kwargs={}):
    bio = open(fn, 'rb')
    bio.seek(start)
    to_read = header + bio.read(end - start)
    bio.close()
    return pd.read_csv(BytesIO(to_read), **kwargs)


def read_csv(filepath,
             sep=',',
             delimiter=None,
             header='infer',
             names=None,
             index_col=None,
             usecols=None,
             squeeze=False,
             prefix=None,
             mangle_dupe_cols=True,
             dtype=None,
             engine=None,
             converters=None,
             true_values=None,
             false_values=None,
             skipinitialspace=False,
             skiprows=None,
             nrows=None,
             na_values=None,
             keep_default_na=True,
             na_filter=True,
             verbose=False,
             skip_blank_lines=True,
             parse_dates=False,
             infer_datetime_format=False,
             keep_date_col=False,
             date_parser=None,
             dayfirst=False,
             iterator=False,
             chunksize=None,
             compression='infer',
             thousands=None,
             decimal=b'.',
             lineterminator=None,
             quotechar='"',
             quoting=0,
             escapechar=None,
             comment=None,
             encoding=None,
             dialect=None,
             tupleize_cols=None,
             error_bad_lines=True,
             warn_bad_lines=True,
             skipfooter=0,
             skip_footer=0,
             doublequote=True,
             delim_whitespace=False,
             as_recarray=None,
             compact_ints=None,
             use_unsigned=None,
             low_memory=True,
             buffer_lines=None,
             memory_map=False,
             float_precision=None):
    """Read csv file from local disk.

    Args:
        filepath:
              The filepath of the csv file.
              We only support local files for now.
        kwargs: Keyword arguments in pandas::from_csv
    """
    kwargs = dict(
        sep=sep,
        delimiter=delimiter,
        header=header,
        names=names,
        index_col=index_col,
        usecols=usecols,
        squeeze=squeeze,
        prefix=prefix,
        mangle_dupe_cols=mangle_dupe_cols,
        dtype=dtype,
        engine=engine,
        converters=converters,
        true_values=true_values,
        false_values=false_values,
        skipinitialspace=skipinitialspace,
        skiprows=skiprows,
        nrows=nrows,
        na_values=na_values,
        keep_default_na=keep_default_na,
        na_filter=na_filter,
        verbose=verbose,
        skip_blank_lines=skip_blank_lines,
        parse_dates=parse_dates,
        infer_datetime_format=infer_datetime_format,
        keep_date_col=keep_date_col,
        date_parser=date_parser,
        dayfirst=dayfirst,
        iterator=iterator,
        chunksize=chunksize,
        compression=compression,
        thousands=thousands,
        decimal=decimal,
        lineterminator=lineterminator,
        quotechar=quotechar,
        quoting=quoting,
        escapechar=escapechar,
        comment=comment,
        encoding=encoding,
        dialect=dialect,
        tupleize_cols=tupleize_cols,
        error_bad_lines=error_bad_lines,
        warn_bad_lines=warn_bad_lines,
        skipfooter=skipfooter,
        skip_footer=skip_footer,
        doublequote=doublequote,
        delim_whitespace=delim_whitespace,
        as_recarray=as_recarray,
        compact_ints=compact_ints,
        use_unsigned=use_unsigned,
        low_memory=low_memory,
        buffer_lines=buffer_lines,
        memory_map=memory_map,
        float_precision=float_precision)

    offsets = _compute_offset(filepath, get_npartitions())

    first_line = _get_firstline(filepath)
    columns = _infer_column(first_line)

    df_obj_ids = []
    for start, end in offsets:
        if start != 0:
            df = _read_csv_with_offset.remote(
                filepath, start, end, header=first_line, kwargs=kwargs)
        else:
            df = _read_csv_with_offset.remote(
                filepath, start, end, kwargs=kwargs)
        df_obj_ids.append(df)

    return DataFrame(row_partitions=df_obj_ids, columns=columns)
