""" Reads in a gct file as a gctoo object.

The main method is parse. parse_into_3_dfs creates the row
metadata, column metadata, and data dataframes, while the
assemble_multi_index_df method in GCToo.py assembles them.

N.B. Only supports v1.3 gct files.

Example GCT:
#1.3
96  36  9 15

96 = number of data rows
36 = number of data columns
9 = number of row metadata fields (+1 for the 'id' column -- first column)
15 = number of col metadata fields (+1 for the 'id' row -- first row)
---------------------------------------------------
|id|        rhd          |          cid           |
---------------------------------------------------
|  |                     |                        |
|c |                     |                        |
|h |      (blank)        |      col_metadata      |
|d |                     |                        |
|  |                     |                        |
---------------------------------------------------
|  |                     |                        |
|r |                     |                        |
|i |    row_metadata     |          data          |
|d |                     |                        |
|  |                     |                        |
---------------------------------------------------

N.B. col_metadata_df is stored as the transpose of how it looks in the gct file.
That is, col_metadata_df.shape = (num_cid, num_chd).
"""

import logging
import setup_GCToo_logger as setup_logger
import pandas as pd
import os.path
import GCToo
import slice_gct

__author__ = "Lev Litichevskiy"
__email__ = "lev@broadinstitute.org"

logger = logging.getLogger(setup_logger.LOGGER_NAME)

# What to label the index and columns of the component dfs
row_index_name = "rid"
column_index_name = "cid"
row_header_name = "rhd"
column_header_name = "chd"


def parse(file_path, nan_values=None, rid=None, cid=None):
    """ The main method.

    Args:
        - file_path (string): full path to gctx file you want to parse
        - nan_values (list of strings): values to be treated as NaN
            (default below)
        - rid (list of strings): list of row ids to specifically keep  None keeps all rids
        - cid (list of strings): list of col ids to specifically keep, None keeps all cids

    Returns:
        gctoo_obj (GCToo object)

    """
    # Use default nan values if none given
    default_nan_values = [
        "#N/A", "N/A", "NA", "#NA", "NULL", "NaN", "-NaN",
        "nan", "-nan", "#N/A!", "na", "NA", "None", "-666"]
    # N.B. -666 is a CMap-specific null value

    if nan_values is None:
        nan_values = default_nan_values

    # Verify that the gct path exists
    if not os.path.exists(file_path):
        err_msg = "The given path to the gct file cannot be found. gct_path: {}"
        logger.error(err_msg.format(file_path))
        raise(Exception(err_msg.format(file_path)))
    logger.info("Reading GCT: {}".format(file_path))

    # Read version and dimensions
    (version, num_data_rows, num_data_cols,
     num_row_metadata, num_col_metadata) = read_version_and_dims(file_path)

    # Read in metadata and data
    (row_metadata, col_metadata, data) = parse_into_3_df(
        file_path, num_data_rows, num_data_cols,
        num_row_metadata, num_col_metadata, nan_values)

    # Create the gctoo object and assemble 3 component dataframes
    gctoo_obj = create_gctoo_obj(file_path, version, row_metadata, col_metadata, data)
   
    # If requested, slice gctoo
    if (rid is not None) or (cid is not None):
        gctoo_obj = slice_gct.slice_gctoo(gctoo_obj, rid=rid, cid=cid)

    return gctoo_obj


def read_version_and_dims(file_path):
    # Open file
    f = open(file_path, "rb")

    # Get version from the first line
    version = f.readline().strip().lstrip("#")

    # Check that the version is 1.3
    if version != "1.3":
        err_msg = ("Only GCT v1.3 is supported. The first row of the GCT " +
                   "file must simply be (without quotes) '#1.3'")
        logger.error(err_msg.format(version))
        raise(Exception(err_msg.format(version)))

    # Read dimensions from the second line
    dims = f.readline().strip().split("\t")

    # Close file
    f.close()

    # Check that the second row is what we expect
    if len(dims) != 4:
        err_msg = "The second row of the GCT file should only have 4 entries. dims: {}"
        logger.error(err_msg.format(dims))
        raise(Exception(err_msg.format(dims)))

    # Explicitly define each dimension
    num_data_rows = int(dims[0])
    num_data_cols = int(dims[1])
    num_row_metadata = int(dims[2])
    num_col_metadata = int(dims[3])

    # Return version and dimensions
    return version, num_data_rows, num_data_cols, num_row_metadata, num_col_metadata


def parse_into_3_df(file_path, num_data_rows, num_data_cols, num_row_metadata, num_col_metadata, nan_values):
    # Read the gct file beginning with line 3
    full_df = pd.read_csv(file_path, sep="\t", header=None, skiprows=2,
                          dtype=str, na_values=nan_values, keep_default_na=False)

    # Check that full_df is the size we expect
    assert full_df.shape == (num_col_metadata + num_data_rows + 1,
                             num_row_metadata + num_data_cols + 1), (
        "The shape of full_df is not as expected. Cannot parse this gct file.")

    # Assemble metadata dataframes
    row_metadata = assemble_row_metadata(full_df, num_col_metadata, num_data_rows, num_row_metadata)
    col_metadata = assemble_col_metadata(full_df, num_col_metadata, num_row_metadata, num_data_cols)

    # Assemble data dataframe
    data = assemble_data(full_df, num_col_metadata, num_data_rows, num_row_metadata, num_data_cols)

    # Return 3 dataframes
    return row_metadata, col_metadata, data


def assemble_row_metadata(full_df, num_col_metadata, num_data_rows, num_row_metadata):
    # Extract values
    row_metadata_row_inds = range(num_col_metadata + 1, num_col_metadata + num_data_rows + 1)
    row_metadata_col_inds = range(1, num_row_metadata + 1)
    row_metadata = full_df.iloc[row_metadata_row_inds, row_metadata_col_inds]

    # Create index from the first column of full_df (after the filler block)
    row_metadata.index = full_df.iloc[row_metadata_row_inds, 0]

    # Create columns from the top row of full_df (before cids start)
    row_metadata.columns = full_df.iloc[0, row_metadata_col_inds]

    # Rename the index name and columns name
    row_metadata.index.name = row_index_name
    row_metadata.columns.name = row_header_name

    # Convert metadata to numeric if possible
    row_metadata = row_metadata.apply(lambda x: pd.to_numeric(x, errors="ignore"))

    return row_metadata


def assemble_col_metadata(full_df, num_col_metadata, num_row_metadata, num_data_cols):
    # Extract values
    col_metadata_row_inds = range(1, num_col_metadata + 1)
    col_metadata_col_inds = range(num_row_metadata + 1, num_row_metadata + num_data_cols + 1)
    col_metadata = full_df.iloc[col_metadata_row_inds, col_metadata_col_inds]

    # Transpose so that samples are the rows and headers are the columns
    col_metadata = col_metadata.T

    # Create index from the top row of full_df (after the filler block)
    col_metadata.index = full_df.iloc[0, col_metadata_col_inds]

    # Create columns from the first column of full_df (before rids start)
    col_metadata.columns = full_df.iloc[col_metadata_row_inds, 0]

    # Rename the index name and columns name
    col_metadata.index.name = column_index_name
    col_metadata.columns.name = column_header_name

    # Convert metadata to numeric if possible
    col_metadata = col_metadata.apply(lambda x: pd.to_numeric(x, errors="ignore"))

    return col_metadata


def assemble_data(full_df, num_col_metadata, num_data_rows, num_row_metadata, num_data_cols):
    # Extract values
    data_row_inds = range(num_col_metadata + 1, num_col_metadata + num_data_rows + 1)
    data_col_inds = range(num_row_metadata + 1, num_row_metadata + num_data_cols + 1)
    data = full_df.iloc[data_row_inds, data_col_inds]

    # Create index from the first column of full_df (after the filler block)
    data.index = full_df.iloc[data_row_inds, 0]

    # Create columns from the top row of full_df (after the filler block)
    data.columns = full_df.iloc[0, data_col_inds]

    # Convert from str to float
    try:
        data = data.astype(float)
    except:

        # If that fails, return the first value that could not be converted
        for col in data:
            try:
                data[col].astype(float)
            except:
                for row_idx, val in enumerate(data[col]):
                    try:
                        float(val)
                    except:
                        bad_row_label = data[col].index[row_idx]
                        err_msg = ("First instance of value that could not be converted: " +
                                   "data.loc['{}', '{}'] = '{}'\nAdd to nan_values if you wish " +
                                   "for this value to be considered NaN.").format(bad_row_label, col, val)
                        logger.error(err_msg)
                        raise(Exception(err_msg))

    # Rename the index name and columns name
    data.index.name = row_index_name
    data.columns.name = column_index_name

    return data


def create_gctoo_obj(file_path, version, row_metadata_df, col_metadata_df, data_df):
    # Move dataframes into GCToo object
    gctoo_obj = GCToo.GCToo(src=file_path,
                            version=version,
                            row_metadata_df=row_metadata_df,
                            col_metadata_df=col_metadata_df,
                            data_df=data_df)
    return gctoo_obj

