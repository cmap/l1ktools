import logging
import setup_GCToo_logger as setup_logger
import pandas as pd
import numpy as np
import os.path


__author__ = "Lev Litichevskiy"
__email__ = "lev@broadinstitute.org"

""" Writes a gctoo object to a gct file.

The main method is write. write_version_and_dims writes the first two
lines of the gct file, assemble_full_df assembles 3 component dfs
into a df of the correct form for a gct file, and write_full_df writes
the full_df into the gct file as lines 3 to the end.
append_dims_and_file_extension is a utility function that can be used to
append the matrix dimensions and .gct extension to the output filename.

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
"""

logger = logging.getLogger(setup_logger.LOGGER_NAME)

# Only writes GCT v1.3
VERSION = "1.3"


def write(gctoo, out_fname, data_null="NaN", metadata_null="-666", filler_null="-666", data_float_format=None):
    """Write a gctoo object to a gct file.

    Args:
        gctoo (gctoo object)
        out_fname (string): filename for output gct file
        data_null (string): how to represent missing values in the data (default = "NaN")
        metadata_null (string): how to represent missing values in the metadata (default = "-666")
        filler_null (string): what value to fill the top-left filler block with (default = "-666")
        data_float_format (string): how many decimal points to keep in representing data (default = None will keep all digits)
    Returns:
        nothing
    """
    # Create handle for output file
    if not out_fname.endswith(".gct"):
        out_fname = out_fname + ".gct"
    f = open(out_fname, "wb")

    # Write first two lines
    dims_ints = [gctoo.data_df.shape[0], gctoo.data_df.shape[1],
                 gctoo.row_metadata_df.shape[1], gctoo.col_metadata_df.shape[1]]
    dims = [str(dim) for dim in dims_ints]
    write_version_and_dims(VERSION, dims, f)

    # Convert 3 component dataframes into correct form
    full_df = assemble_full_df(
        gctoo.row_metadata_df, gctoo.col_metadata_df, gctoo.data_df,
        data_null, metadata_null, filler_null)

    # Write remainder of gct
    write_full_df(full_df, f, data_null, data_float_format)
    f.close()

    logger.info("GCT has been written to {}".format(out_fname))


def write_version_and_dims(version, dims, f):
    """Write first two lines of gct file.

    Args:
        version (string): 1.3 by default
        dims (list of strings): length = 4
        f (file handle): handle of output file
    Returns:
        nothing
    """
    f.write(("#" + version + "\n"))
    f.write((dims[0] + "\t" + dims[1] + "\t" + dims[2] + "\t" + dims[3] + "\n"))


def assemble_full_df(row_metadata_df, col_metadata_df, data_df, data_null, metadata_null, filler_null):
    """Assemble 3 component dataframes into the correct form for gct files.

    Args:
        row_metadata_df (pandas df)
        col_metadata_df (pandas df)
        data_df (pandas df)
        data_null (string): how to represent missing values in the data
        metadata_null (string): how to represent missing values in the metadata
        filler_null (string): what value to fill the top-left filler block with

    Returns:
        full_df (pandas df): shape = (n_chd + n_rid, 1 + n_rhd + n_cid),
            header will become the 3rd line of the gct file
    """
    # Convert metadata to strings
    row_metadata_df = row_metadata_df.astype(str)
    col_metadata_df = col_metadata_df.astype(str)

    # Replace missing values in metadata with metadata_null
    row_metadata_df.replace("nan", value=metadata_null, inplace=True)
    col_metadata_df.replace("nan", value=metadata_null, inplace=True)

    # TOP ROW: horz concatenate "id", rhd, and cid
    rhd_and_cid = np.hstack((row_metadata_df.columns.values, data_df.columns.values))
    top_row = np.hstack(("id", rhd_and_cid))

    # Check that it has correct length
    assert(len(top_row) == (1 + row_metadata_df.shape[1] + data_df.shape[1]))

    # Create nan array to fill the blank top-left quadrant
    filler = np.full((col_metadata_df.shape[1], row_metadata_df.shape[1]),
                     filler_null, dtype="S8")

    # TOP HALF: horz concatenate chd, filler, and col_metadata, which must be transposed
    filler_and_col_metadata = np.hstack((filler, col_metadata_df.T.values))
    top_half = np.column_stack((col_metadata_df.columns.values, filler_and_col_metadata))

    # BOTTOM HALF: horz concatenate rid, row_metadata, and data
    row_metadata_and_data = np.hstack((row_metadata_df.values, data_df.values))
    bottom_half = np.column_stack((data_df.index.values, row_metadata_and_data))

    # Vert concatenate the two halves
    full_df_values = np.vstack((top_half, bottom_half))

    # Stitch together full_df
    full_df = pd.DataFrame(full_df_values, columns=top_row)

    # Check that is has correct dims
    assert (full_df.shape == (
        (col_metadata_df.shape[1] + data_df.shape[0]),
        (1 + row_metadata_df.shape[1] + data_df.shape[1])))
    return full_df


def write_full_df(full_df, f, data_null, data_float_format):
    """Write the full_df to the gct file.

    Args:
        full_df (pandas df): data and metadata arranged correctly
        f (file handle): handle for output file
        data_null (string): how to represent missing values in the data
        data_float_format (string): how many decimal points to keep in representing data
    Returns:
        nothing
    """
    full_df.to_csv(f, header=True, index=False,
                   sep="\t",
                   na_rep=data_null,
                   float_format=data_float_format)


def append_dims_and_file_extension(fname, data_df):
    """Append dimensions and file extension to output filename.
    N.B. Dimensions are cols x rows.

    Args:
        fname (string): output filename
        data_df (pandas df)
    Returns:
        out_fname (string): output filename with matrix dims and .gct appended
    """
    # If there's no .gct at the end of output file name, add the dims and .gct
    if not fname.endswith(".gct"):
        out_fname = '{0}_n{1}x{2}.gct'.format(fname, data_df.shape[1], data_df.shape[0])
        return out_fname

    # Otherwise, only add the dims
    else:
        basename = os.path.splitext(fname)[0]
        out_fname = '{0}_n{1}x{2}.gct'.format(basename, data_df.shape[1], data_df.shape[0])
        return out_fname
