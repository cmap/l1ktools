"""
concat_gctoo.py
This function is for concatenating gct files together. You can tell it to find
gct files using the file_wildcard argument, or you can tell it exactly which
files you want to concatenate using the list_of_gct_paths argument. The meat of
this function is the hstack method (i.e. horizontal concatenation of
gct). Vertical concatenation has not yet been implemented but is very
analogous to what has been done here.
There are 3 arguments that allow you to work around certain obstacles
of concatenation.
1) If the row metadata contains headers with values that are not the same in
all files, then you can remove these headers using the fields_to_remove argument.
2) If the row metadata headers are the same between different files but not in
the same order, you can sort them using the sort_headers argument.
3) If the sample ids are not unique between different files, you can use the
reset_sample_ids argument. This will move the cids to a new metadata field
and assign a unique integer index for each sample.
"""

import argparse
import os
import sys
import glob
import logging
import setup_GCToo_logger as setup_logger
import pandas as pd

import GCToo as GCToo
import parse_gctoo as parse_gctoo
import write_gctoo as write_gctoo

__author__ = "Lev Litichevskiy"
__email__ = "lev@broadinstitute.org"

logger = logging.getLogger(setup_logger.LOGGER_NAME)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Required args
    mutually_exclusive_group = parser.add_mutually_exclusive_group()
    mutually_exclusive_group.add_argument("--list_of_gct_paths", "-lop", nargs="+",
        help="full paths to gct files to be concatenated")
    mutually_exclusive_group.add_argument("--file_wildcard", "-w", type=str,
        help=("wildcard specifying where files should be found " +
              "(make sure to surround in quotes if calling from command line!)"))

    # Optional args
    parser.add_argument("--full_out_name", "-o", type=str, default="concated.gct",
        help="what to name the output file (full path)")
    parser.add_argument("--fields_to_remove", "-ftr", nargs="+",
        default=["pr_probe_suitability_manual", "pr_probe_normalization_group"],
        help="fields to remove from the row metadata headers before concatenating")
    parser.add_argument("--sort_headers", "-sh", action="store_true", default=False,
        help=("whether to sort the headers of row metadata dfs (use this flag " +
              "if row metadata headers are not in the same order in different files)"))
    parser.add_argument("--reset_sample_ids", "-rsi", action="store_true", default=False,
        help="whether to reset sample ids (use this flag if sample ids are not unique)")

    parser.add_argument("-data_null", type=str, default="NA",
        help="how to represent missing values in the data")
    parser.add_argument("-metadata_null", type=str, default="NA",
        help="how to represent missing values in the metadata")
    parser.add_argument("-filler_null", type=str, default="NA",
        help="what value to use for filling the top-left filler block of a .gct")
    parser.add_argument("-verbose", "-v", action="store_true", default=False,
        help="whether to print a bunch of output")

    return parser


def main(args):

    # Get files directly
    if args.list_of_gct_paths is not None:
        files = args.list_of_gct_paths

    # Or find them
    else:
        files = get_file_list(args.file_wildcard)

    # Parse each file and append to a list
    gctoos = []
    for f in files:
        gctoos.append(parse_gctoo.parse(f))

    # Create concatenated gctoo object
    out_gctoo = hstack(gctoos, args.fields_to_remove, args.reset_sample_ids, args.sort_headers)

    # Write out_gctoo to file
    logger.info("Write to file...")
    write_gctoo.write(out_gctoo, args.full_out_name,
                      filler_null=args.filler_null,
                      metadata_null= args.metadata_null,
                      data_null=args.data_null)

def get_file_list(wildcard):
    """Search for files to be concatenated. Currently very basic, but could
    expand to be more sophisticated.
    Args:
        wildcard (regular expression string)
    Returns:
        files (list of full file paths)
    """
    files = glob.glob(os.path.expanduser(wildcard))
    return files


def hstack(gctoos, fields_to_remove, reset_sample_ids, sort_headers):
    """Horizontally concatenate gctoos.
    Args:
        gctoos (list of gctoo objects)
        fields_to_remove (list of strings): can specify certain fields to remove
            from row metadata in order to allow rows to line up
        reset_sample_ids (bool): set to True if sample ids are not unique
        sort_headers (bool): set to True in order to sort the headers of each
            row_metadata_df
    Return:
        concated (gctoo object)
    """

    # Separate each gctoo into its component dfs
    row_meta_dfs = []
    col_meta_dfs = []
    data_dfs = []
    for g in gctoos:
        row_meta_dfs.append(g.row_metadata_df)
        col_meta_dfs.append(g.col_metadata_df)
        data_dfs.append(g.data_df)

    # Concatenate row metadata
    all_row_metadata_df = concat_row_meta(row_meta_dfs, fields_to_remove, sort_headers)

    # Concatenate col metadata
    all_col_metadata_df = concat_col_meta(col_meta_dfs)

    # Concatenate the data_dfs
    all_data_df = concat_data(data_dfs)

    # Make sure df shapes are correct
    assert all_data_df.shape[0] == all_row_metadata_df.shape[0], "Number of rows is incorrect."
    assert all_data_df.shape[1] == all_col_metadata_df.shape[0], "Number of columns is incorrect."
    
    # If requested, assign unique integer as new sample id and move old sample
    # id into the column metadata
    if reset_sample_ids:
        (all_col_metadata_df, all_data_df) = do_reset_sample_ids(
            all_col_metadata_df, all_data_df)

    logger.info("build GCToo of all...")
    concated = GCToo.GCToo(row_metadata_df=all_row_metadata_df,
                           col_metadata_df=all_col_metadata_df,
                           data_df=all_data_df)

    return concated

def concat_row_meta(row_meta_dfs, fields_to_remove, sort_headers):
    """Concatenate the row metadata dfs together and sort the index.
    Args:
        row_meta_dfs (list of pandas dfs)
        fields_to_remove (list of strings): metadata fields to drop from all dfs
            before concatenating
        sort_headers (bool): set to True in order to sort the headers of each
            row_metadata_df
    Returns:
        all_row_meta_df (pandas df)
    """
    # Remove any metadata columns that will prevent probes from being identical between plates
    if fields_to_remove is not None:
        for df in row_meta_dfs:
            df.drop(fields_to_remove, axis=1, inplace=True)

    # Sort metadata headers in case the order is different between dfs
    if sort_headers:
        for df in row_meta_dfs:
            df.sort_index(axis=1, inplace=True)

    # Concat all row_meta_df and then remove duplicate rows (slow...but it works)
    all_row_meta_df_dups = pd.concat(row_meta_dfs, axis=0)
    logger.debug("all_row_meta_df_dups.shape: {}".format(all_row_meta_df_dups.shape))
    all_row_meta_df = all_row_meta_df_dups.drop_duplicates()
    logger.debug("all_row_meta_df.shape: {}".format(all_row_meta_df.shape))

    # Verify that there are no longer any duplicate rids
    duplicate_rids = all_row_meta_df.index.duplicated(keep=False)
    assert all_row_meta_df.index.is_unique, (
        ("The following rids are duplicated because the metadata between " +
         "different files does not agree.\nTry excluding more metadata " +
         "fields using the fields_to_remove argument.\n"
         "all_row_meta_df.index[duplicate_rids]:\n{}").format(
            all_row_meta_df.index[duplicate_rids]))

    # Finally, re-sort the index
    all_row_metadata_df_sorted = all_row_meta_df.sort_index()

    return all_row_metadata_df_sorted

def concat_col_meta(col_meta_dfs):
    """Concatenate the column metadata dfs together.
    Args:
        col_meta_dfs (list of pandas dfs)
    Returns:
        all_col_meta_df (pandas df)
    """
    # Concatenate the col_meta_dfs
    all_col_meta_df = pd.concat(col_meta_dfs, axis=0)

    # Sanity check: the number of rows in all_col_metadata_df should correspond
    # to the sum of the number of rows in the input dfs
    n_rows = all_col_meta_df.shape[0]
    n_rows_cumulative = sum([df.shape[0] for df in col_meta_dfs])
    assert n_rows == n_rows_cumulative

    logger.debug("all_col_meta_df.shape[0]: {}".format(n_rows))

    return all_col_meta_df


def concat_data(data_dfs):
    """Concatenate the data dfs together.
    Args:
        data_dfs (list of pandas dfs)
    Returns:
        all_data_df_sorted (pandas df)
    """
    # Concatenate the data_dfs
    all_data_df = pd.concat(data_dfs, axis=1)

    # Sort the index
    all_data_df_sorted = all_data_df.sort_index()

    # Sanity check: the number of columns in all_data_df_sorted should correspond
    # to the sum of the number of columns in the input dfs
    n_cols = all_data_df_sorted.shape[1]
    n_cols_cumulative = sum([df.shape[1] for df in data_dfs])
    assert n_cols == n_cols_cumulative

    logger.debug("all_data_df_sorted.shape[1]: {}".format(n_cols))

    return all_data_df_sorted


def do_reset_sample_ids(all_col_metadata_df, all_data_df):
    """Rename sample ids in both metadata and data dfs to unique integers.
    Note that the dataframes are modified in-place.
    In order to save the output as a proper gct file, the sample ids need to be
    unique. If sample ids are not unique, then this function will error out.
    However, if you want to concatenate files anyway, you can use the flag
    reset_sample_ids to move the cids to a new metadata field and assign a unique
    integer index for each sample.
    Args:
        all_col_metadata_df (pandas df)
        all_data_df (pandas df)
    Returns:
        all_col_metadata_df (pandas df): updated
        all_data_df (pandas df): updated
    """
    # See how many samples are repeated before resetting
    logger.debug("num samples: {}".format(all_col_metadata_df.shape[0]))
    logger.debug("num unique samples before reset: {}".format(
        len(all_col_metadata_df.index.unique())))

    # First, make sure sample ids agree between data df and col_meta_df
    assert all_col_metadata_df.index.equals(all_data_df.columns), (
        "Sample ids in all_col_metadata_df do not agree with the sample ids in data_df.")

    # Change index name so that the column that it becomes will be
    # appropriately named
    all_col_metadata_df.index.name = "old_cid"

    # Reset index
    all_col_metadata_df = all_col_metadata_df.reset_index()

    # Change the index name back to cid
    all_col_metadata_df.index.name = "cid"

    # Replace sample ids in data_df with the new ones from col_meta_df (just an
    # array of unique integers, zero-indexed)
    all_data_df.columns = pd.Index(all_col_metadata_df.index.values)

    # Assert that the number of unique samples now equals the number of samples
    logger.debug("num unique samples after reset: {}".format(
        len(all_col_metadata_df.index.unique())))
    assert all_col_metadata_df.shape[0] == len(all_col_metadata_df.index.unique()), (
        "The sample ids in all_col_metadata_df still are not unique! Not good! " +
        "\nall_col_metadata_df.index.values:\n{}").format(all_col_metadata_df.index.values)

    return all_col_metadata_df, all_data_df


if __name__ == "__main__":
    args = build_parser().parse_args(sys.argv[1:])
    setup_logger.setup(verbose=args.verbose)

    main(args)