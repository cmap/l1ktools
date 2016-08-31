import numpy as np
import pandas as pd
import numpy as np
import logging
import setup_GCToo_logger as setup_logger

__authors__ = 'Oana Enache, Lev Litichevskiy, Dave Lahr'
__email__ = 'dlahr@broadinstitute.org'

"""
DATA:
-----------------------------
|  |          cid           |
-----------------------------
|  |                        |
|r |                        |
|i |          data          |
|d |                        |
|  |                        |
-----------------------------
ROW METADATA:
--------------------------
|id|        rhd          |
--------------------------
|  |                     |
|r |                     |
|i |    row_metadata     |
|d |                     |
|  |                     |
--------------------------
COLUMN METADATA:
N.B. The df is transposed from how it looks in a gct file.
---------------------
|id|      chd       |
---------------------
|  |                |
|  |                |
|  |                |
|c |                |
|i |  col_metadata  |
|d |                |
|  |                |
|  |                |
|  |                |
---------------------
N.B. rids, cids, rhds, and chds must be unique.
"""


class GCToo(object):
    """Class representing parsed gct(x) objects as pandas dataframes.
    Contains 3 component dataframes (row_metadata_df, column_metadata_df,
    and data_df) as well as an assembly of these 3 into a multi index df
    that provides an alternate way of selecting data.
    """
    def __init__(self, data_df=None, row_metadata_df=None, col_metadata_df=None,
                 src=None, version=None, logger_name=setup_logger.LOGGER_NAME):
        self.logger = logging.getLogger(logger_name)

        self.src = src
        self.version = version
        self.row_metadata_df = row_metadata_df
        self.col_metadata_df = col_metadata_df
        self.data_df = data_df
        self.multi_index_df = None
        
        # check that data frames actually are dataframes, then check uniqueness
        for df in [self.row_metadata_df, self.col_metadata_df, self.data_df]:
            if df is not None:
                # check it's a data frame
                if isinstance(df, pd.DataFrame):
                    # check uniqueness of index and columns
                    self.check_uniqueness(df.index)
                    self.check_uniqueness(df.columns)
                else:
                    self.logger.error("{} is not a pandas DataFrame instance!".format(df))
                
        # check rid matching in data & metadata
        if ((self.data_df is not None) and (self.row_metadata_df is not None)):
            self.rid_consistency_check()
        
        # check cid matching in data & metadata
        if (self.data_df is not None) and (self.col_metadata_df is not None):
            self.cid_consistency_check()

        # If all three component dataframes exist, assemble multi_index_df
        if ((self.row_metadata_df is not None) and
                (self.col_metadata_df is not None) and
                (self.data_df is not None)):
            self.assemble_multi_index_df()
            
    def __str__(self):
        """Prints a string representation of a GCToo object."""
        version = "GCT v{}\n".format(self.version)
        source = "src: {}\n".format(self.src)

        if self.data_df is not None:
            data = "data_df: [{} rows x {} columns]\n".format(
            self.data_df.shape[0], self.data_df.shape[1])
        else:
            data = "data_df: None\n"

        if self.row_metadata_df is not None:
            row_meta = "row_metadata_df: [{} rows x {} columns]\n".format(
            self.row_metadata_df.shape[0], self.row_metadata_df.shape[1])
        else:
            row_meta = "row_metadata_df: None\n"

        if self.col_metadata_df is not None:
            col_meta = "col_metadata_df: [{} rows x {} columns]".format(
            self.col_metadata_df.shape[0], self.col_metadata_df.shape[1])
        else:
            col_meta = "col_metadata_df: None\n"

        full_string = (version + source + data + row_meta + col_meta)
        return full_string

    def assemble_multi_index_df(self):
        """Assembles three component dataframes into a multiindex dataframe.
        Sets the result to self.multi_index_df.
        IMPORTANT: Cross-section ("xs") is the best command for selecting
        data. Be sure to use the flag "drop_level=False" with this command,
        or else the dataframe that is returned will not have the same
        metadata as the input.
        N.B. "level" means metadata header.
        N.B. "axis=1" indicates column annotations.
        Examples:
            1) Select the probe with pr_lua_id="LUA-3404":
            lua3404_df = multi_index_df.xs("LUA-3404", level="pr_lua_id", drop_level=False)
            2) Select all DMSO samples:
            DMSO_df = multi_index_df.xs("DMSO", level="pert_iname", axis=1, drop_level=False)
        """
        #prepare row index
        self.logger.debug("Row metadata shape: {}".format(self.row_metadata_df.shape))
        self.logger.debug("Is empty? {}".format(self.row_metadata_df.empty))
        row_copy = pd.DataFrame(self.row_metadata_df.index) if self.row_metadata_df.empty else self.row_metadata_df.copy()
        row_copy["rid"] = row_copy.index
        row_index = pd.MultiIndex.from_arrays(row_copy.T.values, names=row_copy.columns)

        #prepare column index
        self.logger.debug("Col metadata shape: {}".format(self.col_metadata_df.shape))
        col_copy = pd.DataFrame(self.col_metadata_df.index) if self.col_metadata_df.empty else self.col_metadata_df.copy()
        col_copy["cid"] = col_copy.index
        transposed_col_metadata = col_copy.T
        col_index = pd.MultiIndex.from_arrays(transposed_col_metadata.values, names=transposed_col_metadata.index)

        # Create multi index dataframe using the values of data_df and the indexes created above
        self.logger.debug("Data df shape: {}".format(self.data_df.shape))
        self.multi_index_df = pd.DataFrame(data=self.data_df.values, index=row_index, columns=col_index)

    def check_uniqueness(self, field):
        """
        Checks that elements contained within a given field are unique.
        
        Input: 
            - field (pandas.core.index): Either the index or columns of a pandas DataFrame instance
        """
        assert field.is_unique, (
            ("Field must be unique but is not, field.values:\n{}".format(
                field.values)))

    def rid_consistency_check(self):
        assert self.data_df.index.equals(self.row_metadata_df.index), (
            ("The rids are inconsistent between data_df and row_metadata_df.\n" +
             "self.data_df.index.values:\n{}\nself.row_metadata_df.index.values:\n{}").format(
                self.data_df.index.values, self.row_metadata_df.index.values))
                
    def cid_consistency_check(self):
        assert self.data_df.columns.equals(self.col_metadata_df.index), (
            ("The cids are inconsistent between data_df and col_metadata_df.\n" +
             "self.data_df.columns.values:\n{},\nself.col_metadata_df.index.values:\n{}").format(
                self.data_df.columns.values, self.col_metadata_df.index.values))


def multi_index_df_to_component_dfs(multi_index_df):
    """ Convert a multi-index df into 3 component dfs. """

    # Id level of the multiindex will become the index
    rids = list(multi_index_df.index.get_level_values("rid"))
    cids = list(multi_index_df.columns.get_level_values("cid"))

    # Drop rid and cid because they won't go into the body of the metadata
    mi_df_index = multi_index_df.index.droplevel("rid")
    mi_df_columns = multi_index_df.columns.droplevel("cid")

    # Names of the multiindex become the headers
    rhds = list(mi_df_index.names)
    chds = list(mi_df_columns.names)

    # Assemble metadata values
    row_metadata = np.array([mi_df_index.get_level_values(level).values for level in list(rhds)]).T
    col_metadata = np.array([mi_df_columns.get_level_values(level).values for level in list(chds)]).T

    # Create component dfs
    row_metadata_df = pd.DataFrame.from_records(row_metadata, index=pd.Index(rids, name="rid"), columns=pd.Index(rhds, name="rhd"))
    col_metadata_df = pd.DataFrame.from_records(col_metadata, index=pd.Index(cids, name="cid"), columns=pd.Index(chds, name="chd"))
    data_df = pd.DataFrame(multi_index_df.values, index=pd.Index(rids, name="rid"), columns=pd.Index(cids, name="cid"))

    return data_df, row_metadata_df, col_metadata_df
