import logging
import setup_GCToo_logger as setup_logger
import unittest
import os
import tables
import pandas 
from pandas.util.testing import assert_frame_equal
import numpy 
import GCToo
import h5py
import parse_gctoox
import write_gctoox
import mini_gctoo_for_testing

__author__ = "Oana Enache"
__email__ = "oana@broadinstitute.org"

FUNCTIONAL_TESTS_PATH = "functional_tests"

# instantiate logger
logger = logging.getLogger(setup_logger.LOGGER_NAME)

# instance of mini_gctoo for testing
mini_gctoo = mini_gctoo_for_testing.make()

version_node = "version"
rid_node = "/0/META/ROW/id"
cid_node = "/0/META/COL/id"
data_node = "/0/DATA/0/matrix"
row_meta_group_node = "/0/META/ROW"
col_meta_group_node = "/0/META/COL"


metadata_prefix = "/0/META" 
row_metadata_suffix = "ROW" 
col_metadata_suffix = "COL"
cid_suffix = "id" 
rid_suffix = "id"

class TestParseGCTooX(unittest.TestCase):
	def test_add_gctx_to_out_name(self):
		name1 = "my_cool_file"
		name2 = "my_other_cool_file.gctx"

		# case 1: out file name doesn't end in gctx
		out_name1 = write_gctoox.add_gctx_to_out_name(name1)
		self.assertTrue(out_name1 == name1 + ".gctx", 
			("out name should be my_cool_file.gctx, not {}").format(out_name1))

		# case 2: out file name does end in gctx
		out_name2 = write_gctoox.add_gctx_to_out_name(name2)
		self.assertTrue(out_name2 == name2,
			("out name should be my_other_cool_file.gctx, not {}").format(out_name2))

	def test_write_data_matrix(self):

		# write data df to file & then close pytables writer
		hdf5_writer = tables.openFile(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_data_matrix.gctx"), mode = "w")
		write_gctoox.write_data_matrix(hdf5_writer, mini_gctoo)
		# write ids to file, otherwise extract_data_df throws exception
		hdf5_writer.createGroup(metadata_prefix, row_metadata_suffix, createparents = True)
		hdf5_writer.createArray(os.path.join(metadata_prefix, row_metadata_suffix), 
			rid_suffix, numpy.array(list(mini_gctoo.data_df.index)))
		hdf5_writer.createGroup(metadata_prefix, col_metadata_suffix, createparents = True)
		hdf5_writer.createArray(os.path.join(metadata_prefix, col_metadata_suffix), 
			cid_suffix, numpy.array(list(mini_gctoo.data_df.columns)))
		hdf5_writer.close()

		# read in written data df, then close & delete file
		open_mini_gctoo_data_df = h5py.File(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_data_matrix.gctx"), "r")
		rid_dset = open_mini_gctoo_data_df[rid_node]
		cid_dset = open_mini_gctoo_data_df[cid_node]
		# store appropriate id information in dict for O(1) lookup
		# Note: if slicing in 
		id_info = parse_gctoox.make_id_info_dict(rid_dset, cid_dset, None, None)

		# get data dset
		data_dset = open_mini_gctoo_data_df[data_node]
		# write data (or specified slice thereof) to pandas DataFrame
		written_data_df = parse_gctoox.parse_data_df(data_dset, id_info)
		logger.debug("Shape of original data df: {}".format(mini_gctoo.data_df.shape))
		logger.debug("Shape of written data df: {}".format(written_data_df.shape))
		open_mini_gctoo_data_df.close()
		os.remove(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_data_matrix.gctx"))

		# check rows and columns
		self.assertTrue(set(mini_gctoo.data_df.index) == set(written_data_df.index),
			"Mismatch between expected index values of data df {} and index values written to file: {}".format(mini_gctoo.data_df.index,written_data_df.index))
		self.assertTrue(set(mini_gctoo.data_df.columns) == set(written_data_df.columns),
			"Mismatch between expected column values of data df {} and column values written to file: {}".format(mini_gctoo.data_df.columns, written_data_df.columns))
		assert_frame_equal(mini_gctoo.data_df.sort(axis=1), written_data_df.sort(axis=1))

	def test_write_metadata(self):
		"""
		CASE 1:
			- write metadata (has '-666') to file, do not convert -666
			- parse in written metadata, don't convert -666 
		"""
		# write row and col metadata fields from mini_gctoo_for_testing instance to file
		hdf5_writer = tables.openFile(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_metadata.gctx"), mode = "w")
		write_gctoox.write_metadata(hdf5_writer, "row", mini_gctoo.row_metadata_df, False)
		write_gctoox.write_metadata(hdf5_writer, "col", mini_gctoo.col_metadata_df, False)
		hdf5_writer.close()
		logger.debug("Wrote mini_gctoo_metadata.gctx to {}".format(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_metadata.gctx")))

		# read in written metadata, then close and delete file
		open_mini_gctoo_metadata = h5py.File(FUNCTIONAL_TESTS_PATH + "/mini_gctoo_metadata.gctx", "r", driver = "core")	
		# get dsets corresponding to rids, cids 
		rid_dset = open_mini_gctoo_metadata[rid_node]
		cid_dset = open_mini_gctoo_metadata[cid_node]
		id_info = parse_gctoox.make_id_info_dict(rid_dset, cid_dset, None, None)
		row_meta_group = open_mini_gctoo_metadata[row_meta_group_node]
		mini_gctoo_row_metadata =  parse_gctoox.make_meta_df("rids", row_meta_group, id_info, False)

		col_meta_group = open_mini_gctoo_metadata[col_meta_group_node]
		mini_gctoo_col_metadata =  parse_gctoox.make_meta_df("cids", col_meta_group, id_info, False)

		open_mini_gctoo_metadata.close()
		os.remove(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_metadata.gctx"))

		# check row metadata
		self.assertTrue(set(mini_gctoo.row_metadata_df.columns) == set(mini_gctoo_row_metadata.columns),
			"Mismatch between expected row metadata columns {} and column values written to file: {}".format(mini_gctoo.row_metadata_df.columns, mini_gctoo_row_metadata.columns))
		self.assertTrue(set(mini_gctoo.row_metadata_df.index) == set(mini_gctoo.col_metadata_df.index),
			"Mismatch between expect row metadata index {} and index values written to file: {}".format(mini_gctoo.row_metadata_df.index, mini_gctoo_row_metadata.index))
		for c in list(mini_gctoo.row_metadata_df.columns):
			logger.debug("C1: For column name: {}".format(c))
			logger.debug("C1: populated values: {}".format(set(mini_gctoo_row_metadata[c])))
			logger.debug("C1: mini_gctoo values: {}".format(set(mini_gctoo.row_metadata_df[c])))
			self.assertTrue(set(mini_gctoo.row_metadata_df[c]) == set(mini_gctoo_row_metadata[c]), 
				"Values in column {} differ between expected metadata and written row metadata!".format(c))

		# check col metadata
		self.assertTrue(set(mini_gctoo.col_metadata_df.columns) == set(mini_gctoo_col_metadata.columns),
			"Mismatch between expected col metadata columns {} and column values written to file: {}".format(mini_gctoo.col_metadata_df.columns, mini_gctoo_col_metadata.columns))
		self.assertTrue(set(mini_gctoo.col_metadata_df.index) == set(mini_gctoo.col_metadata_df.index),
			"Mismatch between expect col metadata index {} and index values written to file: {}".format(mini_gctoo.col_metadata_df.index, mini_gctoo_col_metadata.index))
		for c in list(mini_gctoo.col_metadata_df.columns):
			self.assertTrue(set(mini_gctoo.col_metadata_df[c]) == set(mini_gctoo_col_metadata[c]), 
				"Values in column {} differ between expected metadata and written col metadata!".format(c))

		"""
		CASE 2:
			- write metadata (has NaN, not '-666') to file, do convert NaN back to '-666'
			- parse in written metadata, don't convert -666 
		"""

		# first convert mini_gctoo's row & col metadata dfs -666s to NaN
		converted_row_metadata = mini_gctoo.row_metadata_df.replace([-666, "-666", -666.0], [numpy.nan, numpy.nan, numpy.nan])
		logger.debug("First row of converted_row_metadata: {}".format(converted_row_metadata.iloc[0]))
		converted_col_metadata = mini_gctoo.col_metadata_df.replace([-666, "-666", -666.0], [numpy.nan, numpy.nan, numpy.nan])

		# write row and col metadata fields from mini_gctoo_for_testing instance to file
		# Note this time does convert back to -666
		hdf5_writer = tables.openFile(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_metadata.gctx"), mode = "w")
		write_gctoox.write_metadata(hdf5_writer, "row", converted_row_metadata, True)
		write_gctoox.write_metadata(hdf5_writer, "col", converted_col_metadata, True)
		hdf5_writer.close()

		# read in written metadata, then close and delete file
		open_mini_gctoo_metadata = h5py.File(FUNCTIONAL_TESTS_PATH + "/mini_gctoo_metadata.gctx", "r", driver = "core")	
		# get dsets corresponding to rids, cids 
		rid_dset = open_mini_gctoo_metadata[rid_node]
		cid_dset = open_mini_gctoo_metadata[cid_node]
		id_info = parse_gctoox.make_id_info_dict(rid_dset, cid_dset, None, None)
		row_meta_group = open_mini_gctoo_metadata[row_meta_group_node]
		col_meta_group = open_mini_gctoo_metadata[col_meta_group_node]

		mini_gctoo_row_metadata =  parse_gctoox.make_meta_df("rids", row_meta_group, id_info, False)
		mini_gctoo_col_metadata = parse_gctoox.make_meta_df("cids", col_meta_group, id_info, False)		

		open_mini_gctoo_metadata.close()
		os.remove(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_metadata.gctx"))

		# check row metadata
		self.assertTrue(set(mini_gctoo.row_metadata_df.columns) == set(mini_gctoo_row_metadata.columns),
			"Mismatch between expected row metadata columns {} and column values written to file: {}".format(mini_gctoo.row_metadata_df.columns, mini_gctoo_row_metadata.columns))
		self.assertTrue(set(mini_gctoo.row_metadata_df.index) == set(mini_gctoo.col_metadata_df.index),
			"Mismatch between expect row metadata index {} and index values written to file: {}".format(mini_gctoo.row_metadata_df.index, mini_gctoo_row_metadata.index))
		for c in list(mini_gctoo.row_metadata_df.columns):
			logger.debug("C2: For column name: {}".format(c))
			logger.debug("C2: populated values: {}".format(set(mini_gctoo_row_metadata[c])))
			logger.debug("C2: mini_gctoo values: {}".format(set(mini_gctoo.row_metadata_df[c])))
			self.assertTrue(set(mini_gctoo.row_metadata_df[c]) == set(mini_gctoo_row_metadata[c]), 
				"Values in column {} differ between expected metadata and written row metadata!".format(c))

		# check col metadata
		self.assertTrue(set(mini_gctoo.col_metadata_df.columns) == set(mini_gctoo_col_metadata.columns),
			"Mismatch between expected col metadata columns {} and column values written to file: {}".format(mini_gctoo.col_metadata_df.columns, mini_gctoo_col_metadata.columns))
		self.assertTrue(set(mini_gctoo.col_metadata_df.index) == set(mini_gctoo.col_metadata_df.index),
			"Mismatch between expect col metadata index {} and index values written to file: {}".format(mini_gctoo.col_metadata_df.index, mini_gctoo_col_metadata.index))
		for c in list(mini_gctoo.col_metadata_df.columns):
			self.assertTrue(set(mini_gctoo.col_metadata_df[c]) == set(mini_gctoo_col_metadata[c]), 
				"Values in column {} differ between expected metadata and written col metadata!".format(c))

if __name__ == "__main__":
	setup_logger.setup(verbose = True)
	unittest.main()