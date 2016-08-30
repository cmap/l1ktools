import logging
import setup_GCToo_logger as setup_logger
import unittest
import os
import tables
import pandas 
from pandas.util.testing import assert_frame_equal
import numpy 
import GCToo
import GCTXAttrInfo
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
		# Need an instance of GCTXAttrInfo for data reading method to work
		gctoo_attr = GCTXAttrInfo.GCTXAttrInfo()

		# write data df to file & then close pytables writer
		hdf5_writer = tables.openFile(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_data_matrix.gctx"), mode = "w")
		write_gctoox.write_data_matrix(hdf5_writer, mini_gctoo)
		# write ids to file, otherwise extract_data_df throws exception
		hdf5_writer.createGroup(gctoo_attr.metadata_prefix, gctoo_attr.row_metadata_suffix, createparents = True)
		hdf5_writer.createArray(os.path.join(gctoo_attr.metadata_prefix, gctoo_attr.row_metadata_suffix), gctoo_attr.rid_suffix, numpy.array(list(mini_gctoo.data_df.index)))
		hdf5_writer.createGroup(gctoo_attr.metadata_prefix, gctoo_attr.col_metadata_suffix, createparents = True)
		hdf5_writer.createArray(os.path.join(gctoo_attr.metadata_prefix, gctoo_attr.col_metadata_suffix), gctoo_attr.cid_suffix, numpy.array(list(mini_gctoo.data_df.columns)))
		hdf5_writer.close()

		# read in written data df, then close & delete file
		open_mini_gctoo_data_df = parse_gctoox.open_gctx_file(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_data_matrix.gctx"))
		logger.debug("{} was successfully opened by pytables!".format(open_mini_gctoo_data_df))
		written_data_df = parse_gctoox.parse_data_df(open_mini_gctoo_data_df, gctoo_attr, None, None)
		logger.debug("Shape of original data df: {}".format(mini_gctoo.data_df.shape))
		logger.debug("Shape of written data df: {}".format(written_data_df.shape))
		open_mini_gctoo_data_df.close()
		os.remove(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_data_matrix.gctx"))

		# check rows and columns
		self.assertTrue(set(list(mini_gctoo.data_df.index)) == set(list(written_data_df.index)),
			"Mismatch between expected index values of data df {} and index values written to file: {}".format(mini_gctoo.data_df.index,written_data_df.index))
		self.assertTrue(set(list(mini_gctoo.data_df.columns)) == set(list(written_data_df.columns)),
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

		# read in written metadata, then close and delete file
		open_mini_gctoo_metadata = parse_gctoox.open_gctx_file(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_metadata.gctx"))
		node_info = GCTXAttrInfo.GCTXAttrInfo()
		rids = list(open_mini_gctoo_metadata.getNode(os.path.join(node_info.metadata_prefix, node_info.row_metadata_suffix), node_info.rid_suffix))
		cids = list(open_mini_gctoo_metadata.getNode(os.path.join(node_info.metadata_prefix, node_info.col_metadata_suffix), node_info.cid_suffix))
		mini_gctoo_row_metadata = parse_gctoox.parse_metadata("row", node_info, open_mini_gctoo_metadata, None, None, False)
		mini_gctoo_col_metadata = parse_gctoox.parse_metadata("col", node_info, open_mini_gctoo_metadata, None, None, False)
		open_mini_gctoo_metadata.close()
		os.remove(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_metadata.gctx"))

		# check row metadata
		self.assertTrue(set(list(mini_gctoo.row_metadata_df.columns)) == set(list(mini_gctoo_row_metadata.columns)),
			"Mismatch between expected row metadata columns {} and column values written to file: {}".format(mini_gctoo.row_metadata_df.columns, mini_gctoo_row_metadata.columns))
		self.assertTrue(set(list(mini_gctoo.row_metadata_df.index)) == set(list(mini_gctoo.col_metadata_df.index)),
			"Mismatch between expect row metadata index {} and index values written to file: {}".format(mini_gctoo.row_metadata_df.index, mini_gctoo_row_metadata.index))
		for c in list(mini_gctoo.row_metadata_df.columns):
			logger.debug("C1: For column name: {}".format(c))
			logger.debug("C1: populated values: {}".format(set(list(mini_gctoo_row_metadata[c]))))
			logger.debug("C1: mini_gctoo values: {}".format(set(list(mini_gctoo.row_metadata_df[c]))))
			self.assertTrue(set(list(mini_gctoo.row_metadata_df[c])) == set(list(mini_gctoo_row_metadata[c])), 
				"Values in column {} differ between expected metadata and written row metadata!".format(c))

		# check col metadata
		self.assertTrue(set(list(mini_gctoo.col_metadata_df.columns)) == set(list(mini_gctoo_col_metadata.columns)),
			"Mismatch between expected col metadata columns {} and column values written to file: {}".format(mini_gctoo.col_metadata_df.columns, mini_gctoo_col_metadata.columns))
		self.assertTrue(set(list(mini_gctoo.col_metadata_df.index)) == set(list(mini_gctoo.col_metadata_df.index)),
			"Mismatch between expect col metadata index {} and index values written to file: {}".format(mini_gctoo.col_metadata_df.index, mini_gctoo_col_metadata.index))
		for c in list(mini_gctoo.col_metadata_df.columns):
			self.assertTrue(set(list(mini_gctoo.col_metadata_df[c])) == set(list(mini_gctoo_col_metadata[c])), 
				"Values in column {} differ between expected metadata and written col metadata!".format(c))

		"""
		CASE 2:
			- write metadata (has NaN, not '-666') to file, do convert NaN back to '-666'
			- parse in written metadata, don't convert -666 
		"""

		# first convert mini_gctoo's row & col metadata dfs -666s to NaN
		converted_row_metadata = parse_gctoox.replace_666(mini_gctoo.row_metadata_df, True)
		logger.debug("First row of converted_row_metadata: {}".format(converted_row_metadata.iloc[0]))
		converted_col_metadata = parse_gctoox.replace_666(mini_gctoo.col_metadata_df, True)

		# write row and col metadata fields from mini_gctoo_for_testing instance to file
		# Note this time does convert back to -666
		hdf5_writer = tables.openFile(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_metadata.gctx"), mode = "w")
		write_gctoox.write_metadata(hdf5_writer, "row", converted_row_metadata, True)
		write_gctoox.write_metadata(hdf5_writer, "col", converted_col_metadata, True)
		hdf5_writer.close()

		# read in written metadata, then close and delete file
		open_mini_gctoo_metadata = parse_gctoox.open_gctx_file(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_metadata.gctx"))
		node_info = GCTXAttrInfo.GCTXAttrInfo()
		rids = list(open_mini_gctoo_metadata.getNode(os.path.join(node_info.metadata_prefix, node_info.row_metadata_suffix), node_info.rid_suffix))
		cids = list(open_mini_gctoo_metadata.getNode(os.path.join(node_info.metadata_prefix, node_info.col_metadata_suffix), node_info.cid_suffix))
		mini_gctoo_row_metadata = parse_gctoox.parse_metadata("row", node_info, open_mini_gctoo_metadata, None, None, False)
		mini_gctoo_col_metadata = parse_gctoox.parse_metadata("col", node_info, open_mini_gctoo_metadata, None, None, False)
		open_mini_gctoo_metadata.close()
		os.remove(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctoo_metadata.gctx"))

		# check row metadata
		self.assertTrue(set(list(mini_gctoo.row_metadata_df.columns)) == set(list(mini_gctoo_row_metadata.columns)),
			"Mismatch between expected row metadata columns {} and column values written to file: {}".format(mini_gctoo.row_metadata_df.columns, mini_gctoo_row_metadata.columns))
		self.assertTrue(set(list(mini_gctoo.row_metadata_df.index)) == set(list(mini_gctoo.col_metadata_df.index)),
			"Mismatch between expect row metadata index {} and index values written to file: {}".format(mini_gctoo.row_metadata_df.index, mini_gctoo_row_metadata.index))
		for c in list(mini_gctoo.row_metadata_df.columns):
			logger.debug("C2: For column name: {}".format(c))
			logger.debug("C2: populated values: {}".format(set(list(mini_gctoo_row_metadata[c]))))
			logger.debug("C2: mini_gctoo values: {}".format(set(list(mini_gctoo.row_metadata_df[c]))))
			self.assertTrue(set(list(mini_gctoo.row_metadata_df[c])) == set(list(mini_gctoo_row_metadata[c])), 
				"Values in column {} differ between expected metadata and written row metadata!".format(c))

		# check col metadata
		self.assertTrue(set(list(mini_gctoo.col_metadata_df.columns)) == set(list(mini_gctoo_col_metadata.columns)),
			"Mismatch between expected col metadata columns {} and column values written to file: {}".format(mini_gctoo.col_metadata_df.columns, mini_gctoo_col_metadata.columns))
		self.assertTrue(set(list(mini_gctoo.col_metadata_df.index)) == set(list(mini_gctoo.col_metadata_df.index)),
			"Mismatch between expect col metadata index {} and index values written to file: {}".format(mini_gctoo.col_metadata_df.index, mini_gctoo_col_metadata.index))
		for c in list(mini_gctoo.col_metadata_df.columns):
			self.assertTrue(set(list(mini_gctoo.col_metadata_df[c])) == set(list(mini_gctoo_col_metadata[c])), 
				"Values in column {} differ between expected metadata and written col metadata!".format(c))

	# TODO: add test case with id metadata 
	# Also look into R adding/not adding an id field

if __name__ == "__main__":
	setup_logger.setup(verbose = True)
	unittest.main()