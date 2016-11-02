import logging
import setup_GCToo_logger as setup_logger
import unittest
import h5py
import os
import numpy
import parse_gctoox
import write_gctoox
import functional_tests.mini_gctoo_for_testing as mini_gctoo_for_testing

__author__ = "Oana Enache"
__email__ = "oana@broadinstitute.org"

FUNCTIONAL_TESTS_PATH = "functional_tests"

# instantiate logger
logger = logging.getLogger(setup_logger.LOGGER_NAME)

# instance of mini_gctoo for testing
mini_gctoo = mini_gctoo_for_testing.make()

version_node = "version"
src_node = "src"
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

	def test_write_src(self):
		# case 1: gctoo obj doesn't have src
		mini1 = mini_gctoo_for_testing.make()
		mini1.src = None 
		write_gctoox.write(mini1, "no_src_example")
		hdf5_file = h5py.File("no_src_example.gctx")
		hdf5_src1 = hdf5_file.attrs[src_node]
		hdf5_file.close()
		self.assertEqual(hdf5_src1, "no_src_example.gctx")
		os.remove("no_src_example.gctx")
		
		# case 2: gctoo obj does have src 
		mini2 = mini_gctoo_for_testing.make()
		write_gctoox.write(mini2, "with_src_example.gctx")
		hdf5_file = h5py.File("with_src_example.gctx")
		hdf5_src2 = hdf5_file.attrs[src_node]
		hdf5_file.close()
		self.assertEqual(hdf5_src2, "mini_gctoo.gctx")
		os.remove("with_src_example.gctx")

	def test_write_version(self):
		# case 1: gctoo obj doesn't have version
		mini1 = mini_gctoo_for_testing.make()
		mini1.version = None 
		write_gctoox.write(mini1, "no_version_example")
		hdf5_file = h5py.File("no_version_example.gctx")
		hdf5_v1 = hdf5_file.attrs[version_node]
		hdf5_file.close()
		self.assertEqual(hdf5_v1, "GCTX1.0")
		os.remove("no_version_example.gctx")

		# case 2: gctoo obj does have version
		mini2 = mini_gctoo_for_testing.make()
		mini2.version = "MY_VERSION"
		write_gctoox.write(mini2, "with_version_example")
		hdf5_file = h5py.File("with_version_example.gctx")
		hdf5_v2 = hdf5_file.attrs[version_node]
		hdf5_file.close()
		self.assertEqual(hdf5_v2, "MY_VERSION")
		os.remove("with_version_example.gctx")

	def test_write_metadata(self):
		"""
		CASE 1:
			- write metadata (has '-666') to file, do not convert -666
			- parse in written metadata, don't convert -666 
		"""
		hdf5_writer = h5py.File(FUNCTIONAL_TESTS_PATH + "/mini_gctoo_metadata.gctx", "w")
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
		os.remove(FUNCTIONAL_TESTS_PATH + "/mini_gctoo_metadata.gctx")

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
		hdf5_writer = h5py.File(FUNCTIONAL_TESTS_PATH + "/mini_gctoo_metadata.gctx", "w")
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
		os.remove(FUNCTIONAL_TESTS_PATH + "/mini_gctoo_metadata.gctx")

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