import logging
import setup_GCToo_logger as setup_logger
import unittest
import os
import pandas as pd
import numpy as np
import GCToo
import h5py
import parse_gctoox
import mini_gctoo_for_testing
from pandas.util.testing import assert_frame_equal

__author__ = "Oana Enache"
__email__ = "oana@broadinstitute.org"

FUNCTIONAL_TESTS_PATH = "functional_tests"

logger = logging.getLogger(setup_logger.LOGGER_NAME)

version_node = "version"
rid_node = "/0/META/ROW/id"
cid_node = "/0/META/COL/id"
data_node = "/0/DATA/0/matrix"
row_meta_group_node = "/0/META/ROW"
col_meta_group_node = "/0/META/COL"

class TestParseGCToox(unittest.TestCase):
	def test_check_id_inputs(self):
		ridx = [0,1,2]
		cidx = [4, 6, 8, 9]
		rid = ["a", "b", "c"]
		cid = ["l", "m", "n", "o"]

		# case 1: row and col lists are populated and same type
		self.assertEqual((ridx, cidx), parse_gctoox.check_id_inputs(ridx, None, cidx, None))

		# case 2: row list and col lists are populated different types
		with self.assertRaises(Exception) as context:
			parse_gctoox.check_id_inputs(None, ridx, cid, None)
		self.assertTrue("Please specify ids to subset in a consistent manner" in str(context.exception))

		# case 3: row list and col lists are both None 
		self.assertEqual(([], []), parse_gctoox.check_id_inputs(None, None, None, None))

		# case 4: row list is populated, col list is None 
		self.assertEqual((rid, []), parse_gctoox.check_id_inputs(rid, None, None, None))

	def test_check_id_idx_exclusivity(self):
		ids = ["a", "b", "c"]
		idx = [0, 1, 2]

		# case 1: id != None and idx != None
		with self.assertRaises(Exception) as context:
			parse_gctoox.check_id_idx_exclusivity(ids, idx)
		self.assertTrue("'id' and 'idx' fields can't both not be None" in str(context.exception))

		# case 2: id != None
		self.assertEqual(ids, parse_gctoox.check_id_idx_exclusivity(ids, None))

		# case 3: idx != None
		self.assertEqual(idx, parse_gctoox.check_id_idx_exclusivity(None, idx))

		# case 4: id == None & idx == None 	
		self.assertEqual([], parse_gctoox.check_id_idx_exclusivity(None, None))	

	def test_parse_metadata_df(self):
		mini_gctoo = mini_gctoo_for_testing.make()
		# convert row_metadata to np.nan
		mini_row_meta = mini_gctoo.row_metadata_df.replace([-666, "-666", -666.0], 
			[np.nan, np.nan, np.nan])

		gctx_file = h5py.File("mini_gctoo_for_testing.gctx", "r")
		row_dset = gctx_file[row_meta_group_node]
		col_dset = gctx_file[col_meta_group_node]

		# with convert_neg_666
		row_df = parse_gctoox.parse_metadata_df("row", row_dset, True)
		assert_frame_equal(mini_row_meta, row_df)

		# no convert_neg_666
		col_df = parse_gctoox.parse_metadata_df("col", col_dset, False)
		assert_frame_equal(mini_gctoo.col_metadata_df, col_df)

	def test_set_metadata_index_and_column_names(self):
		mini_gctoo = mini_gctoo_for_testing.make()
		mini_gctoo.row_metadata_df.index.name = None
		mini_gctoo.row_metadata_df.columns.name = None 
		mini_gctoo.col_metadata_df.index.name = None
		mini_gctoo.col_metadata_df.columns.name = None 

		# case 1: dim == "row"
		parse_gctoox.set_metadata_index_and_column_names("row", mini_gctoo.row_metadata_df)
		self.assertEqual(mini_gctoo.row_metadata_df.index.name, "rid")
		self.assertEqual(mini_gctoo.row_metadata_df.columns.name, "rhd")

		# case 2: dim == "col"
		parse_gctoox.set_metadata_index_and_column_names("col", mini_gctoo.col_metadata_df)
		self.assertEqual(mini_gctoo.col_metadata_df.index.name, "cid")
		self.assertEqual(mini_gctoo.col_metadata_df.columns.name, "chd")

	def test_get_ordered_idx(self):
		mini_gctoo = mini_gctoo_for_testing.make()

		# case 1 : len(id_list) == 0
		id_list1 = []
		expected_sorted_idx_list1 = range(0, len(list(mini_gctoo.col_metadata_df.index)))
		sorted_idx_list1 = parse_gctoox.get_ordered_idx(id_list1, mini_gctoo.col_metadata_df)
		self.assertEqual(expected_sorted_idx_list1, sorted_idx_list1, 
			"Expected idx list {} but found {}".format(expected_sorted_idx_list1, 
				sorted_idx_list1))

		# case 2: type(id_list[0]) == str 
		# aka ids are supplied instead of idx
		id_list2 = ["LJP007_MCF10A_24H:TRT_CP:BRD-K93918653:3.33", 
			"MISC003_A375_24H:TRT_CP:BRD-K93918653:3.33"]
		expected_sorted_idx_list2 = [list(mini_gctoo.col_metadata_df.index).index(i) for i in id_list2]
		sorted_idx_list2 = parse_gctoox.get_ordered_idx(id_list2, mini_gctoo.col_metadata_df)
		self.assertEqual(expected_sorted_idx_list2, sorted_idx_list2, 
			"Expected idx list {} but found {}".format(expected_sorted_idx_list2, 
				sorted_idx_list2))

		# case 3: idx list is supplied 
		id_list3 = [1,0]
		expected_sorted_idx_list3 = [0, 1]
		sorted_idx_list3 = parse_gctoox.get_ordered_idx(id_list3, mini_gctoo.col_metadata_df)
		self.assertEqual(expected_sorted_idx_list3, sorted_idx_list3, 
			"Expected idx list {} but found {}".format(expected_sorted_idx_list3, 
				sorted_idx_list3))

	def test_parse_data_df(self):
		mini_data_df = pd.DataFrame([[-0.283359, 0.011270],[0.304119, 1.921061],[0.398655, -0.144652]], 
			index=["200814_at", "218597_s_at", "217140_s_at"], 
			columns = ["LJP005_A375_24H:DMSO:-666", "LJP005_A375_24H:BRD-K76908866:10"])
		mini_data_df = mini_data_df.astype(np.float32)
		mini_data_df.index.name = "rid"
		mini_data_df.columns.name = "cid"

		# create h5py File instance 
		mini_gctx = h5py.File("functional_tests/mini_gctx_with_metadata_n2x3.gctx", "r")
		data_dset = mini_gctx[data_node]

		# get relevant metadata fields
		col_meta = parse_gctoox.get_column_metadata("functional_tests/mini_gctx_with_metadata_n2x3.gctx")
		row_meta = parse_gctoox.get_row_metadata("functional_tests/mini_gctx_with_metadata_n2x3.gctx")

		# case 1: no slicing
		data_df1 = parse_gctoox.parse_data_df(data_dset, [0,1,2], [0,1], row_meta, col_meta)
		# note: checks to 3 decimal places 
		assert_frame_equal(mini_data_df, data_df1, 
			check_exact=False, check_less_precise = True)

		# case 2: slice; ridx < cidx 
		data_df2 = parse_gctoox.parse_data_df(data_dset, [0], [0,1], row_meta, col_meta)
		assert_frame_equal(mini_data_df.iloc[[0],[0,1]], data_df2, 
			check_exact=False, check_less_precise = True)

		# case 3: slice; ridx == cidx 
		data_df3 = parse_gctoox.parse_data_df(data_dset, [0],[0], row_meta, col_meta)
		assert_frame_equal(mini_data_df.iloc[[0],[0]], data_df3, 
			check_exact=False, check_less_precise = True)

		# case 4: slice; ridx > cidx 
		data_df4 = parse_gctoox.parse_data_df(data_dset, [0,1,2],[0], row_meta, col_meta)
		assert_frame_equal(mini_data_df.iloc[[0,1,2],[0]], data_df4, 
			check_exact=False, check_less_precise = True)
	

		mini_gctx.close()

if __name__ == "__main__":
	setup_logger.setup(verbose=True)

	unittest.main()