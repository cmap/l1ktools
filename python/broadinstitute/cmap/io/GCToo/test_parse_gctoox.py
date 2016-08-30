import logging
import setup_GCToo_logger as setup_logger
import unittest
import os
import pandas 
import numpy 
import GCToo
import GCTXAttrInfo
import parse_gctoox
import tables
from pandas.util.testing import assert_frame_equal

__author__ = "Oana Enache"
__email__ = "oana@broadinstitute.org"

FUNCTIONAL_TESTS_PATH = "functional_tests"

logger = logging.getLogger(setup_logger.LOGGER_NAME)

class TestParseGCTooX(unittest.TestCase):

	def test_open_gctx_file(self):
		# case 1: not a file
		fake_file = "does_not_exist.gctx"
		with self.assertRaises(ValueError) as context:
                	parse_gctoox.open_gctx_file(fake_file)
		self.assertIsNotNone(context.exception)
		logger.debug("context.exception:  {}".format(context.exception))
		self.assertIn("file does not exist", str(context.exception))
		self.assertIn(fake_file, str(context.exception))

		# case 2: not an hdf5 file (this one is a gct aka txt)
		gct_filepath = os.path.join(FUNCTIONAL_TESTS_PATH, "both_metadata_example_n1476x978.gct")
		with self.assertRaises(TypeError) as context:
			parse_gctoox.open_gctx_file(gct_filepath)
		self.assertIsNotNone(context.exception)
		logger.debug("context.exception:  {}".format(context.exception))
		self.assertIn("not an hdf5 file", str(context.exception))
		self.assertIn(gct_filepath, str(context.exception))

		# case 3: is an hdf5 file
		curr_gctx = parse_gctoox.open_gctx_file(os.path.join(FUNCTIONAL_TESTS_PATH, "both_metadata_example_n1476x978.gctx"))
		self.assertIsInstance(curr_gctx, tables.File, "Actual gctx file failed to open!")
		curr_gctx.close()

	def test_get_dim_specific_meta_group_info(self):
		node_info = GCTXAttrInfo.GCTXAttrInfo()

		# case 1: rows
		row_info = parse_gctoox.get_dim_specific_meta_group_info("row", node_info)
		self.assertEqual(row_info,
			(node_info.metadata_prefix, node_info.row_metadata_suffix), 
			"Incorrect GCTXAttrInfo found: {} & {}".format(row_info[0], row_info[1]))

		# case 2: cols
		col_info = parse_gctoox.get_dim_specific_meta_group_info("col", node_info)
		self.assertEqual(col_info,
			(node_info.metadata_prefix, node_info.col_metadata_suffix), 
			"Incorrect GCTXAttrInfo found: {} & {}".format(col_info[0], col_info[1]))

	def test_populate_meta_df(self):
		# what we expect the values to be
		expected = {}
		expected["pr_gene_id"] = numpy.array([  5720.,  55847.,   7416.])
		expected["pr_analyte_id"] = ['Analyte 11', 'Analyte 12', 'Analyte 12']
		expected["pr_model_id"] = numpy.array([-666., -666., -666.])
		expected["pr_analyte_num"] = numpy.array([ 11.,  12.,  12.])

		# prep before can populate meta_dict
		node_info = GCTXAttrInfo.GCTXAttrInfo()
		mini = parse_gctoox.open_gctx_file(os.path.join(FUNCTIONAL_TESTS_PATH, "mini_gctx_with_metadata_n2x3.gctx"))
		meta_group_info = parse_gctoox.get_dim_specific_meta_group_info("row", node_info)
		meta_group = mini.getNode(meta_group_info[0], meta_group_info[1])
		full_node_path = os.path.join(meta_group_info[0], meta_group_info[1])

		meta_dict = parse_gctoox.populate_meta_df(full_node_path, meta_group.__members__, mini)

		mini.close()

		self.assertTrue(expected.keys() == meta_dict.keys(),
			"Mismatch between expected keys {} and those found: {}".format(expected.keys(), meta_dict.keys()))

		for k in expected.keys():
			logger.debug("Type of value at {}: {}".format(k, type(expected[k])))
			self.assertEquals(list(expected[k]), list(meta_dict[k]),
				"Mismatch found at {} between expected {} and observed {}".format(k, list(expected[k]), list(meta_dict[k])))

	def test_get_dim_specific_id_info(self):
		node_info = GCTXAttrInfo.GCTXAttrInfo()

		# case 1: rows
		row_info = parse_gctoox.get_dim_specific_id_info("row", node_info)
		correct_row_prefix = os.path.join(node_info.metadata_prefix, node_info.row_metadata_suffix)
		correct_row_suffix = node_info.rid_suffix
		self.assertEqual(row_info,
			(correct_row_prefix, correct_row_suffix), 
			"Incorrect GCTXAttrInfo rid info found: {} & {}".format(row_info[0], row_info[1]))

		# case 2: cols
		col_info = parse_gctoox.get_dim_specific_id_info("col", node_info)
		correct_col_prefix = os.path.join(node_info.metadata_prefix, node_info.col_metadata_suffix)
		correct_col_suffix = node_info.cid_suffix
		self.assertEqual(col_info,
			(correct_col_prefix, correct_col_suffix), 
			"Incorrect GCTXAttrInfo cid info found: {} & {}".format(col_info[0], col_info[1]))

	def test_set_id_vals(self):
		id_vals = ["first:one", "second:one", "third:one"]
		# case 1: yes metadata 
		mini_df = pandas.DataFrame([[1,2], [3,4], [5,6]])
		mini_df = parse_gctoox.set_id_vals(id_vals, mini_df)
		self.assertTrue(mini_df.shape == (3,2),
			"Expected mini_df to have size (3,2) but it has size {}".format(mini_df.shape))
		self.assertTrue(list(mini_df.index) == id_vals,
			"Mismatched indexes! Expected {} but found {}".format(id_vals, mini_df.index))

		# case 2: no metadata
		empty_df = pandas.DataFrame()
		logger.debug("expected empty shape? {}".format((empty_df.shape == (0,0))))
		empty_df = parse_gctoox.set_id_vals(id_vals, empty_df)
		logger.debug("empty df after setting id values: {}".format(empty_df))
		self.assertTrue(empty_df.empty, "Expected empty DataFrame but has shape {}".format(empty_df.shape))
		self.assertTrue(list(empty_df.index) == id_vals, 
			"Mismatched indexes! Expected {} but found {}".format(id_vals, empty_df.index))

	def test_set_metadata_index_and_column_names(self):
		mini_df1 = pandas.DataFrame([[1,2],[3,4], [4,5]])

		# case 1: rows
		row_names_set = parse_gctoox.set_metadata_index_and_column_names("row", mini_df1)
		self.assertTrue(row_names_set.index.name == "rid",
			"Expected index name to be 'rid' but was {}".format(row_names_set.index.name))
		self.assertTrue(row_names_set.columns.name == "rhd",
			"Expected columns name to be 'rhd' but was {}".format(row_names_set.columns.name))

		# case 2: cols
		col_names_set = parse_gctoox.set_metadata_index_and_column_names("col", mini_df1)
		self.assertTrue(col_names_set.index.name == "cid",
			"Expected index name to be 'cid' but was {}".format(col_names_set.index.name))
		self.assertTrue(col_names_set.columns.name == "chd",
			"Expected columns name to be 'chd' but was {}".format(col_names_set.columns.name))

	def test_subset_by_ids(self):
		mini_df = pandas.DataFrame([[1,2],[3,4], [4,5]])
		mini_df.index = ["first", "second", "third"]
		mini_df.columns = ["c1", "c2"]

		rid_subset = ["first", "second"]
		cid_subset = ["c1"]

		# case 1: rid != None & cid != None
		case1 = parse_gctoox.subset_by_ids(mini_df, rid_subset, cid_subset)
		self.assertTrue(case1.shape == (2,1),
			"Expected shape when subsetting by {} & {} to be (2,1) but was {}".format(rid_subset, 
																					cid_subset,
																					case1.shape))

		# case 2: rid == None & cid != None
		case2 = parse_gctoox.subset_by_ids(mini_df, None, cid_subset)
		self.assertTrue(case2.shape == (3,1),
			"Expected shape when subsetting by {} & {} to be (3,1) but was {}".format(None, 
																					cid_subset,
																					case2.shape))

		# case 3: rid != None & cid == None 
		case3 = parse_gctoox.subset_by_ids(mini_df, rid_subset, None)
		self.assertTrue(case3.shape == (2,2),
			"Expected shape when subsetting by {} & {} to be (2,2) but was {}".format(rid_subset, 
																					None,
																					case3.shape))

		# case 4: rid == None & cid == None 
		case4 = parse_gctoox.subset_by_ids(mini_df, None, None)
		self.assertTrue(case4.shape == (3,2),
			"Expected shape when subsetting by {} & {} to be (3,2) but was {}".format(None, 
																					None, 
																					case4.shape))
	
	def test_replace_666(self):
		# case 1: convert_neg_666 = True
		case1 = ["-666", "-666", "-666"]
		case2 = [-666, -666, -666]
		case3 = ["-666", "other", "strings"]
		case4 = [100, -666, 32]
		case5 = [1.1, 1.7853, -666.0]

		expected_dict = {}
		expected_dict["c1"] = [numpy.nan, numpy.nan, numpy.nan]
		expected_dict["c2"] = [numpy.nan, numpy.nan, numpy.nan]
		expected_dict["c3"] = [numpy.nan, "other", "strings"]
		expected_dict["c4"] = [100, numpy.nan, 32]
		expected_dict["c5"] = [1.1, 1.7853, numpy.nan]
		expected_df = pandas.DataFrame.from_dict(expected_dict)

		test_dict = {}
		test_dict["c1"] = case1
		test_dict["c2"] = case2
		test_dict["c3"] = case3
		test_dict["c4"] = case4
		test_dict["c5"] = case5
		test_df = pandas.DataFrame.from_dict(test_dict)		

		replaced_df = parse_gctoox.replace_666(test_df, True)	

		self.assertTrue(replaced_df.equals(expected_df),
			"Replacing -666 values didn't work as expected from {} to {}".format(replaced_df, expected_df))

		# case 2: convert_neg_666 = False
		not_actually_replaced_df = parse_gctoox.replace_666(test_df, False)
		str_666s_test_df = test_df.replace([-666,-666.0],["-666","-666"]) # if not replaced, -666 repr as strings
		assert_frame_equal(str_666s_test_df, not_actually_replaced_df)

if __name__ == "__main__":
	setup_logger.setup(verbose = True)
	unittest.main()
