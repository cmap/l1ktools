import unittest
import logging
import setup_GCToo_logger as setup_logger
import numerical_slice
import mini_gctoo_for_testing


logger = logging.getLogger(setup_logger.LOGGER_NAME)

class TestRandomSlice(unittest.TestCase):
	def test_make_specified_size_gctoo(self):
		mini_gctoo = mini_gctoo_for_testing.make()
		logger.debug("mini gctoo data_df shape: {}".format(mini_gctoo.data_df.shape))
		logger.debug("mini gctoo row_meta shape: {}".format(mini_gctoo.row_metadata_df.shape))
		logger.debug("mini gctoo col_meta shape: {}".format(mini_gctoo.col_metadata_df.shape))

		# case 1: dim isn't 'row' or 'col'
		with self.assertRaises(AssertionError) as context:
			numerical_slice.make_specified_size_gctoo(mini_gctoo, 3, "aaaalll")
		self.assertEqual(str(context.exception), "dim specified must be either 'row' or 'col'")

		# case 2: row subsetting - happy
		row_subset = numerical_slice.make_specified_size_gctoo(mini_gctoo, 3, "row")
		self.assertEqual(row_subset.data_df.shape, (3,6), 
			"data_df after row slice is incorrect shape: {} vs (3,6)".format(row_subset.data_df.shape))
		self.assertEqual(row_subset.row_metadata_df.shape, (3,5), 
			"row_metadata_df after row slice is incorrect shape: {} vs (3,5)".format(row_subset.row_metadata_df.shape))
		self.assertEqual(row_subset.col_metadata_df.shape, (6,5),
			"col_metadata_df after row slice is incorrect shape: {} vs (6,5)".format(row_subset.col_metadata_df.shape))

		# case 3: row subsetting - sample subset > og # of samples
		with self.assertRaises(AssertionError) as context:
			numerical_slice.make_specified_size_gctoo(mini_gctoo, 30, "row")
		self.assertEqual(str(context.exception), "number of samples must be subset of original file sample size")

		# case 4: col subsetting - happy
		col_subset = numerical_slice.make_specified_size_gctoo(mini_gctoo, 3, "col")
		self.assertEqual(col_subset.data_df.shape, (6,3), 
			"data_df after col slice is incorrect shape: {} vs (6,3)".format(col_subset.data_df.shape))
		self.assertEqual(col_subset.row_metadata_df.shape, (6, 5), 
			"row_metadata_df after col slice is incorrect shape: {} vs (6, 5)".format(col_subset.row_metadata_df.shape))
		self.assertEqual(col_subset.col_metadata_df.shape, (3,5),
			"col_metadata_df after col slice is incorrect shape: {} vs (3,5)".format(col_subset.col_metadata_df.shape))

		# case 5: col subsetting - sample subset > og # of samples
		with self.assertRaises(AssertionError) as context:
			numerical_slice.make_specified_size_gctoo(mini_gctoo, 7, "col")
		self.assertEqual(str(context.exception), "number of samples must be subset of original file sample size")

	def test_generate_specified_length_unique_ids(self):
		sample_list = numerical_slice.generate_specified_length_unique_ids(10)
		self.assertEqual(len(sample_list), 10, "Length of unique ids generated is incorrect")
		self.assertEqual(len(set(sample_list)), 10, "Ids generated are not unique!")

if __name__ == "__main__":
	
	setup_logger.setup(verbose=True)

	unittest.main()