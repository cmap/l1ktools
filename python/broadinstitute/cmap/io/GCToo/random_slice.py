import logging
import setup_GCToo_logger as setup_logger
import argparse
import sys
import pandas
import GCToo
import uuid

logger = logging.getLogger(setup_logger.LOGGER_NAME)

def make_specified_size_gctoo(og_gctoo, num_samples, dim):
	"""
	Subsets a GCToo instance along either rows or columns to obtain a specified size.

	Input:
		- og_gctoo (GCToo): a GCToo instance 
		- num_samples (int): the number of samples to keep
		- dim (str): the dimension along which to subset. Must be "row" or "col"

	Output:
		- new_gctoo (GCToo): the GCToo instance subsetted as specified. 
	"""	
	assert dim in ["row", "col"], "dim specified must be either 'row' or 'col'"

	if dim == "col":
		assert num_samples < og_gctoo.data_df.shape[1], "number of samples must be subset of original file sample size"
		# generate a consistent set of rids/cids
		new_cids = generate_specified_length_unique_ids(num_samples)
		random_cols = random.sample(range(0, og_gctoo.data_df.shape[1]), num_samples)
		new_rids = generate_specified_length_unique_ids(og_gctoo.data_df.shape[0])
		# subset relevant data frames 
		new_data_df = og_gctoo.data_df.iloc[:,random_cols]
		new_row_meta = og_gctoo.row_metadata_df
		new_col_meta = og_gctoo.col_metadata_df.iloc[random_cols,:]
		logger.debug("New col_meta shape after slice: {}".format(new_col_meta.shape))
	elif dim == "row":
		assert num_samples < og_gctoo.data_df.shape[0], "number of samples must be subset of original file sample size"
		# generate a consistent set of rids/cids
		new_rids = generate_specified_length_unique_ids(num_samples)
		random_rows = random.sample(range(0, og_gctoo.data_df.shape[0]), num_samples)
		new_cids = generate_specified_length_unique_ids(og_gctoo.data_df.shape[1])
		logger.debug("New cids: {}".format(new_cids))
		# subset relevant data frames 
		new_data_df = og_gctoo.data_df.iloc[random_rows,:]
		new_row_meta = og_gctoo.row_metadata_df.iloc[random_rows,:]
		new_col_meta = og_gctoo.col_metadata_df

	# relabel rids/cids to avoid consistency_check error 
	new_data_df.index = new_rids
	new_data_df.columns = new_cids
	new_row_meta.index = new_rids
	new_col_meta.index = new_cids
	# make & return new gctoo instance
	new_gctoo = GCToo.GCToo(data_df=new_data_df, row_metadata_df=new_row_meta, col_metadata_df=new_col_meta)
	return new_gctoo

def generate_specified_length_unique_ids(id_length):
	"""
	Generates a list of unique ids of specified length. 

	Input:
		- id_length (int): length of id list to generate

	Output:
		- a list of unique ids 
	"""
	s = set()
	for i in range(id_length):
		# Note: uuid4 generates a random UUID (Universally Unique IDentifier)
		#	There is a *very minor* chance of collisions b/c of random generation, but very low likelihood 
		s.add(str(uuid.uuid4())) 
	return list(s)

# def main(args):
# 	# TODO

# if __name__ == "__main__":
# 	args = build_parser().parse_args(sys.argv[1:])

# 	setup_logger.setup(verbose=args.verbose)

# 	logger.debug("args:  {}".format(args))

# 	main(args)

