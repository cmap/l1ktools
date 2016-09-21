import logging
import setup_GCToo_logger as setup_logger
# import sys
import os 
import numpy as np 
import pandas as pd 
import h5py
import GCToo

__author__ = "Oana Enache"
__email__ = "oana@broadinstitute.org"

#instantiate logger
logger = logging.getLogger(setup_logger.LOGGER_NAME)
# when not in debug mode, probably best to set verbose=False
setup_logger.setup(verbose = True)

version_node = "version"
rid_node = "/0/META/ROW/id"
cid_node = "/0/META/COL/id"
data_node = "/0/DATA/0/matrix"
row_meta_group_node = "/0/META/ROW"
col_meta_group_node = "/0/META/COL"

def parse(gctx_file_path, convert_neg_666=True, rid=None, cid=None): 
	"""
	Primary method of script. Reads in path to a gctx file and parses into GCToo object.

	Input:
		Mandatory:
		- gctx_file_path (str): full path to gctx file you want to parse. 
		
		Optional:
		- convert_neg_666 (bool): whether to convert -666 values to numpy.nan or not 
			(see Note below for more details on this). Default = False.
		- rid (list of strings): list of row ids to specifically keep from gctx. Default=None. 
		- cid (list of strings): list of col ids to specifically keep from gctx. Default=None. 

	Output:
		- myGCToo 

	Note: why is convert_neg_666 even a thing? 
		- In CMap--for somewhat obscure historical reasons--we use "-666" as our null value 
		for metadata. However (so that users can take full advantage of pandas' methods, 
		including those for filtering nan's etc) we provide the option of converting these 
		into numpy.NaN values, the pandas default. 
	"""
	full_path = os.path.expanduser(gctx_file_path)
	# open file 
	gctx_file = h5py.File(full_path, "r", driver = "core")

	# get version
	# Note: this should be "GCTX1.0"
	my_version = gctx_file.attrs[version_node][0]

	# get dsets corresponding to rids, cids 
	rid_dset = gctx_file[rid_node]
	cid_dset = gctx_file[cid_node]
	# store appropriate id information in dict for O(1) lookup
	# Note: if slicing in 
	id_info = make_id_info_dict(rid_dset, cid_dset, rid, cid)

	# get data dset
	data_dset = gctx_file[data_node]
	# write data (or specified slice thereof) to pandas DataFrame
	data_df = parse_data_df(data_dset, id_info)

	# get row metadata
	row_group = gctx_file[row_meta_group_node]
	row_meta_df = make_meta_df("rids", row_group, id_info, convert_neg_666)
	logger.debug("row_meta_df index: {}".format(row_meta_df.index))
	logger.debug("row_meta_df columns: {}".format(row_meta_df.columns))

	# get col metadata
	col_group = gctx_file[col_meta_group_node]
	col_meta_df = make_meta_df("cids", col_group, id_info, convert_neg_666)
	logger.debug("col_meta_df index: {}".format(col_meta_df.index))
	logger.debug("col_meta_df columns: {}".format(col_meta_df.columns))

	# close file 
	gctx_file.close()

	# make GCToo instance 
	curr_GCToo = GCToo.GCToo(src=full_path, version=my_version,
		row_metadata_df=row_meta_df, col_metadata_df=col_meta_df, data_df=data_df)

	return curr_GCToo

def make_id_info_dict(rid_dset, cid_dset, rid, cid):
	"""
	TODO
	"""
	# for looking up id information 
	# (full & subsets, as applicable)
	id_dict = {}
	# read in full id values 
	get_all_id_values(rid_dset, cid_dset, id_dict)
	# to store slice lengths (if applicable)
	# Note: Currently h5py throws an error if you try hyperslab selection along
	#  		more than one dimension at once, so we hyperslab first by the longer slice length
	#		(in cases where both dimensions are sliced)
	id_dict["slice_lengths"] = {}
	# set subsetted id values to whatever's applicable
	# if taking slices w/hyperslabs, need to obtain an ordered list of indexes
	if rid != None:
		get_ordered_slice_indexes("rids", id_dict, rid)
		id_dict["slice_lengths"]["rids"] = len(rid)
	if cid != None:
		get_ordered_slice_indexes("cids", id_dict, cid)
		id_dict["slice_lengths"]["cids"] = len(cid)
	return id_dict


def get_all_id_values(rid_dset, cid_dset, id_dict):
	"""
	TODO
	"""
	# make empty numpy arrays of proper dims to populate
	# Note: ids are currently truncated at > 50 chars
	# TODO: Q: What's the limit for id lengths? 
	rid_array = np.empty(rid_dset.shape, dtype = "S50")
	cid_array = np.empty(cid_dset.shape, dtype = "S50")
	# set numpy arrays to respective id values 
	rid_dset.read_direct(rid_array)
	cid_dset.read_direct(cid_array)
	# convert to pandas DataFrames
	# this structure enables us to easily get a list of slice value indexes
	rid_df = pd.DataFrame(range(0, rid_array.shape[0]), index = rid_array).transpose()
	cid_df = pd.DataFrame(range(0, cid_array.shape[0]), index = cid_array).transpose()
	# add full values to id info dict
	id_dict["rids"] = {}
	id_dict["rids"]["full_id_list"] = rid_df
	id_dict["cids"] = {}
	id_dict["cids"]["full_id_list"] = cid_df

def get_ordered_slice_indexes(dim_id_key, id_dict, slice_id_list):
	"""
	TODO: Explain why this is necessary for hyperslab slicing w/h5py
	"""
	# get unsorted index values of slice_id_list 
	unordered_slice_df = id_dict[dim_id_key]["full_id_list"].loc[:, slice_id_list]
	# gets indexes that would properly sort slice_id_list values, 
	# and orders elements of slice_id_list accordingly
	ordered_columns = unordered_slice_df.columns[unordered_slice_df.ix[unordered_slice_df.last_valid_index()].argsort()]	
	# order index values corresponding to slice_id_list in ascending numerical order
	ordered_slice_df = unordered_slice_df[ordered_columns]
	# set 
	id_dict[dim_id_key]["slice_indexes"] = ordered_slice_df[ordered_columns].values.tolist()[0]
	id_dict[dim_id_key]["slice_values"] = list(ordered_columns)


def parse_data_df(data_dset, id_dict):
	"""
	TODO

	NOTE: Data_DF is stored transposed from final parsed form;
		so, the indexing of slicing is the opposite of what would normally
		be expected. 
	"""
	# for setting proper index/columns of data df 
	rids = id_dict["rids"]["full_id_list"].columns
	cids = id_dict["cids"]["full_id_list"].columns 
	# if applicable, determine how to slice hdf5 file
	(slice_both, first_dim) = set_slice_order(id_dict) 
	if slice_both: # slice both columns 
		rids = id_dict["rids"]["slice_values"]
		cids = id_dict["cids"]["slice_values"]
		if first_dim == "rids":
			first_slice = data_dset[:, id_dict["rids"]["slice_indexes"]]
			data_array = first_slice[id_dict["cids"]["slice_indexes"],:]
		else:
			first_slice = data_dset[id_dict["cids"]["slice_indexes"],:]
			data_array = first_slice[:, id_dict["rids"]["slice_indexes"]]
	elif first_dim != None: # slice one column 
		if first_dim == "rids":
			data_array = data_dset[:, id_dict["rids"]["slice_indexes"]]
			rids = id_dict["rids"]["slice_values"]
		elif first_dim == "cids":
			data_array = data_dset[(id_dict["cids"]["slice_indexes"]),:]
			cids = id_dict["cids"]["slice_values"]
	else: # slice no columns 
		# empty numpy array to populate w/data dset values
		data_array = np.empty(data_dset.shape, dtype = np.float64) 
		data_dset.read_direct(data_array)

	logger.debug("rids: {}".format(rids))
	logger.debug("cids: {}".format(cids))
	data_df = pd.DataFrame(data_array.transpose(), index = rids, columns = cids)
	logger.debug("parsed data_df: {}".format(data_df))
	return data_df

def set_slice_order(id_dict):
	"""
	TODO
	"""
	# (for current h5py version) can only take hyperslab on one dimension
	# ... so we choose the larger one 
	slice_both_dims = False 
	first_slice_dim = None 
	if len(id_dict["slice_lengths"]) == 2: 
		first_slice_dim = max(id_dict['slice_lengths'].iterkeys(), key=(lambda key: id_dict["slice_lengths"][key]))
		slice_both_dims = True
	elif len(id_dict["slice_lengths"]) == 1:
		first_slice_dim = id_dict["slice_lengths"].keys()[0]
	return slice_both_dims, first_slice_dim

def make_meta_df(dim_id_key, dset, id_dict, convert_neg_666):
	"""
	"""
	(row_indexes, df_index_vals) = set_meta_index_to_use(dim_id_key, id_dict)
	logger.debug("row_indexes: {}".format(row_indexes))
	logger.debug("{} df index vals: {}".format(dim_id_key, df_index_vals))

	if len(dset.keys()) > 1: # aka if there is metadata besides ids
		meta_df = populate_meta_array(dset, row_indexes)

		# column values shouldn't include "id"
		df_column_vals = list(dset.keys()[:])
		df_column_vals.remove("id")
		logger.debug("df columns vals: {}".format(df_column_vals))

		meta_df.index = df_index_vals
		meta_df.columns = df_column_vals
		meta_df = set_metadata_index_and_column_names(dim_id_key, meta_df)
	else:
		meta_df = pd.DataFrame(index= df_index_vals)

	if convert_neg_666:
		meta_df = meta_df.replace([-666, "-666", -666.0], [np.nan, np.nan, np.nan])

	# Convert metadata to numeric if possible, after converting everything to string first 
	# Note: This conversion first to string is to ensure consistent behavior between
	#	the gctx and gct parser (which by default reads the entire text file into a string)
	meta_df = meta_df.astype(str)
	meta_df = meta_df.apply(lambda x: pd.to_numeric(x, errors="ignore"))
	
	return meta_df

def set_meta_index_to_use(dim_id_key, id_dict):
	if len(id_dict[dim_id_key]) == 3: # yes slice
		row_indexes = id_dict[dim_id_key]["slice_indexes"]
		df_index_vals = id_dict[dim_id_key]["slice_values"]
	else: # no slice
		row_indexes = range(0,len(id_dict[dim_id_key]["full_id_list"].columns))
		df_index_vals = list(id_dict[dim_id_key]["full_id_list"].columns)
	return row_indexes, df_index_vals	

def populate_meta_array(meta_group, row_indexes):
	meta_values = {}
	for k in meta_group.keys():
		if k != "id":
			with meta_group[k].astype("S50"):
				curr_meta = list(meta_group[k][row_indexes])
				# logger.debug("curr_meta values: {}".format(curr_meta))
				# logger.debug("curr_meta as list: {}".format(list(curr_meta)))
				meta_values[k] = curr_meta
	# logger.debug("meta values before concat: {}".format(meta_values))
	meta_value_df = pd.DataFrame.from_dict(meta_values)
	return meta_value_df


def set_metadata_index_and_column_names(dim_id_key, meta_df):	
	"""
	Sets index and column names to GCTX convention.

	Input:
		- dim (str): Dimension of metadata to read. Must be either "row" or "col"
		- meta_df (pandas.DataFrame): data frame corresponding to metadata fields of dimension specified.

	Output:
		- meta_df (pandas.DataFrame): data frame corresponding to metadata fields of dimension specified.
	"""
	if dim_id_key == "rids":
		meta_df.index.name = "rid"
		meta_df.columns.name = "rhd"
	elif dim_id_key == "cids":
		meta_df.index.name = "cid"
		meta_df.columns.name = "chd"
	return meta_df

