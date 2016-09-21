import logging
import setup_GCToo_logger as setup_logger
import os 
import numpy as np 
import pandas as pd 
import h5py

__author__ = "Oana Enache"
__email__ = "oana@broadinstitute.org"

# instantiate logger
logger = logging.getLogger(setup_logger.LOGGER_NAME)
# when not in debug mode, probably best to set verbose=False
setup_logger.setup(verbose = True)

version_node = "version"
rid_node = "/0/META/ROW/id"
cid_node = "/0/META/COL/id"
data_node = "/0/DATA/0/matrix"

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
	gctx_file = h5py.File(full_path)

	# get version
	# Note: this should be "GCTX1.0"
	my_version = h5py.attrs[version_node][0]

	# get dsets corresponding to rids, cids 
	rid_dset = gctx_file[rid_node]
	cid_dset = gctx_file[cid_node]
	# store appropriate id information in dict for O(1) lookup
	# Note: if slicing in 
	id_info = set_id_info(rid_dset, cid_dset, rid, cid)

	# get data dset
	data_dset = gctx_file[data_node]
	# write data (or specified slice thereof) to pandas DataFrame
	data_df = parse_data_df(data_dset, id_info)

	# get row metadata

	# get col metadata

	# close file 

	# make GCToo instance 

def make_id_info_dict(rid_dset, cid_dset, rid, cid):
	# for looking up id information 
	# (full & subsets, as applicable)
	id_info = {}

	# read in full id values 
	get_all_id_values(rid_dset, cid_dset, id_info)

	# to store slice lengths (if applicable)
	# Note: Currently h5py throws an error if you try hyperslab selection along
	#  		more than one dimension at once, so we hyperslab first by the longer slice length
	#		(in cases where both dimensions are sliced)
	id_info["slice_lengths"] = {}

	# set subsetted id values to whatever's applicable
	# if taking slices w/hyperslabs, need to obtain an ordered list of indexes
	if rid != None:
		get_ordered_slice_indexes("rids", id_info, rid)
		id_info["slice_lengths"]["rids"] = len(rid)
	if cid != None:
		get_ordered_slice_indexes("cids", id_info, cid)
		id_info["slice_lengths"]["cids"] = len(cid)


def get_all_id_values(rid_dset, cid_dset, id_dict):
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
	id_info["rids"]["full_id_list"] = rid_df
	id_info["cids"]["full_id_list"] = cid_df

def get_ordered_slice_indexes(dim_id_key, id_dict, slice_id_list):
	"""
	TODO: Explain why this is necessary for hyperslab slicing w/h5py
	"""
	# get unsorted index values of slice_id_list 
	unordered_slice_df = id_dict[dim_id_key][slice_id_list]

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

	"""
	(slice_both, first_dim) = determine_slice_order(id_dict) 
	if slice_both: # slice both columns 
		if first_dim == "row":
			first_slice = data_dset[id_dict["rids"]["slice_indexes"],:]
			data_array = first_slice[:, id_dict["cids"]["slice_indexes"]]
		else:
			first_slice = data_dset[:, id_dict["cids"]["slice_indexes"]]
			data_array = first_slice[id_dict["rids"]["slice_indexes"],:]
	elif first_dim != None: # slice one column 
		if first_dim == "row":
			data_array = data_dset[id_dict["rids"]["slice_indexes"],:]
		elif first_dim == "col":
			data_array = first_slice[:, id_dict["cids"]["slice_indexes"]]
	else: # slice no columns 
		# empty numpy array to populate w/data dset values
		data_array = np.empty() 

	return data_df


def determine_slice_order(id_dict):
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



