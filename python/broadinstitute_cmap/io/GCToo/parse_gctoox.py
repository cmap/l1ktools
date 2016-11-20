"""
Reads in a .gctx file as a gctoo object. 

The main method is parse; subsequent methods are helpers that read/format different 
components of the underlying HDF5 file.

Note. Only supports v1.0 gctx files. 

Structure of a complete (minimal) GCTX:

	/version -> required. Version of the file; currently we only support 1.0
	/src -> required. Input source of file 
	/0
		/DATA 
			/0
				/matrix -> required. Data matrix of expression values. 
		/META 
			/ROW 
				/id -> required. Unique row identifiers. 
				/... -> optional. Other row metadata fields 
			/COL 
				/id -> required. Unique col identifiers. 
				/... -> optional. Other col metadata fields 			

"""
import logging
import setup_GCToo_logger as setup_logger
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
setup_logger.setup(verbose = False)

version_node = "version"
rid_node = "/0/META/ROW/id"
cid_node = "/0/META/COL/id"
data_node = "/0/DATA/0/matrix"
row_meta_group_node = "/0/META/ROW"
col_meta_group_node = "/0/META/COL"

def parse(gctx_file_path, convert_neg_666=True, rid=None, cid=None, meta_only=None): 
	"""
	Primary method of script. Reads in path to a gctx file and parses into GCToo object.

	Input:
		Mandatory:
		- gctx_file_path (str): full path to gctx file you want to parse. 
		
		Optional:
		- convert_neg_666 (bool): whether to convert -666 values to numpy.nan or not 
			(see Note below for more details on this). Default = False.
		- rid (list of strings): only read the row ids in this list from the gctx. Default=None. 
		- cid (list of strings): only read the column ids in this list from the gctx. Default=None. 
		- meta_only (str, must be "row" or "col"): dimension of metadata to return only. 

	Output:
		- myGCToo (GCToo): A GCToo instance containing content of parsed gctx file 

	Note: why does convert_neg_666 exist? 
		- In CMap--for somewhat obscure historical reasons--we use "-666" as our null value 
		for metadata. However (so that users can take full advantage of pandas' methods, 
		including those for filtering nan's etc) we provide the option of converting these 
		into numpy.NaN values, the pandas default. 
	"""
	full_path = os.path.expanduser(gctx_file_path)
	# open file 
	gctx_file = h5py.File(full_path, "r")

	# get version
	my_version = gctx_file.attrs[version_node]
	if type(my_version) == np.ndarray:
		my_version = my_version[0]

	# get dsets corresponding to rids, cids 
	rid_dset = gctx_file[rid_node]
	cid_dset = gctx_file[cid_node]
	# store commonly accessed id-related information in dict for O(1) lookup
	id_info = make_id_info_dict(rid_dset, cid_dset, rid, cid)

	if meta_only == None:
		# Reads data (or specified slice thereof) from input GCTX to pandas DataFrame
		data_dset = gctx_file[data_node]
		data_df = parse_data_df(data_dset, id_info)

		# Reads row metadata (or specified slice thereof) from input GCTX to pandas DataFrame
		row_group = gctx_file[row_meta_group_node]
		row_meta_df = make_meta_df("rids", row_group, id_info, convert_neg_666)

		# Reads col metadata (or specified slice thereof) from input GCTX to pandas DataFrame
		col_group = gctx_file[col_meta_group_node]
		col_meta_df = make_meta_df("cids", col_group, id_info, convert_neg_666)

		# close file 
		gctx_file.close()

		# make GCToo instance 
		curr_GCToo = GCToo.GCToo(src=full_path, version=my_version,
			row_metadata_df=row_meta_df, col_metadata_df=col_meta_df, data_df=data_df)

		return curr_GCToo
	elif meta_only == "row":
		# Reads row metadata (or specified slice thereof) from input GCTX to pandas DataFrame
		row_group = gctx_file[row_meta_group_node]
		row_meta_df = make_meta_df("rids", row_group, id_info, convert_neg_666)

		# close file 
		gctx_file.close()

		return row_meta_df

	elif meta_only == "col":
		# Reads col metadata (or specified slice thereof) from input GCTX to pandas DataFrame
		col_group = gctx_file[col_meta_group_node]
		col_meta_df = make_meta_df("cids", col_group, id_info, convert_neg_666)

		# close file 
		gctx_file.close()

		return col_meta_df	
	else: 
		error_msg = "meta_only argument must be either 'row' or 'col'"
		logger.error(error_msg)	
		raise(Exception(error_msg))

def make_id_info_dict(rid_dset, cid_dset, rid, cid):
	"""
	Generactes a dict to commonly accessed id-related information for input file.

	Input:
		- rid_dset (HDF5 Dataset): Array of rid values. 
		- cid_dset (HDF5 Dataset): Array of cid values. 
		- rid (list): Either a number of rids to subset from full list, or None. 
		- cid (list): Either a number of cids to subset from full list, or None. 

	Output: 
		- id_dict (dict)
	"""
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
	Helper to make_id_info_dict(). 
	Populates and properly formats rids and cids from input file.

	Input:
		- rid_dset (HDF5 Dataset): Array of rid values. 
		- cid_dset (HDF5 Dataset): Array of cid values. 
		- id_dict (dict): Dictionary of commonly accessed id-related information for input file.

	Output:
		None
	"""
	# make empty numpy arrays of proper dims to populate
	# Note: ids are currently truncated at > 50 chars
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
	Helper to make_id_info_dict(). 
	Determines numerical indexes and orders them appropriately for compatibility
	with fancy indexing requirements for h5py v.2.5.

	(Oct. 2016) See: http://docs.h5py.org/en/latest/high/dataset.html#fancy-indexing

	Input:
		- dim_id_key (str, either "rids" or "cids"): dimension on which to perform operations. 
		- id_dict (dict): Dictionary of commonly accessed id-related information for input file.
		- slice_id_list (list): list of id values to slice out  

	Output:
		None 
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
	Reads data (or specified slice thereof) from input GCTX to pandas DataFrame. 

	Input:
		- data_dset (HDF5 Dataset): Dataset of matrix of expression values 
		- id_dict (dict): Dictionary of commonly accessed id-related information for input file.

	Output:
		- data_df (pandas DataFrame): data frame of expression values 

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

	data_df = pd.DataFrame(data_array.transpose(), index = rids, columns = cids)
	data_df.index.name = "rid"
	data_df.columns.name = "cid"

	return data_df

def set_slice_order(id_dict):
	"""
	Helper to parse_data_df(). 
	For h5py v.2.5, we can only slice on one dimension at a time; so, the larger dimension is 
	sliced first. 

	TODO (oana): This is not necessarily optimal; fix to instead slice by largest area 

	Input: 
		- id_dict (dict): Dictionary of commonly accessed id-related information for input file.

	Output:
		- slice_both_dims (bool): whether to slice both dims or not 
		- first_slice_dim (str): first dimension to slice on 

	"""
	# (for current h5py version) can only take hyperslab on one dimension
	# ... so we choose the larger one 
	slice_both_dims = False 
	first_slice_dim = None 

	if len(id_dict["slice_lengths"]) == 2: 
		first_slice_dim = max(id_dict['slice_lengths'].iterkeys(), 
			key=(lambda key: id_dict["slice_lengths"][key]))
		slice_both_dims = True
	elif len(id_dict["slice_lengths"]) == 1:
		first_slice_dim = id_dict["slice_lengths"].keys()[0]

	return slice_both_dims, first_slice_dim

def make_meta_df(dim_id_key, dset, id_dict, convert_neg_666):
	"""
	Reads metadata (or specified slice thereof) from input GCTX to pandas DataFrame

	Input:
		- dim_id_key (str; either "rids" or "cids"): dimension of ids to get metadata for 
		- dset (HDF5 Group): Group from which to parse metadata values 
		- id_dict (dict): Dictionary of commonly accessed id-related information for input file.
		- convert_neg_666 (bool): Whether to convert -666 values to numpy.Nan

	Output:
		- meta_df (pandas DataFrame)
	"""
	(row_indexes, df_index_vals) = set_meta_index_to_use(dim_id_key, id_dict)

	if len(dset.keys()) > 1: # aka if there is metadata besides ids
		meta_df = populate_meta_array(dset, row_indexes)

		# column values shouldn't include "id"
		df_column_vals = list(dset.keys()[:])
		df_column_vals.remove("id")

		meta_df.index = df_index_vals
		meta_df.columns = df_column_vals
	else:
		meta_df = pd.DataFrame(index= df_index_vals)

	meta_df = set_metadata_index_and_column_names(dim_id_key, meta_df)

	# Convert metadata to numeric if possible, after converting everything to string first 
	# Note: This conversion first to string is to ensure consistent behavior between
	#	the gctx and gct parser (which by default reads the entire text file into a string)
	meta_df = meta_df.apply(lambda x: pd.to_numeric(x, errors="ignore"))

	if convert_neg_666:
		meta_df = meta_df.replace([-666, "-666", -666.0], [np.nan, np.nan, np.nan])
	else:
		meta_df = meta_df.replace([-666, -666.0], ["-666", "-666"])
	
	return meta_df

def set_meta_index_to_use(dim_id_key, id_dict):
	"""
	Helper to make_meta_df. 
	Sets indexes/columns of metadata df appropriately.

	Input:
		- dim_id_key (str; either "rids" or "cids"): dimension of ids to get metadata for
		- id_dict (dict): Dictionary of commonly accessed id-related information for input file.

	Output:
		- row_indexes (list): numerical indexes to keep for metadata df 
		- df_index_vals (list): values of index to keep for metadata df 
	"""
	if len(id_dict[dim_id_key]) == 3: # yes slice
		row_indexes = id_dict[dim_id_key]["slice_indexes"]
		df_index_vals = id_dict[dim_id_key]["slice_values"]
	else: # no slice
		row_indexes = range(0,len(id_dict[dim_id_key]["full_id_list"].columns))
		df_index_vals = list(id_dict[dim_id_key]["full_id_list"].columns)
	return row_indexes, df_index_vals	

def populate_meta_array(meta_group, row_indexes):
	"""
	Helper to make_meta_df().

	Input:
		- meta_group (HDF5 group): Group from which to read metadata values 
		- row_indexes (list): indexes of metadata fields to keep 

	Output:
		- meta_value_df (pandas DataFrame)
	"""
	meta_values = {}
	for k in meta_group.keys():
		if k != "id":
			with meta_group[k].astype("S50"):
				curr_meta = list(meta_group[k][row_indexes])
				meta_values[k] = [str(elem).strip() for elem in curr_meta]
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



