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

def parse(gctx_file_path, convert_neg_666=True, rid=None, cid=None, 
		ridx=None, cidx=None, meta_only=False): 
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
		- meta_only (bool): Whether to load data + metadata (if True), or just row/column metadata (if false)

	Output:
		- myGCToo (GCToo): A GCToo instance containing content of parsed gctx file. Note: if meta_only = True,
			this will be a GCToo instance where the data_df is empty, i.e. data_df = pd.DataFrame(index=rids, 
			columns = cids)

	Note: why does convert_neg_666 exist? 
		- In CMap--for somewhat obscure historical reasons--we use "-666" as our null value 
		for metadata. However (so that users can take full advantage of pandas' methods, 
		including those for filtering nan's etc) we provide the option of converting these 
		into numpy.NaN values, the pandas default. 
	"""
	# validate optional input ids 
	(row_id_list, col_id_list) = check_id_inputs(rid, ridx, cid, cidx)

	full_path = os.path.expanduser(gctx_file_path)
	# open file 
	gctx_file = h5py.File(full_path, "r")

	# read in row metadata 
	row_dset = gctx_file[row_meta_group_node]
	row_meta = parse_metadata_df("row", row_dset, convert_neg_666)

	# read in col metadata 
	col_dset = gctx_file[col_meta_group_node]
	col_meta = parse_metadata_df("col", col_dset, convert_neg_666)

	# get idx values to actually use 
	ridx = get_ordered_idx(row_id_list, row_meta)
	cidx = get_ordered_idx(col_id_list, col_meta)

	if meta_only:
		data_df = pd.DataFrame(index = row_meta.index[ridx], columns = col_meta.index[cidx])
	else:
		data_dset = gctx_file[data_node]
		data_df = parse_data_df(data_dset, ridx, cidx, row_meta, col_meta)

	# (if slicing) slice metadata 
	row_meta = row_meta.iloc[ridx]
	col_meta = col_meta.iloc[cidx]

	# get version
	my_version = gctx_file.attrs[version_node]
	if type(my_version) == np.ndarray:
		my_version = my_version[0]

	gctx_file.close()

	# make GCToo instance 
	my_gctoo = GCToo.GCToo(data_df=data_df, row_metadata_df = row_meta, col_metadata_df = col_meta,
		src = full_path, version=my_version)
	return my_gctoo


def check_id_inputs(rid, ridx, cid, cidx):
	row_list = check_id_idx_exclusivity(rid, ridx, "row")
	col_list = check_id_idx_exclusivity(cid, cidx, "col")

	if (len(row_list) != 0 and len(col_list) != 0 and type(row_list[0]) != type(col_list[0])):
		msg = ("Please specify ids to subset in a consistent manner: " + 
			"rids are specified by {} while cids are specified" + 
			"by {}".format(row_id_type, col_id_type))
		self.logger.error(msg)
		raise Exception("parse_gctoox.id_inputs_ok: " + msg)
	else:
		return (row_list, col_list) 

def check_id_idx_exclusivity(id, idx, dim):
	if (id != None and idx != None):
		msg = ("Parsing in {} dimension: 'id' and 'idx' fields can't both not be None," +
			" please specify slice in only one of these fields")
		self.logger.error(msg)
		raise Exception("parse_gctoox.check_id_idx_exclusivity: " + msg)
	elif id != None:
		return id 
	elif idx != None:
		return idx 
	else: 
		return [] 
	
def parse_metadata_df(dim, meta_group, convert_neg_666):
	"""
	Reads in all metadata from .gctx file to pandas DataFrame. 

	Input:
		- dim (str): Dimension of metadata; either "row" or "column"
		- meta_group (HDF5 group): Group from which to read metadata values 

	Output:
		- meta_df (pandas DataFrame): data frame corresponding to metadata fields 
			of dimension specified.
	"""
	# read values from hdf5 & make a DataFrame
	meta_values = {}
	for k in meta_group.keys():
		with meta_group[k].astype("S50"):
			curr_meta = list(meta_group[k])
			meta_values[k] = [str(elem).strip() for elem in curr_meta]
	meta_df = pd.DataFrame.from_dict(meta_values)
	meta_df.set_index("id", inplace = True)

	# set index and columns appropriately 
	meta_df = set_metadata_index_and_column_names(dim, meta_df)

	# if specified, convert "-666" to np.nan
	if convert_neg_666:
		meta_df = meta_df.replace([-666, "-666", -666.0], [np.nan, np.nan, np.nan])
	else:
		meta_df = meta_df.replace([-666, -666.0], ["-666", "-666"])


	return meta_df 

def set_metadata_index_and_column_names(dim, meta_df):	
	"""
	Sets index and column names to GCTX convention.

	Input:
		- dim (str): Dimension of metadata to read. Must be either "row" or "col"
		- meta_df (pandas.DataFrame): data frame corresponding to metadata fields 
			of dimension specified.

	Output:
		None 
	"""
	if dim == "row":
		meta_df.index.name = "rid"
		meta_df.columns.name = "rhd"
	elif dim == "col":
		meta_df.index.name = "cid"
		meta_df.columns.name = "chd"

def get_ordered_idx(id_list, meta_df):
	if len(id_list) == 0:
		id_list = range(0, len(list(meta_df.index)))
	elif type(id_list[0]) == str:
		id_list = [list(meta_df.index).index(i) for i in id_list]
	return sorted(id_list)

def parse_data_df(data_dset, ridx, cidx, row_meta, col_meta):
	"""
	TODO 

	Note. KNOWN ISSUE: (to my knowledge), h5py slicing reads to dtype np.float64, not np.float32
	"""
	if len(ridx) == len(row_meta.index) and len(cidx) == len(col_meta.index): # no slice
		data_array = np.empty(data_dset.shape, dtype = np.float32) 
		data_dset.read_direct(data_array)
	elif len(ridx) <= len(cidx):
		first_slice = data_dset[:, ridx]
		data_array = first_slice[cidx, :].transpose()
	elif len(cidx) < len(ridx):
		first_slice = data_dset[cidx, :]
		data_array = first_slice[:, ridx].transpose()
	# make DataFrame instance
	data_df = pandas.DataFrame(data_array.astype(np.float32), index = row_meta.index[ridx], columns = col_meta.index[cidx])
	return data_df 

def get_column_metadata(gctx_file_path, convert_neg_666=True):
	full_path = os.path.expanduser(gctx_file_path)
	# open file 
	gctx_file = h5py.File(full_path, "r")
	col_dset = gctx_file[col_meta_group_node]
	col_meta = parse_metadata_df("col", col_dset, convert_neg_666)
	gctx_file.close()
	return col_meta

def get_row_metadata(gctx_file_path, convert_neg_666=True):
	full_path = os.path.expanduser(gctx_file_path)
	# open file 
	gctx_file = h5py.File(full_path, "r")
	row_dset = gctx_file[row_meta_group_node]
	row_meta = parse_metadata_df("row", row_dset, convert_neg_666)
	gctx_file.close()
	return row_meta