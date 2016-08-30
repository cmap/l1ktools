import logging
import setup_GCToo_logger as setup_logger
import sys
import os
import tables
import numpy
import pandas
import GCToo
import GCTXAttrInfo

__author__ = "Oana Enache"
__email__ = "oana@broadinstitute.org"

# instantiate logger
logger = logging.getLogger(setup_logger.LOGGER_NAME)
# when not in debug mode, probably best to set verbose=False
setup_logger.setup(verbose = True)


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
	# open gctx file from specified path
	logger.info("Opening gctx file from specified path...")
	gctx_file = open_gctx_file(gctx_file_path)

	# instantiate gctx node attribute object
	node_info = GCTXAttrInfo.GCTXAttrInfo()

	# Get gctx version
	logger.info("Getting gctx version...")
	my_version = gctx_file.getNodeAttr(node_info.version_prefix, node_info.version_suffix)
	logger.debug("GCTX Version: {}".format(my_version))

	# convert data matrix to pandas dataframe 
	logger.info("Extracting data values from gctx... ")
	data = parse_data_df(gctx_file, node_info, rid, cid)

	# convert row meta data to pandas dataframe 
	logger.info("Extracting row metadata from gctx... ")
	row_metadata = parse_metadata("row", node_info, gctx_file, rid, cid, convert_neg_666)

	# convert col meta data to pandas dataframe 
	logger.info("Extracting col metadata from gctx... ")
	col_metadata = parse_metadata("col", node_info, gctx_file, rid, cid, convert_neg_666)

	# close gctx file
	gctx_file.close()

	# instantiate GCToo object and set fields correspondingly
	logger.info("Creating GCToo instance... ")
	my_GCToo = GCToo.GCToo(src=gctx_file_path, version=my_version,
				 row_metadata_df=row_metadata, col_metadata_df=col_metadata, data_df=data)

	return my_GCToo

def open_gctx_file(gctx_file_path):
	"""
	Checks that file path corresponds to an actual HDF5 file and opens it for reading.

	Input:
		- gctx_file_path (str): path to gctx file you wish to parse. 

	Output:
		- gctx_file (tables.File): instance of a PyTables File object to read from.
	"""
	# expand input gctx path to full path
	gctx_file_path = os.path.expanduser(gctx_file_path)
	# check if file exists
	if os.path.isfile(gctx_file_path):
		# check if file is hdf5; if so, try reading it in
		if tables.is_hdf5_file(gctx_file_path):
				# open file 
				logger.info("Opening gctx file...")
				gctx_file = tables.openFile(gctx_file_path)
				return gctx_file
		else:
			raise TypeError("Input is not an hdf5 file - gctx_file_path:  {}".format(gctx_file_path))
	else:
		raise ValueError("file does not exist gctx_file_path:  {}".format(gctx_file_path))

def parse_data_df(open_gctx_file, node_info, rid, cid):
	"""
	Main parsing method for data matrix.

	Input:
		- open_gctx_file (tables.File): File instance from which to read
		- node_info (GCTXAttrInfo): GCTXAttrInfo instance from which to get node prefixes/suffixes.
		- rid (list of strings): list of row ids to specifically keep from gctx. Default=None. 
		- cid (list of strings): list of col ids to specifically keep from gctx. Default=None. 

	Output:
		- data_df (pandas.DataFrame): data frame corresponding to data matrix. 
	"""
	# open data node
	data_node = open_gctx_file.getNode(node_info.data_matrix_prefix, node_info.matrix_suffix)

	# convert node to pandas DataFrame
	data_df = pandas.DataFrame(data_node[:]).transpose()

	# convert data types to string, then convert to numeric
	# Note: This conversion first to string is to ensure consistent behavior between
	#	the gctx and gct parser (which by default reads the entire text file into a string)
	data_df = data_df.astype(str)
	data_df = data_df.apply(lambda x: pandas.to_numeric(x, errors='ignore'))

	# set rids
	rid_info = get_dim_specific_id_info("row", node_info)
	all_rids = pandas.Series(list(open_gctx_file.getNode(rid_info[0], rid_info[1])[:])).str.strip()

	# set cids
	cid_info = get_dim_specific_id_info("col", node_info)
	all_cids = pandas.Series(list(open_gctx_file.getNode(cid_info[0], cid_info[1])[:])).str.strip()

	# set columns 
	data_df.columns = all_cids 
	data_df.columns.name = "cid"

	# set index
	data_df.index = all_rids 
	data_df.index.name = "rid"

	# subset to specified rids/cids 
	data_df = subset_by_ids(data_df, rid, cid)

	return data_df 


def parse_metadata(dim, node_info, open_gctx_file, rid, cid, convert_neg_666):
	"""
	Main parsing method for metadata nodes. 

	Input:
		- dim (str): Dimension of metadata to read. Must be either "row" or "col"
		- node_info (GCTXAttrInfo): GCTXAttrInfo instance from which to get node prefixes/suffixes.
		- open_gctx_file (tables.File): File instance from which to read
		- rid (list of strings): list of row ids to specifically keep from gctx. Default=None. 
		- cid (list of strings): list of col ids to specifically keep from gctx. Default=None. 
		- convert_neg_666 (bool): whether to convert -666 values to numpy.nan or not.

	Output: 
		- meta_df (pandas.DataFrame): data frame corresponding to metadata fields of dimension specified.
	"""
	# open metadata node
	meta_group_info = get_dim_specific_meta_group_info(dim, node_info)
	meta_group = open_gctx_file.getNode(meta_group_info[0], meta_group_info[1])

	# populate dict with metadata header values
	full_node_path = meta_group_info[0] + "/" + meta_group_info[1]
	meta_dict = populate_meta_df(full_node_path, meta_group.__members__, open_gctx_file)

	# convert dict to DataFrame
	meta_df = pandas.DataFrame.from_dict(meta_dict)

	# subset to ids specified
	id_info = get_dim_specific_id_info(dim, node_info)
	id_vals = pandas.Series(list(open_gctx_file.getNode(id_info[0], id_info[1])[:])).str.strip()
	meta_df = set_id_vals(id_vals, meta_df)
	meta_df = set_metadata_index_and_column_names(dim, meta_df)
	# metadata convention is that ids are always index values 
	if dim == "row":
		meta_df = subset_by_ids(meta_df, rid, None)
	elif dim == "col":
		meta_df = subset_by_ids(meta_df, cid, None)

	# Convert metadata to numeric if possible, after converting everything to string first 
	# Note: This conversion first to string is to ensure consistent behavior between
	#	the gctx and gct parser (which by default reads the entire text file into a string)
	meta_df = meta_df.astype(str)
	meta_df = meta_df.apply(lambda x: pandas.to_numeric(x, errors="ignore"))
	logger.debug("First row of metadata after numeric conversion: {}".format(meta_df.iloc[1]))
	logger.debug("Post-numeric conversion dtypes: {}".format(meta_df.dtypes))

	# convert -666s if specified
	meta_df = replace_666(meta_df, convert_neg_666)

	return meta_df

def get_dim_specific_meta_group_info(dim, node_info):
	"""
	Helper method for metadata parser. 
	Returns proper dimension-specific prefix/suffix for node access.

	Input:
		- dim (str): Dimension of metadata to read. Must be either "row" or "col"
		- node_info (GCTXAttrInfo): GCTXAttrInfo instance from which to get node prefixes/suffixes.

	Output:
		tuple containing relevant metadata_prefix and suffix in positions 0 and 1, respectively.
	"""
	if dim == "row":
		return (node_info.metadata_prefix, node_info.row_metadata_suffix)
	elif dim == "col":
		return (node_info.metadata_prefix, node_info.col_metadata_suffix)

def populate_meta_df(full_node_path, group_members, open_gctx_file):
	"""
	Helper method for metadata parser.
	Iterates over metadata nodes by header and populates a dictionary with values.

	Input: 
		- full_node_path (str): path to full (dimension-specific) metadata group
		- group_members (list): list of metadata headers
		- open_gctx_file (tables.File): File instance from which to read

	Output:
		- meta_dict (dict): Dictionary where metadata headers are keys and values are metadata values.
	"""
	meta_dict = {}
	for m in group_members:
		if m != "id":
			logger.debug("Member name: {}".format(m))
			m_values = open_gctx_file.getNode(full_node_path, m)
			m_values = m_values[:][:]
			# if m_values contains strings, need to strip whitespace
			if list(m_values)[0] == str or "numpy.string" in str(type(list(m_values)[0])):
				m_values = [x.strip() for x in m_values]
			# can't have duplicate header values 
			if m.strip() not in meta_dict.keys():
				meta_dict[m.strip()] = m_values
			else:
				logger.error("Can't have duplicated metadata fields: {} detected more than once".format(m.strip()))
	return meta_dict

def get_dim_specific_id_info(dim, node_info):
	"""
	Helper method for metadata parser. 
	Returns proper dimension-specific prefix/suffix for rid/cid value access.

	Input:
		- dim (str): Dimension of metadata to read. Must be either "row" or "col"
		- node_info (GCTXAttrInfo): GCTXAttrInfo instance from which to get node prefixes/suffixes.

	Output:
		tuple containing relevant prefix and suffix in positions 0 and 1, respectively.
	"""
	if dim == "row":
		id_vals_prefix = node_info.metadata_prefix + "/" + node_info.row_metadata_suffix
		id_vals_suffix = node_info.rid_suffix
	elif dim == "col":
		id_vals_prefix = node_info.metadata_prefix + "/" + node_info.col_metadata_suffix
		id_vals_suffix = node_info.cid_suffix
	return (id_vals_prefix, id_vals_suffix)

def set_id_vals(id_vals, meta_df):
	"""
	Sets index of metadata data frame to id values.

	Input: 
		- id_vals (list of str): rid or cid values to set as index. 
		- meta_df (pandas.DataFrame): data frame corresponding to metadata fields of dimension specified.

	Output:
		- meta_df (pandas.DataFrame): data frame corresponding to metadata fields of dimension specified.
	"""
	if meta_df.shape != (0,0): # if there is metadata 
		meta_df.index = id_vals
	else: 
		meta_df = pandas.DataFrame(id_vals)
		logger.debug("empty meta_df after id_vals introduced: {}".format(meta_df))
		meta_df.set_index(0, inplace = True)
		logger.debug("empty meta_df after id_vals set as index: {}".format(meta_df))
	return meta_df

def set_metadata_index_and_column_names(dim, meta_df):	
	"""
	Sets index and column names to GCTX convention.

	Input:
		- dim (str): Dimension of metadata to read. Must be either "row" or "col"
		- meta_df (pandas.DataFrame): data frame corresponding to metadata fields of dimension specified.

	Output:
		- meta_df (pandas.DataFrame): data frame corresponding to metadata fields of dimension specified.
	"""
	if dim == "row":
		meta_df.index.name = "rid"
		meta_df.columns.name = "rhd"
	elif dim == "col":
		meta_df.index.name = "cid"
		meta_df.columns.name = "chd"
	return meta_df

def subset_by_ids(df, rid, cid):
	"""
	Subsets a DataFrame to specified rids/cids (if not None).

	Input: 
		- df: a DataFrame, with ids set to index and columns 
			(as relevant; note this is different for data_df vs. meta_df)
		- rid (list of strings): list of row ids to specifically keep from gctx. Default=None. 
		- cid (list of strings): list of col ids to specifically keep from gctx. Default=None. 

	Output:
		- df: subsetted DataFrame. 
	"""
	if rid == None and cid == None:
		pass
	elif rid != None and cid != None:
		df = df.loc[rid][cid]
	elif rid == None:
		df = df[cid]
	elif cid == None:
		df = df.loc[rid]
	return df

def replace_666(meta_df, convert_neg_666):
	"""
	Converts -666 values into numpy.NaN. 

	Input: 
		- meta_df (pandas.DataFrame): data frame corresponding to metadata fields of dimension specified.
		- convert_neg_666 (bool): whether to convert -666 values to numpy.nan or not 

	Output: 
		- meta_df (pandas.DataFrame): data frame corresponding to metadata fields of dimension specified.
	"""
	# TODO: Add comment on why does this exist 
	if convert_neg_666:
		meta_df = meta_df.replace([-666, "-666", -666.0], [numpy.nan, numpy.nan, numpy.nan])
	else:
		meta_df = meta_df.replace([-666, -666.0], ["-666", "-666"])
	return meta_df

