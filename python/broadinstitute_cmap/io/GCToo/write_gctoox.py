import logging
import setup_GCToo_logger as setup_logger
import re
import warnings
import tables
import numpy
import GCToo
import parse_gctoox

__author__ = "Oana Enache"
__email__ = "oana@broadinstitute.org"

logger = logging.getLogger(setup_logger.LOGGER_NAME)

def write(gctoo_object, out_file_name, convert_back_to_neg_666 = True):
	"""
	Writes a GCToo instance to specified file.

	Input:
		- gctoo_object (GCToo): A GCToo instance.
		- out_file_name (str): file name to write gctoo_object to.
	"""
	# Note: pyTables throws a Natural Naming warning that needs to be caught
	with warnings.catch_warnings():
		warnings.simplefilter("ignore")

		# make sure out file has a .gctx suffix
		gctx_out_name = add_gctx_to_out_name(out_file_name)

		# open an hdf5 file to write to
		hdf5_out = tables.openFile(gctx_out_name, mode = "w")

		# write version
		write_version(hdf5_out, gctoo_object)

		# write data matrix
		write_data_matrix(hdf5_out, gctoo_object)

		# write col metadata
		write_metadata(hdf5_out, "col", gctoo_object.col_metadata_df, convert_back_to_neg_666)

		# write row metadata
		write_metadata(hdf5_out, "row", gctoo_object.row_metadata_df, convert_back_to_neg_666)

		# close gctx file
		hdf5_out.close()

def add_gctx_to_out_name(out_file_name):
	"""
	If there isn't a '.gctx' suffix to specified out_file_name, it adds one.

	Input:
		- out_file_name (str): the file name to write gctx-formatted output to.
			(Can end with ".gctx" or not)

	Output:
		- out_file_name (str): the file name to write gctx-formatted output to, with ".gctx" suffix
	"""
	if not out_file_name.endswith(".gctx"):
		out_file_name = out_file_name + ".gctx"
	return out_file_name

def write_version(open_hdf5_writer, gctoo_object, version_number="GCTX1.0"):
	"""
	Writes version to proper node of gctx out file. 

	Input:
		- open_hdf5_writer (hdf5): open hdf5 file (via PyTables) to write to
		- version_number (str): version of gctx file. Default = 1.0 

	Note: for now version is always 1.0 so this is hardcoded as default
	"""
	if gctoo_object.version is None:
		open_hdf5_writer.setNodeAttr("/", "version", version_number)
	else:
		open_hdf5_writer.setNodeAttr("/", "version", gctoo_object.version)

def write_data_matrix(open_hdf5_writer, gctoo_object):
	"""
	Writes data matrix to proper node of gctx out (hdf5) file. 

	Input:
		- open_hdf5_writer (hdf5): open hdf5 file (via PyTables) to write to
		- gctoo_object (GCToo): a GCToo instance 

	"""
	# create a group with proper nesting/headings first
	open_hdf5_writer.createGroup("/","0")
	open_hdf5_writer.createGroup("/0/DATA", "0", createparents = True)
	# write data matrix to expected node
	# Note: need to re-transpose parsed data matrix to preserve standard format
	logger.debug("Writing data df to /0/DATA/0/matrix...")
	logger.debug("Data DF being written's shape: {}".format(gctoo_object.data_df.transpose().as_matrix().shape))
	open_hdf5_writer.createArray("/0/DATA/0", "matrix", gctoo_object.data_df.transpose().as_matrix())

def write_metadata(open_hdf5_writer, dim, metadata_df, convert_back_to_neg_666):
	"""
	Writes either column or row metadata to proper node of gctx out (hdf5) file.

	Input: 
		- open_hdf5_writer (hdf5): open hdf5 file (via PyTables) to write to
		- metadata_df (PANDAS DataFrame): metadata data frame to write to node 
	"""
	if dim == "col":
		open_hdf5_writer.createGroup("/0/META", "COL", createparents=True)
		gctx_hdf5_metadata_node_name = "/0/META/COL"
	elif dim == "row":
		open_hdf5_writer.createGroup("/0/META", "ROW", createparents=True)
		gctx_hdf5_metadata_node_name = "/0/META/ROW"
	else:
		logger.error("dim parameter must be 'row' or 'col'!")
	# write id field to expected node
	open_hdf5_writer.createArray(gctx_hdf5_metadata_node_name, "id", list(metadata_df.index))
	# if specified, convert NaNs in metadata back to -666
	if convert_back_to_neg_666:
		for c in list(metadata_df.columns):
			metadata_df[[c]] = metadata_df[[c]].replace([numpy.nan], ["-666"])
	# write metadata columns to their own arrays
	all_fields = list(metadata_df.columns)	
	for field in [entry for entry in all_fields if entry != 'ind']:
		open_hdf5_writer.createArray(gctx_hdf5_metadata_node_name, field,
			numpy.array(list(metadata_df[field])))
