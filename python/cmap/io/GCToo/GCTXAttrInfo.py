__author__ = 'Oana Enache'
__email__ = 'oana@broadinstitute.org'



class GCTXAttrInfo(object):
	"""
	Class representing all node prefixes and suffixes needed to access 
	relevant groups/nodes of gctx.
	"""
	def __init__(self):

		self.version_prefix = "/"
		self.src_prefix = "/"
		self.data_matrix_prefix = "/0/DATA/0"
		self.metadata_prefix = "/0/META" 

		self.version_suffix = "version"
		self.src_suffix = "src"
		self.matrix_suffix = "matrix"
		self.row_metadata_suffix = "ROW" 
		self.col_metadata_suffix = "COL"
		self.cid_suffix = "id" 
		self.rid_suffix = "id"
