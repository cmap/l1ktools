import logging
import setup_GCToo_logger as setup_logger
import parse_gctoo
import parse_gctoox

__author__ = "Oana Enache"
__email__ = "oana@broadinstitute.org"

# instantiate logger
logger = logging.getLogger(setup_logger.LOGGER_NAME)
# when not in debug mode, probably best to set verbose=False
setup_logger.setup(verbose = True)

def parse(file_path, convert_neg_666=True, rid=None, cid=None, nan_values=None):
	""" 
	Identifies whether file_path corresponds to a .gct or .gctx file and calls the
	correct corresponding parse method.

	Input:

		- file_path (str):
	""" 
	if file_path.endswith(".gct"):
		curr = parse_gctoo.parse(file_path, convert_neg_666, rid, cid)
	elif file_path.endswith(".gctx"):
		curr = parse_gctoox.parse(file_path, convert_neg_666, rid, cid)
	else:
		logger.error("File to parse must be .gct or .gctx!")
	return curr 



