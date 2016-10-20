"""
Generic parse method to parse either a .gct or a .gctx. 

Takes in a file path corresponding to either a .gct or .gctx, 
	and parses to a GCToo instance accordingly.

Note: Only supports v1.3 .gct files and v1.0 .gctx files. 
"""

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

def parse(file_path, convert_neg_666=True, rid=None, cid=None, nan_values=None, meta_only=False):
	""" 
	Identifies whether file_path corresponds to a .gct or .gctx file and calls the
	correct corresponding parse method.

	Input:
		Mandatory:
		- gctx_file_path (str): full path to gctx file you want to parse. 
		
		Optional:
		- convert_neg_666 (bool): whether to convert -666 values to numpy.nan or not 
			(see Note below for more details on this). Default = False.
		- rid (list of strings): list of row ids to specifically keep from gctx. Default=None. 
		- cid (list of strings): list of col ids to specifically keep from gctx. Default=None. 


	Output:
		- myGCToo (GCToo)

	Note: why does convert_neg_666 exist? 
		- In CMap--for somewhat obscure historical reasons--we use "-666" as our null value 
		for metadata. However (so that users can take full advantage of pandas' methods, 
		including those for filtering nan's etc) we provide the option of converting these 
		into numpy.NaN values, the pandas default. 
	""" 
	if file_path.endswith(".gct"):
		curr = parse_gctoo.parse(file_path, convert_neg_666, rid, cid)
	elif file_path.endswith(".gctx"):
		curr = parse_gctoox.parse(file_path, convert_neg_666, rid, cid)
	else:
		logger.error("File to parse must be .gct or .gctx!")
	return curr 



