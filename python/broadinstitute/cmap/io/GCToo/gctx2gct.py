import logging
import cmap.io.setup_logger as setup_logger
import argparse
import sys
import GCToo
import parse_gctoox
import write_gctoo
import cmap.io.plategrp 

logger = logging.getLogger(setup_logger.LOGGER_NAME)


def build_parser():
	parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	# required
	parser.add_argument("-filename", help=".gct file that you would like converted to .gctx form")
	# optional
	parser.add_argument("-outname", help="(optional) name for output gct file", default=None)
	parser.add_argument("-verbose", "-v", help="Whether to print a bunch of output.", action="store_true", default=False)
	return parser


def main(args):
	in_gctoo = parse_gctoox.parse(args.filename, convert_neg_666=False, rid=rid_list, cid=cid_list)

	if args.outname != None:
		out_name = str.split(in_gctoo.src, "/")[-1].split(".")[0]
	else:
		out_name = args.outname

	write_gctoo.write(in_gctoo, out_name)


if __name__ == "__main__":
	args = build_parser().parse_args(sys.argv[1:])

	setup_logger.setup(verbose=args.verbose)

	main(args)