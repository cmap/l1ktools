# This script contains examples for reading .gctx files into R.
# In order for the script to work properly, make sure that the R working
# directory is the directory containing the script.

# source the io script
source("cmap/io.R")


# initialize a GCT object with a single signature
ds <- parse.gctx("/cmap/data/build/a2y13q1/modzs.gctx", cid="CPC005_A375_6H:BRD-A85280935-003-01-7:10")
