# This script contains examples for reading .gctx files into R.
# In order for the script to work properly, make sure that the R working
# directory is the directory containing the script.

# source the io script
source("cmap/io.R")


# read the gctx file
ds <- parse.gctx("../data/modzs_n272x978.gctx")

# inspect the matrix
print(ds@mat[1:10,1:10])

# inspect the row annotations
print(ds@rdesc[1:10,])

# inspect the column annotations
print(ds@cdesc[1:10,])


# example of slicing a file
# read the same gct file but just a single column (the first)
ds_sliced <- parse.gctx("../data/modzs_n272x978.gctx", cid=1)

# example of slicing a file
# similar example, but using the column ID instead of index
ds_sliced <- parse.gctx("../data/modzs_n272x978.gctx", cid="CPC006_A549_6H:BRD-U88459701-000-01-8:10")

# write this file
write.gctx(ds_sliced, "my_slice")
