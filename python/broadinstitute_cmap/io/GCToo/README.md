# GCToo library 

This is a package of Python scripts that enable reading, writing, and basic modifications (slicing, concatenation) of .gct and .gctx files.

GCToo will eventually be a package available on Bioconda, but for now it is a component of the l1ktools repository. Briefly, l1ktools provides basic parse/slice/writing functionality in Matlab, Python, and R for those interacting with .gct and .gctx files. 

## Maintainer

Oana Enache 
oana@broadinstitute.org  
October 2016

## What is GCToo? 

GCToo is a class representing the contents of .gct or .gctx files. For instance, a GCT file is represented as illustrated in the blue figure at left; parsing in a file creates a GCToo instance with the attributes delimited at right. 

![GCT_to_GCToo](https://github.com/cmap/l1ktools/blob/master/python/broadinstitute_cmap/io/GCToo/simple_GCT_to_GCToo_figure.png)

## Setting up your environment

### To set up your environment the first time:

*NOTE* We intend to package and distribute this on Bioconda soon; when this occurs, installation instructions will change (hopefully to become even easier!). 

#### (if applicable) Install miniconda

1. Install miniconda (if you don't have it already!). Download/follow installation instructions from http://conda.pydata.org/miniconda.html. Unless you have personal preferences/reasons for doing so, we recommend installing Miniconda over Anaconda (it's more lightweight).

2. Type `conda info` to verify that conda has been installed on your system. You should see some information about the "Current conda install." If you don't, then conda has not been installed properly.

#### Clone GCToo to a local directory

1. Clone the l1ktools directory ([l1ktools](https://github.com/cmap/l1ktools "l1ktools")) onto your computer. For example, I might clone it to `/Users/my_name/code/l1ktools`.

2. We will now a run a script to make our environment aware of files that we need. (You may need to checkout a different branch first; try `git checkout develop`.) Move to the `python` directory and run another setup script by typing the following:
    
      ```
      cd /Users/my_name/code/l1ktools/python/
      python setup.py develop
      ```

#### Setup the GCToo conda environment for the first time 

More details can be found here:http://conda.pydata.org/docs/using/envs.html#create-an-environment

1. To create the GCToo environment (with appropriate versions of packages), type the following:

      ```
      conda create --name GCToo python=2.7.11 numpy=1.11.2 pandas=0.18 h5py=2.6.0 
      ```

2. Activate the new environment (GCToo)
    
    On Windows:
     ```
     activate GCToo
     ```
    
    On Linux, OS X: 
     ```
     source activate GCToo
     ```
    
3. Check that the environment was installed correctly by typing: 
     ```
     conda list
     ```
    The packages listed should be the same as those in conda_environment.yml. 

### To set up your environment after the first time:

  1. Activate your conda environment by typing:
       
    On Windows:
     ```
     activate GCToo
     ```
    
    On Linux, OS X: 
     ```
     source activate GCToo
     ```
  2. It's easiest to run your code from the `GCToo` directory, so move there. For instance, if we wanted to run a test we would do the    following:  
    
      ```
      cd /Users/my_name/code/l1ktools/python/broadinstitute_cmap/io/GCToo
      python test_parse_gctoo.py
       ```

## Adding packages to your environment
For instance, if you'd like to add matplotlib, plotly, seaborn, etc. Activate your GCToo conda environment, then type the following:
```
conda install --name GCToo matplotlib
```
...and follow instructions. 

More details at: http://conda.pydata.org/docs/using/pkgs.html#install-a-package

## Examples

For all of these, don't forget to activate your GCToo conda environment first!

### Use Case 1: Read an entire .gct or .gctx file to a GCToo instance

From an active python session in your local GCToo directory (adjust import statements accordingly if not):

```python
from broadinstitute_cmap.io.GCToo import parse

# works for either .gct or .gctx files
# Note: suggestions welcome for names! We think "parse.parse" sounds a little goofy. 
my_gctoo = parse.parse("something.gctx")
```
     
### Use Case 2 (GCTX only): Only read in row or column metadata from a .gctx file.

Say your GCTX file is too big (and so you don't want to read the entire thing into memory), and/or you know you only want certain rids/cids but need to find what those are. Returns a pandas DataFrame of row or column metadata as specified. 

```python
from broadinstitute_cmap.io.GCToo import parse

# read in row metadata only
my_row_metadata = parse.parse("something.gctx", meta_only = "row")

# read in column metadata only
my_col_metadata = parse.parse("something.gctx", meta_only = "col")
```

### Use Case 3: Read in only a certain subset of rids and/or cids to a GCToo instance

Notes: 
- Practically speaking, this is more useful for GCTX files than GCT files, since (as a text file) you'll need to read in the entire GCT file anyway.  
- You'll need to have a list of desired rids and/or cids already (can be obtained from doing Use Case 2)
  
```python
from broadinstitute_cmap.io.GCToo import parse

my_rids = ['200814_at', '218597_s_at', '217140_s_at']
my_cids = ['LJP005_MCF7_24H_X1_B17:A03', 'LJP005_MCF7_24H_X1_B17:A04', 'LJP005_MCF7_24H_X1_B17:A05']

# works for both .gct and .gctx files
# you can subset by rids, cids, or both rids and cids
my_gctoo = parse.parse("something.gctx", rid = my_rids, cid = my_cids)
```

### Use Case 4: Slice a .gct or .gctx file to a specific subset of rows/columns (either from command line to a {.gct, .gctx} file or in an active python session to a GCToo instance)

Note: In CMap world, a .grp file is just a new-line delimited text file. 

From the command line: Say you have a .gct file (found at path/to/file.gct) and two (new line delimited!) text files of row ids:
    - interesting_genes.grp: probes of interest that you'd like to keep for further analysis
    - boring_genes.grp: probes of interest that you'd like to remove from your .gct file for now. 

```
python ~/code/l1ktools/python/broadinstitute_cmap/io/GCToo/slice_gct.py -i path/to/file.gct --rid interesting_genes.grp --exclude_rid boring_genes.grp
```
From an active python session (assume you've already parsed in a file and have an instance of GCToo called `my_gctoo`). For this use case, you'll need to have a list of desired and/or undesired rids and/or cids already (can be obtained from doing Use Case 2 or by other means)

```python
from broadinstitute_cmap.io.GCToo import slice_gct

interesting_rids = ['200814_at', '218597_s_at']
boring_rids = ['217140_s_at']
interesting_cids = ['LJP005_MCF7_24H_X1_B17:A03', 'LJP005_MCF7_24H_X1_B17:A04', 'LJP005_MCF7_24H_X1_B17:A05']

sliced_gctoo = slice_gct.slice_gctoo(my_gctoo, rid = interesting_rids, cid = interesting_cids, exclude_rid = boring_rids)
```
     
### Use Case 5: Write a GCToo instance to .gct or .gctx 
Assume you're in an active python session in your local GCToo directory and have a GCToo instance (let's call it `my_gctoo` that you'd like to write to .gct or .gctx. 

To write to a GCT file: 
```python
from broadinstitute_cmap.io.GCToo import write_gctoo 

write_gctoo.write(my_gctoo, "some/path/to/my_gctoo.gct")  
```
     
To write to a GCTX file: 
```python
from broadinstitute_cmap.io.GCToo import write_gctoox

write_gctoox.write(my_gctoo, "some/path/to/my_gctoo.gctx")  
```

### Use Case 6: From your own DataFrames of expression values and/or metadata, create a GCToo instance
Say you have 3 pandas DataFrames consisting of a data matrix, row metadata values, and col metadata values. For this, you'll need to call `from broadinstitute_cmap.io.GCToo import GCToo`.

*NOTE* To create a valid GCToo instance, these DataFrames must satisfy the following requirements (the GCToo constructor will also check for these):
- data matrix DF (call it `my_data`): index are unique rids that map to corresponding row metadata values, columns are unique cids that map to corresponding col metadata values 
- row metadata DF (call it `my_row_metadata`): index are unique rids that map to corresponding numerical values in data matrix; columns are unique descriptive headers
- col metadata DF (call it `my_col_metadata`): index are unique cids that map to corresponding numerical values in data matrix; columns are unique descriptive headers

```python
from broadinstitute_cmap.io.GCToo import GCToo

my_GCToo = GCToo.GCToo(data_df=my_data, row_metadata_df=my_row_metadata, col_metadata_df=my_col_metadata)
```

### Use Case 7: From the command line, convert a gct -> gctx (or vice versa) 
Converting from a gct to a gctx might be useful if you have a large gct and want faster IO in the future. 

To write some_thing.gct -> some_thing.gctx in working directory:

```
python gct2gctx.py -filename some_thing.gct 
```

To write some_thing.gct to a .gctx named something_else.gctx in a different out directory (both -outname and -outpath are optional):
```
python gct2gctx.py -filename some_thing.gct -outname something_else -outpath my/special/folder
```

Converting a gctx to a gct might be useful if you want to look at your .gctx file in a text editor or something similar. 

To write some_thing.gctx -> some_thing.gct in working directory:
```
python gctx2gct.py -filename some_thing.gctx
```

To write some_thing.gctx to a .gct named something_else.gct in a different out directory (both -outname and -outpath are optional):
```
python gctx2gct.py -filename some_thing.gctx -outname something_else -outpath my/special/folder
```


### Use Case 8: From the command line, concatenate a bunch of .gct or .gctx files 

A. You have a bunch of files that start with 'LINCS_GCP' in your Downloads folder that you want to concatenate. Type the following in your command line:

```
python /Users/some_name/code/l1ktools/python/broadinstitute_cmap/io/GCToo/concat_gctoo.py --file_wildcard '/Users/some_name/Downloads/LINCS_GCP*'
```

This will save a file called `concated.gct` in your current directory.  Make sure that the wildcard is in quotes!

B. You have 2 files that you want to concatenate: /Users/some_name/file_to_concatenate1.gct and /Users/some_name/file_to_concatenate2.gct. Type the following in your command line:

```
python /Users/some_name/code/l1ktools/python/broadinstitute_cmap/io/GCToo/concat_gctoo.py --list_of_gct_paths /Users/some_name/file_to_concatenate1.gct /Users/some_name/file_to_concatenate2.gct
```

C. You have 2 GCToo objects in memory that you want to concatenate. hstack is the method in concat_gctoo.py that actually does the concatenation. From within the Python console or script where you have your 2 GCToos (gct1 & gct2), type the following:

```python
from broadinstitute_cmap.io.GCToo import concat_gctoo as cg
concated = cg.hstack([gct1, gct2])
```

