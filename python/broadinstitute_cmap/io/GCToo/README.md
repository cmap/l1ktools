# GCToo library 

This is a package of Python scripts that enable reading, writing, and basic modifications (slicing, concatenation) of .gct and .gctx files.

GCToo will eventually be a package available on Bioconda, but for now it is a component of the l1ktools repository. Briefly, l1ktools provides basic parse/slice/writing functionality in Matlab, Python, and R for those interacting with .gct and .gctx files. 

## Maintainer

Oana Enache 
oana@broadinstitute.org  
October 2016

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

1. You'll notice there's a file in /GCToo very creatively called conda_environment.yml. This has all the dependencies (in the proper version) of GCToo. To create the GCToo environment, move to the 'GCToo' directory and type the following:

      ```
      conda env create -f conda_environment.yml
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
