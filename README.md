# L1000 Analysis Tools v1.1


Copyright 2011-2015 Broad Institute of MIT and Harvard.

A collection of software tools to read and analyze data produced from
the LINCS project. For more information on the project,
including licensing, please visit ([lincsproject.org](http://lincsproject.org)).

## Analysis Tools

A brief description of the tools included in this software package is
given below. The Matlab implementation of the tools is currently the
most mature. Some basic utilities in R and java are also included. We
will update the tools as they become available. Please view the changelog
for changes.

### Matlab Tools: matlab/

#### Requirements

1. Matlab R2014b and above
2. Statistics Toolbox
3. Parallel Processing Toolbox [Optional]

#### Setting the MATLAB path
Run `cd l1ktools/matlab; l1kt_setup` or enter the `pathtool` command, click "Add with Subfolders...", and select the directory `l1ktools/matlab`.

### Running the standard CMap data processing pipeline
All scripts are contained within the `matlab/data_pipeline` folder.
A directory of example .lxb files from a LINCS Joint Project (LJP) plate under the `data/lxb` directory. Note that processing a full 384 well plate can take a long time depending on the machine you're running it on. For test purposes we provide a test plate with a smaller number of samples. We also provide pre-computed results in `matlab/data_pipeline/results` to enable viewing the outputs without running the pipeline and/or to compare their results with CMap's. These outputs are described in the section below. By default all outputs are saved in the current working directory, but this can be overriden using the `plate_path` argument to any of the scripts below.

```matlab
% Setup the envronment
l1kt_setup

% Plate to process
plate_name='LJP009_A375_24H_X1_B20';
% plate_name='TEST_A375_24H_X1_B20'; % A smaller test plate
% Parent folder containing raw lxb files for a given plate
raw_path=fullfile(mortarpath, '../data/lxb');
% Path to map files with sample annotations
map_path=fullfile(mortarpath, '../data/maps');
% Results folder
plate_path=pwd;

% Run the full pipeline on a plate of data
[gex_ds, qnorm_ds, inf_ds, zs_ds_qnorm, zs_ds_inf] = process_plate('plate', plate_name, 'raw_path', raw_path, 'map_path', map_path);

% Run specific parts of the pipeline
% Convert a directory of LXB files (level 1) into gene expression (GEX, level 2) matrix.
% here, using example data
gex_ds = level1_to_level2('plate', plate_name, 'raw_path', raw_path, 'map_path', map_path)

% Convert the GEX matrix (level 2) to quantile normalized (QNORM, level 3) matrices
% in both landmark and inferred (INF) gene spaces.
[qnorm_ds, inf_ds] = level2_to_level3('plate', plate_name, 'plate_path', plate_path)

% Convert the QNORM matrix (level 3) into z-scores (level 4).
% same procedure can be performed using INF matrix (not shown).
zs_ds = level3_to_level4(qnorm_ds, 'plate', plate_name, 'plate_path', plate_path)

```
**Note:** Because the peak detection algorithm is non-deterministic, it's possible that data in levels 2 through 4 could differ slightly for two instances of processing the same plate. The code allows reproducing a previous run by passing a random seed file to the `process_plate` script. We provide such a file at `resources/rndseed.mat`. Reproducing the results provided in `matlab/data_pipeline/results can` be done as follows:

```matlab
% reproduce provided results
[gex_ds, qnorm_ds, inf_ds, zs_ds_qnorm, zs_ds_inf] = process_plate('plate', plate_name, 'raw_path', raw_path, 'map_path', map_path, 'rndseed', 'resources/rndseed.mat');
```

#### Description of Pipeline Outputs

| File | Data Level | Gene Space | Description |
| ---- | ----------- | ----------- | ---------- |
| LJP009_A375_24H_X1_B20.map | n/a | n/a | Sample annotations file |
| LJP009_A375_24H_X1_B20_COUNT_n384x978.gct | n/a | landmark | Matrix of analyte counts per sample|
| LJP009_A375_24H_X1_B20_GEX_n384x978.gct | 2 | landmark | gene expression (GEX) values|
| LJP009_A375_24H_X1_B20_NORM_n371x978.gct | n/a | landmark | LISS normalized expession profiles |
| LJP009_A375_24H_X1_B20_QNORM_n371x978.gct | 3 | landmark | quantile normalized (QNORM) expession profiles |
| LJP009_A375_24H_X1_B20_INF_n371x22268.gct | 3 | full | quantile normalized (QNORM) expession profiles |
| LJP009_A375_24H_X1_B20_ZSPCQNORM_n371x978.gct | 4 | landmark | differential expression (z-score) signatures |
| LJP009_A375_24H_X1_B20_ZSPCINF_n371x22268.gct | 4 | full | differential expression (z-score) signatures |
| dpeak | n/a | n/a | folder containing peak detection outputs and QC | 
| liss | n/a | n/a | folder containing LISS outputs and QC | 
| calibplot.png |  n/a | n/a | Plot of invariant gene sets for each sample |
| quantile_plots.png |  n/a | n/a | Plot of the normalized and non-normalized expression quantiles |


---
#### Other Tools and Demos (under matlab/demos_and_examples)
* **l1kt_dpeak.m**: Performs peak deconvolution for all analytes in a single LXB file, and outputs a report of the detected peaks.
* **l1kt_plot_peaks.m**: Plots intensity distributions for one or more analytes in an LXB file.
* **l1kt_parse_lxb.m**:	Reads an LXB file and returns the RID and RP1 values.
* **l1kt_liss.m**: Performs Luminex Invariant Set Smoothing on a raw (GEX) input .gct file
* **l1kt_qnorm.m**:	Performs quantile normalization on an input .gct file
* **l1kt_infer.m**:	Infers expression of target genes from expression of landmark genes in an input .gct file

See the documentation included with each script for a details on usage
and input parameters.

#### Demo
* **dpeak_demo.m**: Demo of peak detection. To run the demo, start Matlab, change to the folder containing dpeak_demo and
type dpeak_demo in the Command Window. This will read a sample LXB
file (A10.lxb), generate a number of intensity distribution plots and create a
text report of the statistics of the detected peaks (A10_pkstats.txt).

* **example_methods.m**: Reads in a .gct and a .gctx file, z-score the data in the .gctx file, and read in an .lxb file. To run the demo, start Matlab, change to the folder containing example_methods and type example_methods at the command line.

---
### R Tools: R/

#### Requirements:

1. R versions 3.1 and above
2. prada package: http://www.bioconductor.org/packages/devel/bioc/html/prada.html
3. rhdf5 package: http://bioconductor.org/packages/release/bioc/html/rhdf5.html

#### Tools:

R tools are found under R/cmap

* **lxb2txt.R**:	Saves values from an LXB file as a tab-delimited text file.
* **lxb2txt.sh**: Bash wrapper to lxb2txt.R 
* **io.R**: Classes for reading and writing .gct / .gctx files

#### Demo
* **example_methods.R**: To run the demo, change to the folder containing example_methods.R and source the script. It will read in a .gctx file and display its contents.

---
### Java Tools: java/

#### Tools

* **GctxDataset.java**: Class to store a GCTX data set.
* **GctxReader.java**: Class to read a GCTX file.
* **LXBUtil.java**: Class to read and store .lxb files.

#### Demo
* **ReadGctxExample.java**: To run the demo, change to the java/examples folder, then compile by running `sh compileExamples.sh`, then run by running the `runExample*.sh` file that alligns with your OS. 

---
### Python Tools: python/

#### Requirements

1. Python 2.7 (untested under Python 3)
2. numpy: [http://numpy.scipy.org](http://numpy.scipy.org)
3. pandas: [http://pandas.pydata.org/](http://pandas.pydata.org/)
4. requests: [http://docs.python-requests.org/en/latest/](http://docs.python-requests.org/en/latest/)
5. pytables: [http://www.pytables.org/moin](http://www.pytables.org/moin)
6. blessings: [http://pypi.python.org/pypi/blessings](http://pypi.python.org/pypi/blessings)

#### Setting the Python path
To run, append l1ktools/python to the `PYTHONPATH` environment variable.

#### Tools
* **cmap/io/gct.py**: Classes to interact with .gct and .gctx files.

#### Demo
* **example_methods.py**: To run the demo, change to the folder containing `example_methods.py` and run the script. It will read in a .gctx file, display its contents, and write to disc.

---
## Common data analysis tasks


### Reading .gct and .gctx files
* **MATLAB**: Use the `parse_gctx` function.
* **R**: Source the script `l1ktools/R/cmap/io.R`. Then use the `parse.gctx` function.
* **Python**: Import the module `cmap.io.gct`. Then instantiate a GCT object, and call its `read()` method. For more information, see the documentation on the GCT class.
* **Java**: See `ReadGctxExample.java` for an example.

### Creating .gct and .gctx files
* **MATLAB**: Use the `mkgct` and `mkgctx` functions.
* **R** Source the script `l1ktools/R/cmap/io.R`. Then use the `write.gctx` or `write.gct` functions.
* **Python**: Import `cmap.io.gct` and instantiate a GCT object. Then call the `build` method or `build_from_DataFrame` method to assmble a GCT object from a data matrix and optionally row and column annotations. Finally, call the `write` method to write to file as a .gctx.

### Z-Scoring a data set
* **MATLAB**: Use the `robust_zscore` function. Also see the `example_methods.m` script.

### Reading / converting .lxb files
* **MATLAB**: To read an .lxb into the MATLAB workspace, use the `l1kt_parse_lxb` function.
* **R**: To convert an .lxb file to text, use the `R/cmap/lxb2txt.sh` script.

