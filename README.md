SageIngest
================

This code uses the DBIngestor library (https://github.com/aipescience/DBIngestor) to ingest SAGE catalogues (= Semi-Analytic Galaxy Evolution data from cosmological simulations) into a database.

These catalogues have a custom binary format, see https://github.com/darrencroton/sage for the original code and https://github.com/darrencroton/sage/blob/master/output/allresults.py for a Python read routine.

The data file to database field mapping is done directly in the reader/schema-mapper (no additional map-file).

For any questions, please contact me at
Kristin Riebe, kriebe@aip.de


Data files
-----------
There are usually a number of files per snapshot, either grouped together in subdirectories or all in the same directory. The snapshot number is given as one of the fields in the data file.
The column names roughly correspond to the names in the database table for most columns. Some columns are ignored for the database, though, some more are added (see getDataItem in SageReader.cpp).

WARNING
---------
Byte-alignment is set to 8 inside the code, since this is what was (automatically) used by the data creators when writing the C-structures into data files. May need to be adjusted for different versions of the data. 

Installation
--------------
see INSTALL

Example
--------
The *Example* directory contains:

* *create_sage_test.sql*: example create table statement  
* *sage_test.dat*: a test data file with 100 galaxies, binary format, little endian, galaxy-structure with 8-byte alignment, 64-bit machine (i.e. sizeof(GalaxyData)=240).

First a database and table must be created on your server (in the example, I use MySQL, adjust to your own needs). Then you can ingest the example data into the `SAGE` table with a command line like this: 

```
build/SageIngest.x  -s mysql -D TestDB -T SAGE -U myusername -P mypassword -H 127.0.0.1 -O 3306 -w 0 -h 0.6777 --maxRows=100 --fileNum=13 --blocksize=40 Example/sage_test.dat
```

Replace *myusername* and *mypassword* with your own credentials for your own database. 

The important new options are:  

`--swap`, `-w`: 0 if no byteswapping, 1 if byteswaping is necessary  p  
`--Planck`, `-h`: Planck's constant h (e.g. 0.6777 [default] for simulation MDPL2)  
`--blocksize`: number of rows to be read in one block; make sure that it fits into the memory of your machine [default: 1000]  
`-m`, `--maxRows`: maximum number of rows to be read; not more than total num. 
of rows will be read; used mainly for testing  



TODO
-----
* Make mapping more adjustable by using an external mapping file
* Allow to read only a subset of the data fields (those in mapping file)
* Calculate ix, iy, iz on the fly
* Stop when file-end is reached (not only at maxRows; do not rely on Ngals-value from file for total number of rows)
* Properly test byteswapping

