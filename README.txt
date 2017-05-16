A PostgreSQL implementation of D-CUBE (Disk-based Dense-block Detection).

Authors:
Lingxue Zhu
Jining Qin
============

This is the course project of CMU 15-826:
http://www.cs.cmu.edu/~christos/courses/826.S17/project-default/index.html

D-CUBE algorithm is described in the following paper: 
http://www.cs.cmu.edu/~kijungs/codes/dcube/paper.pdf

The original Java implementation of D-CUBE is available at 
http://www.cs.cmu.edu/~kijungs/codes/dcube/
============

USAGE
============
$ python dcube.py [-h] -db DBNAME -user USERNAME -port PORT
            -in INFILE -K K -N N [-outdir OUTDIR] 
            [-dmeasure DMEASURE] [-policy POLICY] [-opt OPTMETHOD]

D-Cube Using PostgreSQL.

optional arguments:
  -h, --help            show this help message and exit
  -db, --dbname         the name of the database to use; default is system $USER
  -user, --user         the database user; default is system $USER
  -port, --port         the database port number; default is 5432
  -in, --file_name      Full path to the .csv file to load from. The file should have
                        N+1 columns, where the first N columns are N attributes,
                        and the last column is the mass
  -K, --K               number of dense blocks to detect
  -N, --N               number of dimensions of the tensor
  -outdir, --outdir     output directory; the results will be saved under this directory
  -dmeasure, --dmeasure  
                        density measure method, one of
                        'arithmetic', 'geometric' or 'suspicious';
                        default is 'arithmetic'
  -policy, --policy     dimension selection policy, either 'density' or 'cardinality';
                        default is 'density'
  -opt, --opt           optimization method, either 'copy' or 'mark';
                        default is 'copy', which is in general more efficient
  -data, --data         default is 'custom', where the user specifies all the above parameters;
                        in addition, the script provides special settings for 5 datasets:
                        'darpa', 'wiki', 'amazon', 'yelp', 'airforce',
                        where specific N, dmeasure and policy are used, and the options 
                        -dmeasure, -N, -policy will be ignored

STEPS TO RUN
============
1. Create a postgres database, specify the database user and port number. 
   The easiest and default way is to use the system $USER for the database name and user, 
   and use the default port number 5432. This can be done by running
    $ make setup
   
2. Make sure python 2.7 is installed in the system, and psycopg2 is installed.

3. Give read permissions to the postgres user for all the input files,
   and write permission for the output directory.

4. Start the PostgreSQL server. If you used the default setting in step 1, 
   then this can be done by running
    $ make start

5. Run the python script with your specified input file. A demo input file for a 3-way tensor
   is provided along with this package. Assuming you used the default setting in step 1, 
   the following command will find the dense block with size 5 x 5 x 5:
    $ python dcube.py -in demo/demo_data.csv -K 1 -N 3 \
      -outdir demo/demo_out -dmeasure arithmetic -policy density

   Alternatively, run
    $ make demo

   If you used a different database setting in step 1, please specify them through
   the options -db, -user, -port

6. After finishing, we recommend you to stop the PostgreSQL server. 
   If you used the default setting in step 1, then this can be done by running
    $ make stop

