###########################################
## PostgreSQL implementation of D-Cube.
##
## Authors:
## Lingxue Zhu (lzhu1@andrew.cmu.edu)
## Jining Qin (jiningq@andrew.cmu.edu)
###########################################
## Usage: 
## $ python dcube.py [-h] -db DBNAME -user USERNAME -port PORT
##            -in INFILE -K K -N N [-outdir OUTDIR] 
##            [-dmeasure DMEASURE] [-policy POLICY] [-opt OPTMETHOD]
##
## optional arguments:
##  -h, --help            show this help message and exit
##  -db, --dbname         the name of the database to use; default is system $USER
##  -user, --user         the database user; default is system $USER
##  -port, --port         the database port number; default is 5432
##  -in, --file_name      Full path to the .csv file to load from. The file should have
##                        N+1 columns, where the first N columns are N attributes,
##                        and the last column is the mass
##  -K, --K               number of dense blocks to detect
##  -N, --N               number of dimensions of the tensor
##  -outdir, --outdir     output directory; the results will be saved under this directory
##  -dmeasure, --dmeasure  
##                        density measure method, one of 'arithmetic', 'geometric',
##                        or 'suspicious'; default is 'arithmetic'
##  -policy, --policy     dimension selection policy, either 'density' or 'cardinality';
##                        default is 'density'
##  -opt, --opt           optimization method, either 'copy' or 'mark';
##                        default is 'copy', which is in general more efficient
##  -data, --data         default is 'custom', where user specifies the above parameters;
##                        in addition, the script provides special settings for 5 datasets:
##                        'darpa', 'wiki', 'amazon', 'yelp', 'airforce',
##                        where specific N, dmeasure and policy are used;
##                        in this case, the options -dmeasure, -N, -policy will be ignored
##########################################

import argparse, os, sys
from dcube_sql_mark import *
from dcube_sql_copy import *

def dcube(data_table, col_names, X_name, K, N, cur, 
            dmeasure="arithmetic", policy="cardinality", 
            outdir="out/", out_prefix="out",
            opt="copy", verbose=True,
            para_index=True, r_index=-1, b_index=2, Bn_index=True):
    """
    D-Cube algorithm. Find K dense blocks in a given tensor.
    Args:
        data_table: name of the relation, it has N+1 columns, 
                where the first N columns for the N dimensions with names col_names,
                and the last column specifying the measure attribute with name X_name.
        col_names: column names of the relation for the N dimensions
        X_name: column name of the measure attribute
        K: number of blocks
        N: number of dimension
        cur: cursor of database connection
        dmeasure: "arithmetic" or "geometric" or "suspicious", density measure
        policy: "cardinality" or "density"
        outdir: output directory
        out_prefix: prefix of the output results
        opt: optimization method: "copy" or "mark"
        para_index: whether to create a hash index for parameters
        r_index: create an index for the i-th attribute in data_table
        b_index: create an index for the i-th attribute in Btable
        Bn_index: whether to create index on Bn
    """
    if opt == "mark":
        # print "\tUsing 'Mark' implementation."
        dcube_mark(data_table, col_names, X_name, K, N, cur, 
            dmeasure, policy, outdir, out_prefix,
            verbose, para_index, r_index, b_index, Bn_index)
    elif opt == "copy":
        # print "\tUsing 'Copy' implementation."
        dcube_copy(data_table, col_names, X_name, K, N, cur, 
            dmeasure, policy, outdir, out_prefix,
            verbose, para_index, r_index, b_index, Bn_index)
    else:
        print "ERROR: -opt must be either 'mark' or 'copy'."



def dcube_custom(dbname, user, port, file_name, K, N, sep=",",
                dmeasure="arithmetic", policy="density", outdir="out/", opt="copy"):
    """
    Dense subtensor mining using D-cube.
    Args:
        dbname: an existing database name
        user: user
        port: port number
        file_name: .csv file to load data
        K: number of blocks to detect
        N: number of dimensions
        dmeasure: "arithmetic" or "geometric" or "suspicious", density measure
        policy: "cardinality" or "density"
        outdir: output directory
        out_prefix: prefix of the output results
        opt: optimization method: "copy" or "mark"
    """
    ## N-way tensor + measure
    data_table = "test_data"
    col_names = ["D"+str(i) for i in range(N)]
    columns = [col_names[i] + " varchar" for i in range(N)]
    X_name = "measure" ## name for measure attribute
    X_fmt = "double precision" ## type for measure attribute

    ## Connect to the database
    DSN = "dbname=%s user=%s port=%s host='/tmp/'" % (dbname, user, port)
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            ## drop existing tables in database
            cur.execute("DROP SCHEMA public CASCADE;")
            cur.execute("CREATE SCHEMA public;")

            ## load data from file
            print ("Loading data from %s..." % file_name)
            cur.execute("CREATE TABLE %s (%s, %s %s);" % (data_table, ",".join(columns), X_name, X_fmt))
            with open(file_name, mode="rt") as fin:
                cur.copy_from(fin, data_table, sep=sep)

            ## D-CUBE
            print "Performing D-Cube..."
            file_prefix=(file_name.rsplit("/")[len(file_name.rsplit("/"))-1]).split(".csv")[0]
            dcube(data_table, col_names, X_name, K, N, cur, dmeasure, policy,
                outdir=outdir, out_prefix=file_prefix, verbose=False, opt=opt)

            ## clean up
            cur.execute("DROP TABLE %s;" % data_table)


def info_realdata(data="darpa"):
    """
    Get the information for real-world data sets.
    Args:
        data: name of the real dataset
    """
    data_table = data
    if data == "darpa":
        N = 3
        col_names = ["src", "dst", "time"]
        b_index = 2
        dmeasure = "arithmetic"
    elif data == "wiki":
        N = 3
        col_names = ["usr", "page", "time"]
        b_index = 2
        dmeasure = "geometric"
    elif data == "amazon":
        N = 4
        col_names = ["usr", "item", "time", "rating"]
        b_index = 0
        dmeasure = "arithmetic"
    elif data == "yelp":
        N = 4
        col_names = ["usr", "business", "time", "rating"]
        b_index = 0
        dmeasure = "arithmetic"
    elif data == "airforce":
        N = 7
        col_names = ["protocol", "service", "src", "dst", "flag", "host", "srv"]
        b_index = 4
        dmeasure = "arithmetic"
    else:
        print "Invalid -data option."
        sys.exit(1)

    return (data_table, N, col_names, b_index, dmeasure)
        


def dcube_realdata(dbname, user, port, file_name, K, data="darpa", 
            sep=",", outdir="out/", opt="copy"):
    """
    Dense subtensor mining using D-cube.
    Args:
        dbname: an existing database name
        user: user
        port: port number
        file_name: .csv file to load data
        sep: delimiter for the input file; "," for .csv
        K: number of blocks to detect
        N: number of dimensions
        data: name of the real dataset
        outdir: output directory
        out_prefix: prefix of the output results
        opt: optimization method: "copy" or "mark"
    """
    ## settings
    policy="density"
    (data_table, N, col_names, b_index, dmeasure) = info_realdata(data=data)

    ## columns
    columns = [col_names[i] + " varchar" for i in range(N)]
    X_name = "measure" ## name for measure attribute
    X_fmt = "double precision" ## type for measure attribute

    ## Connect to the database
    DSN = "dbname=%s user=%s port=%s host='/tmp/'" % (dbname, user, port)
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            ## drop existing tables in database
            cur.execute("DROP SCHEMA public CASCADE;")
            cur.execute("CREATE SCHEMA public;")

            ## load data from file
            print ("Loading data from %s..." % file_name)
            cur.execute("CREATE TABLE rawData (%s);" % (",".join(columns)))
            with open(file_name, mode="rt") as fin:
                cur.copy_from(fin, "rawData", sep=sep)

            ## compute measurement: number of times the entry appears
            cur.execute(("CREATE TABLE %s AS " % data_table)
                + "SELECT %s, count(*) AS %s FROM rawData GROUP BY %s;" % 
                        (",".join(col_names), X_name, ",".join(col_names)))
            cur.execute("DROP TABLE rawData;")


            ## D-CUBE
            print ("Performing D-Cube on data %s ..." % data)
            file_prefix=(file_name.rsplit("/")[len(file_name.rsplit("/"))-1]).split(".csv")[0]

            dcube(data_table, col_names, X_name, K, N, cur, dmeasure, policy,
                outdir=outdir, out_prefix=file_prefix, verbose=False, opt=opt,
                b_index=b_index)

            ## clean up
            cur.execute("DROP TABLE %s;" % data_table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="D-Cube Using PostgreSQL.")
    parser.add_argument("-db", "--dbname", type=str, default=os.environ['USER'],
        help="the name of the database to use; default is system $USER")
    parser.add_argument("-user", "--user", type=str, default=os.environ['USER'],
        help="the database user; default is system $USER")
    parser.add_argument("-port", "--port", type=str, default="5432",
        help="the database port number; default is 5432")
    parser.add_argument("-in", "--file_name", type=str, default="demo/demo_data.csv",
        help="""Full path to the .csv file to load from. The file should have N+1 columns,
        where the first N columns are N attributes, and the last column is the mass""")
    parser.add_argument("-K", "--K", type=int, default=1,
        help="number of dense blocks to detect")
    parser.add_argument("-N", "--N", type=int, default=3,
        help="number of dimensions of the tensor")
    parser.add_argument("-outdir", "--outdir", type=str, default="demo/demo_out",
        help="output directory; the results will be saved under this directory")
    parser.add_argument("-dmeasure", "--dmeasure", type=str, default="arithmetic",
        help="""density measure method, one of 'arithmetic', 'geometric',
        or 'suspicious'; default is 'arithmetic'""")
    parser.add_argument("-policy", "--policy", type=str, default="density",
        help="""dimension selection policy, either 'density' or 'cardinality'; 
        default is 'density'""")
    parser.add_argument("-opt", "--opt", type=str, default="copy",
        help="""optimization method, either 'copy' or 'mark';
        default is 'copy', which is in general more efficient""")
    parser.add_argument("-data", "--data", type=str, default="custom",
        help="""default is 'custom', where the user specifies all the above parameters;
        in addition, the script provides special settings for 5 datasets:
        'darpa', 'wiki', 'amazon', 'yelp', 'airforce', where specific N, dmeasure 
        and policy are used, and the options -dmeasure, -N, -policy will be ignored""")
    args = parser.parse_args()

    ## check validity of the arguments
    if not os.path.exists(args.outdir):
        os.makedirs(args.outdir)

    if (args.dmeasure != "arithmetic" and args.dmeasure != "geometric" 
            and args.dmeasure != "suspicious"):
        print "-dmeasure must be one of 'arithmetic' or 'geometric' or 'suspicious'."
        sys.exit(1)
    
    if args.policy != "density" and args.policy != "cardinality":
        print "-policy must be one of 'density' or 'cardinality'."
        sys.exit(1)        

    ## D-cube
    if args.data == "custom":
        dcube_custom(args.dbname, args.user, args.port, args.file_name, args.K, args.N,
                dmeasure=args.dmeasure, policy=args.policy, outdir=args.outdir, opt=args.opt)
    else:
        ## the 5 built-in datasets with specific parameter choices
        dcube_realdata(args.dbname, args.user, args.port, args.file_name, args.K, args.data,
                outdir=args.outdir, opt=args.opt)


