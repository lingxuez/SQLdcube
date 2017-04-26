###########################################
## D-Cube for dense sub-block detection.
##
## Usage: 
## $ python dcube.py -db <database> -user <user> -port <port> \
##          -in <input_file> -K <K> -N <N>
##
## More options:
##   -db: an existing database name in postgresql;
##   -user: user for postgresql
##   -port: port for postgresql
##   -in: path to input .csv file
##   -K: number of dense blocks to detect
##   -N: number of dimensions
##   -outdir: output directory
##   -dmeasure: "arithmetic" or "geometric" or "suspicious"
##   -policy: "density" or "cardinality"
##   -opt: implementation method, "copy" or "mark"; "copy" is usually more efficient.
##   -data: "custom" if using custom input dataset;
##       the algorithm also provides 5 built-in scenarios, including
##       "darpa", "airforce", "wiki", "amazon", "yelp".
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
        print "\tUsing 'Mark' implementation."
        dcube_mark(data_table, col_names, X_name, K, N, cur, 
            dmeasure, policy, outdir, out_prefix,
            verbose, para_index, r_index, b_index, Bn_index)
    elif opt == "copy":
        print "\tUsing 'Copy' implementation."
        dcube_copy(data_table, col_names, X_name, K, N, cur, 
            dmeasure, policy, outdir, out_prefix,
            verbose, para_index, r_index, b_index, Bn_index)
    else:
        print "ERROR: -opt must be either 'mark' or 'copy'."



def dcube_test(dbname, user, port, file_name, K, N, sep=",",
                dmeasure="arithmetic", policy="density", outdir="out/", opt="copy"):
    """
    Dense subtensor mining using D-cube.
    Args:
        dbname: an existing database name
        user: user
        port: port number
        file_name: .csv file to load data
        K: number of blocks to detect
        dmeasure: "arithmetic" or "geometric" or "suspicious", density measure
        policy: "cardinality" or "density"
    """
    ## N-way tensor + measure
    data_table = "test_data"
    col_names = ["D"+str(i) for i in range(N)]
    columns = [col_names[i] + " varchar(40)" for i in range(N)]
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
                outdir=outdir, out_prefix=file_prefix, verbose=True, opt=opt)

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
        data: name of the real dataset
    """
    ## settings
    policy="density"
    (data_table, N, col_names, b_index, dmeasure) = info_realdata(data=data)

    ## columns
    columns = [col_names[i] + " varchar(80)" for i in range(N)]
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
    parser.add_argument("-db", "--dbname", type=str, default="test")
    parser.add_argument("-user", "--user", type=str, default="postgres")
    parser.add_argument("-port", "--port", type=str, default="5432")
    parser.add_argument("-data", "--data", type=str, default="custom")
    parser.add_argument("-in", "--file_name", type=str, default="tests/test_data.csv")
    parser.add_argument("-K", "--K", type=int, default=1)
    parser.add_argument("-N", "--N", type=int, default=3)
    parser.add_argument("-outdir", "--outdir", type=str, default="out/")
    parser.add_argument("-dmeasure", "--dmeasure", type=str, default="arithmetic")
    parser.add_argument("-policy", "--policy", type=str, default="density")
    parser.add_argument("-opt", "--opt", type=str, default="copy")
    args = parser.parse_args()

    ## check validity of input
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
        dcube_test(args.dbname, args.user, args.port, args.file_name, args.K, args.N,
                dmeasure=args.dmeasure, policy=args.policy, outdir=args.outdir, opt=args.opt)
    else:
        dcube_realdata(args.dbname, args.user, args.port, args.file_name, args.K, args.data,
                outdir=args.outdir, opt=args.opt)


