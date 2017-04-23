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

import argparse, os, sys
from dcube_sql import *


def dcube_test(dbname, user, port, file_name, K, N, sep=",",
                dmeasure="arithmetic", policy="cardinality", outdir="out/"):
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
                outdir=outdir, out_prefix=file_prefix, verbose=True)

            ## clean up
            cur.execute("DROP TABLE %s;" % data_table)


def info_realdata(data="darpa"):
    """
    Get the information for real-world data sets.
    Args:
        data: "darpa"

    """
    if data == "darpa":
        data_table = "darpa"
        N = 3
        col_names = ["src", "dst", "time"]
    else:
        print "Invalid dataset name."

    return (data_table, N, col_names)
        


def dcube_realdata(dbname, user, port, file_name, K, data="darpa", sep=",",
                dmeasure="arithmetic", policy="cardinality", outdir="out/"):
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
    ## column names
    (data_table, N, col_names) = info_realdata(data=data)
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
            cur.execute("CREATE TABLE rawData (%s);" % (",".join(columns)))
            with open(file_name, mode="rt") as fin:
                cur.copy_from(fin, "rawData", sep=sep)

            ## compute measurement: number of times the entry appears
            cur.execute(("CREATE TABLE %s AS " % data_table)
                + "SELECT %s, count(*) AS %s FROM rawData GROUP BY %s;" % 
                        (",".join(col_names), X_name, ",".join(col_names)))
            cur.execute("DROP TABLE rawData;")

            ## D-CUBE
            print "Performing D-Cube..."
            file_prefix=(file_name.rsplit("/")[len(file_name.rsplit("/"))-1]).split(".csv")[0]

            dcube(data_table, col_names, X_name, K, N, cur, dmeasure, policy,
                outdir=outdir, out_prefix=file_prefix, verbose=False)

            ## clean up
            cur.execute("DROP TABLE %s;" % data_table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-db", "--dbname", type=str, default="test")
    parser.add_argument("-user", "--user", type=str, default="postgres")
    parser.add_argument("-port", "--port", type=str, default="5432")
    parser.add_argument("-data", "--data", type=str, default="test")
    parser.add_argument("-in", "--file_name", type=str, default="tests/test_data_2.csv")
    parser.add_argument("-K", "--K", type=int, default=1)
    parser.add_argument("-N", "--N", type=int, default=1)
    parser.add_argument("-outdir", "--outdir", type=str, default="out/")
    parser.add_argument("-dmeasure", "--dmeasure", type=str, default="arithmetic")
    parser.add_argument("-policy", "--policy", type=str, default="density")
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
    if args.data == "test":
        dcube_test(args.dbname, args.user, args.port, args.file_name, args.K, args.N,
                dmeasure=args.dmeasure, policy=args.policy, outdir=args.outdir)
    else:
        dcube_realdata(args.dbname, args.user, args.port, args.file_name, args.K, args.data,
                dmeasure=args.dmeasure, policy=args.policy, outdir=args.outdir)


