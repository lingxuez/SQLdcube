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

def load_table_from_file(table_name, col_names, col_fmts,
                        file_name, cur, sep=",", create=True):
    """
    Create table from files.
    Args:
        table_name: desired tablename
        col_names: column names of the table
        col_fmts: formats
        file_name: path to .csv file
        cur: cursor of database connection
        sep: "," for csv, " " or "\t" for txt
    """
    if create:
        ## create new table
        if len(col_names) != len(col_fmts):
            raise ValueError("Length of col_names and col_fmts must be the same.")
        columns = [col_names[i]+" "+col_fmts[i] for i in range(len(col_names))]
        cur.execute("CREATE TABLE %s (%s);" % (table_name, ",".join(columns)))

    ## load data from file
    with open(file_name, mode="rt") as fin:
        cur.copy_from(fin, table_name, sep=sep)


def dcube_run(dbname, user, port, file_name, 
                        table_name, K, N,
                        dmeasure="arithmetic", policy="cardinality",
                        outdir="out/"):
    """
    Dense subtensor mining using D-cube.
    Args:
        dbname: an existing database name
        user: user
        port: port number
        file_name: .csv file to load data
        table_name: desired tablename to store data
        K: number of blocks to detect
        dmeasure: "arithmetic" or "geometric" or "suspicious", density measure
        policy: "cardinality" or "density"
    """
    ## N-way tensor + measure
    col_names = ["D"+str(n) for n in range(N)]
    col_fmts = ["varchar(40)"] * N
    X_name = "measure" ## name for measure attribute
    X_fmt = "double precision" ## type for measure attribute

    ## Connect to the database
    DSN = "dbname=%s user=%s port=%s host='/tmp/'" % (dbname, user, port)
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            ## load data
            print ("Loading data from %s..." % file_name)
            load_table_from_file(table_name, 
                            col_names + [X_name], 
                            col_fmts + [X_fmt], 
                            file_name, cur)

            ## D-CUBE
            print "Performing D-Cube..."
            file_prefix=(file_name.rsplit("/")[len(file_name.rsplit("/"))-1]).split(".csv")[0]
            dcube(table_name, col_names, X_name, K, N, cur, dmeasure, policy,
                outdir=outdir, out_prefix=file_prefix)

            ## clean up: drop the created data table
            cur.execute("DROP TABLE %s;" % table_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-db", "--dbname", type=str, default="test")
    parser.add_argument("-user", "--user", type=str, default="postgres")
    parser.add_argument("-port", "--port", type=str, default="5432")
    parser.add_argument("-in", "--file_name", type=str, default="tests/test_data_2.csv")
    parser.add_argument("-tb", "--table_name", type=str, default="test_data")
    parser.add_argument("-K", "--K", type=int, default=2)
    parser.add_argument("-N", "--N", type=int, default=3)
    parser.add_argument("-outdir", "--outdir", type=str, default="out/")
    parser.add_argument("-dmeasure", "--dmeasure", type=str, default="arithmetic")
    parser.add_argument("-policy", "--policy", type=str, default="density")
    args = parser.parse_args()

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
    dcube_run(args.dbname, args.user, args.port, args.file_name, 
                            args.table_name, args.K, args.N, 
                            dmeasure=args.dmeasure, policy=args.policy,
                            outdir=args.outdir)


