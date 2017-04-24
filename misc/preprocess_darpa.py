## Pre-process the darpa data for D-cube algorithm.

import psycopg2
import os, argparse

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
        ## create table
        if len(col_names) != len(col_fmts):
            raise ValueError("Length of col_names and col_fmts must be the same.")
        columns = [col_names[i]+" "+col_fmts[i] for i in range(len(col_names))]
        cur.execute("CREATE TABLE %s (%s);" % (table_name, ",".join(columns)))

    ## load data from file
    with open(file_name, mode="rt") as fin:
        cur.copy_from(fin, table_name, sep=sep)


def clean_darpa(dbname, user, port, file_name, outfile="darpa_cleaned.csv"):
    """
    Clean the DARPA data for D-cube algorithm.
    This includes: 
    (i) bucketize the time by date; 
    (ii) add an extra column of 1's for the measure attribute;
    (iii) only retain the distinct rows 
    Args:
        dbname: data base name
        user: user
        port: port number
        file_name: .csv file
        table_name: desired tablename
        K: number of blocks
        dmeasure: "arithmetic" or "geometric", density measure
        policy: "cardinality" or "density"
    """
    ## DARPA is a 3-way tensor: source ip, destination ip, date
    table_name = "darpa"
    N = 3
    col_names = ["src", "dst", "pdate"]
    col_fmts = ["varchar(40)"]*N
    X_name = "measure" ## name for measure attribute
    X_fmt = "double precision" ## type for measure attribute

    ## Connect to the database
    DSN = "dbname=%s user=%s port=%s host='/tmp/'" % (dbname, user, port)
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            ## load data from file; first load all data as string
            print ("Loading data from %s..." % file_name)
            load_table_from_file(table_name, col_names, col_fmts, file_name, cur)

            print "Cleaning data..."
            ## Convert the 3rd column to date (i.e., bucketize by dates)
            cur.execute(("UPDATE %s SET " % table_name) + 
                ("%s=to_char(to_date(%s, 'MM/DD/YYYY HH24:MI'), 'MM/DD/YYYY');" % 
                    (col_names[2], col_names[2])))

            ## keep distinct values only
            cur.execute(("DELETE FROM %s WHERE ctid NOT IN " % table_name) +
                ("(SELECT min(ctid) FROM %s " % table_name) + 
                ("GROUP  BY %s);" % ",".join(col_names)))

            ## the original file does not have attribute measure 
            ## so we also need to add another column with all 1's
            cur.execute("ALTER TABLE %s ADD COLUMN %s %s;" % (table_name, X_name, X_fmt))
            cur.execute("UPDATE %s SET %s = 1;" % (table_name, X_name))

            ## save to disk
            print "Saving cleaned data..."
            with open(outfile, mode="wt") as fout:
                cur.copy_to(fout, table_name, sep=',')
            print ("The pre-processed DARPA data is written to file '%s'." % outfile)

            ## clean up: drop the created data table
            cur.execute("DROP TABLE %s;" % table_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-db", "--dbname", type=str, default="test")
    parser.add_argument("-user", "--user", type=str, default="postgres")
    parser.add_argument("-port", "--port", type=str, default="5432")
    parser.add_argument("-in", "--file_name", type=str, default="data/darpa.csv")
    parser.add_argument("-out", "--outfile", type=str, default="data/darpa_cleaned.csv")
    args = parser.parse_args()

    clean_darpa(args.dbname, args.user, args.port, args.file_name, args.outfile)

