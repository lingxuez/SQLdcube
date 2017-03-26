## D-Cube

import argparse
from dcube_sql import *


def load_table_from_file(table_name, col_names, col_fmts,
                        file_name, cur, sep=",", create=True):
    """
    Create table from files.
    Bucketize time by hours/days.
    Args:
        table_name: desired tablename
        table_cols: a list of column names and formats. 
                Example: ["source varchar(20)", "dest varchar(20)", "date varchar(20)"]
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


# def dcube_darpa(dbname, user, port, file_name, 
#                         table_name, K,
#                         dmeasure="arithmetic", policy="cardinality"):
#     """
#     Dense subtensor mining on the DARPA data.
#     Args:
#         dbname: data base name
#         user: user
#         port: port number
#         file_name: .csv file
#         table_name: desired tablename
#         K: number of blocks
#         dmeasure: "arithmetic" or "geometric", density measure
#         policy: "cardinality" or "density"
#     """
#     ## 3-way tensor: source ip, destination ip, date
#     N=3
#     col_names = ["src", "dst", "date"]
#     col_fmts = ["varchar(40)"]*N

#     ## Connect to the database
#     DSN = "dbname=%s user=%s port=%s" % (dbname, user, port)
#     with psycopg2.connect(DSN) as conn:
#         with conn.cursor() as cur:
#             ## load data
#             print ("Loading data from %s..." % file_name)
#             load_table_from_file(table_name, col_names, col_fmts, file_name, cur)

#             ## D-CUBE
#             print "Performing D-Cube..."
#             dcube(table_name, col_names, K, N, cur, dmeasure, policy)

#             ## clean up: drop the created data table
#             cur.execute("DROP TABLE %s;" % table_name)



def dcube_test(dbname, user, port, file_name, 
                        table_name, K,
                        dmeasure="arithmetic", policy="cardinality"):
    """
    Dense subtensor mining on the DARPA data.
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
    ## 3-way tensor + measure
    N=3
    col_names = ["D1", "D2", "D3"]
    col_fmts = ["varchar(40)"] * N
    X_name = "measure" ## name for measure attribute
    X_fmt = "double precision" ## type for measure attribute

    ## Connect to the database
    DSN = "dbname=%s user=%s port=%s" % (dbname, user, port)
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
            dcube(table_name, col_names, X_name, K, N, cur, dmeasure, policy)

            ## clean up: drop the created data table
            cur.execute("DROP TABLE %s;" % table_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-db", "--dbname", type=str, default="test")
    parser.add_argument("-user", "--user", type=str, default="postgres")
    parser.add_argument("-port", "--port", type=str, default="5432")
    parser.add_argument("-f", "--file_name", type=str, default="tests/test_data.csv")
    parser.add_argument("-tb", "--table_name", type=str, default="test_data")
    parser.add_argument("-K", "--K", type=int, default=2)
    args = parser.parse_args()

    ## D-cube
    # dcube_darpa(args.dbname, args.user, args.port, args.file_name, 
    #                         args.table_name, args.K, 
    #                         dmeasure="geometric", policy="cardinality")
    dcube_test(args.dbname, args.user, args.port, args.file_name, 
                            args.table_name, args.K, 
                            dmeasure="geometric", policy="cardinality")

    # ## Connect to the database
    # DSN = "dbname=%s user=%s port=%s" % (args.dbname, args.user, args.port)
    # with psycopg2.connect(DSN) as conn:
    #     with conn.cursor() as cur:
    #         ## D-cube
    #         dcube_darpa(args.file_name, args.table_name, K=1, cur=cur)
    #         ## drop the data tables
    #         cur.execute("DROP TABLE %s;" % args.table_name)


