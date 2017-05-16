#################################################
## D-CUBE
## using "Mark" impelemntation
##
## Dependency: Psycopg2 (Access PostgreSQL with Python)
#################################################

import psycopg2
import math
from dcube_utils import *


def dcube_mark(data_table, col_names, X_name, K, N, cur, 
            dmeasure="arithmetic", policy="cardinality", 
            outdir="out/", out_prefix="out", verbose=True,
            para_index=True, r_index=-1, b_index=-1, Bn_index=False):
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
        para_index: whether to create a hash index for parameters
        r_index: create an index for the i-th attribute in data_table
        b_index: create an index for the i-th attribute in Btable
        Bn_index: whether to create index on Bn
    """
    ## add a column to data table indicating whether entry has been removed
    cur.execute("ALTER TABLE %s ADD COLUMN exists int;" % data_table)
    cur.execute("UPDATE %s SET exists = 1;" % data_table)

    ## create index on the i-th attribute
    if r_index >= 0 and r_index < N:
        cur.execute("CREATE INDEX ON %s USING hash (%s);" % (data_table, col_names[r_index]))
        # print ("\tcreated index in Rtable on %s" % col_names[r_index])

    ## initialize needed tables
    init_dcube_tables_mark(data_table, col_names, X_name, K, N, cur, para_index)

    ## repeatedly find dense sub-blocks
    for k in range(K):
        ## update total mass according to current table
        cur.execute(("UPDATE parameters SET value=(SELECT sum(%s) FROM %s WHERE exists=1) " 
            % (X_name, data_table)) + " WHERE par='total_mass';" )

        total_mass = get_parameter(cur, 'total_mass')
        if total_mass == 0:
            print ("Algorithm stopped after finding %d blocks because no mass is left." %
                (k+1))
            break

        ## find single block
        print ("Start finding the %d-th block ..." % (k+1))
        find_single_block_mark(data_table, col_names, X_name, N, cur, dmeasure, policy, 
                        b_index, Bn_index)

        ## the found block
        cur.execute(("CREATE TABLE block%d AS " % k) + 
               ("SELECT %s FROM %s as R, %s " %  
                    (",".join(col_names + [X_name]), data_table,
                         ",".join(["final_B"+str(n) for n in range(N)]))) + 
               ("WHERE %s;" % 
                    " and ".join([("R.%s=final_B%d.value" % (col_names[n], n)) for n in range(N)])))

        ## print block results to stdout
        if verbose:
            print ("\tFound block %d:" % (k+1))
            for n in range(N):        
                print ("\tdimension %d:" % n)
                cur.execute("SELECT * FROM final_B%d;" % n)
                print "\t", cur.fetchall()
                
        ## save the block to disk
        outfile = outdir+"/"+out_prefix+"_block"+str(k+1)+".csv"
        with open(outfile, mode="wt") as fout:
            cur.copy_to(fout, ("block%d" % k), sep=',')
        print ("\tThe %d-th block is written to file '%s'." % (k+1, outfile))
  
        ## remove the entries in the found block from current table
        cur.execute(("UPDATE %s SET exists=0 WHERE EXISTS " % data_table) + 
            ("(SELECT 1 FROM block%d as B WHERE %s);" % 
                (k, " and ".join([("%s.%s=B.%s" % (data_table, col_names[n], col_names[n])) 
                                    for n in range(N)]))))

        ## clean up: drop the final_Bn and block-k tables
        for n in range(N):
            cur.execute("DROP TABLE final_B%d;" % n)
        cur.execute("DROP TABLE block%d;"%k)

        ## if nothing left, then stop
        R_card = compute_card_mark(cur, data_table)
        # print ("\tAfter removing the block, %d entries are left." % R_card)
        if R_card == 0:
            print ("Algorithm stopped after finding %d blocks because no entries are left." %
                (k+1))
            break

    ## clean up: drop the temporary tables
    cur.execute("DROP TABLE parameters;")
    for n in range(N):
        cur.execute("DROP TABLE R%d;" % n)



def find_single_block_mark(data_table, col_names, X_name, N, cur, dmeasure, policy, 
            b_index, Bn_index):
    """
    Args:
        data_table: the current relation table (the relation "R" in paper)
        col_names: lisf of column names of the relation, with length N
        X_name: column name of the measure attribute
        N: number of dimension
        cur: cursor of database connection
        dmeasure: "arithmetic" or "geometric" or "suspicious", density measure
        policy: "cardinality" or "density"
        b_index: create an index for the i-th attribute in Btable
        Bn_index: whether to create index on Bn
    """
    ## initialize
    init_B_tables_mark(data_table, col_names, X_name, N, cur, b_index, Bn_index)
    max_dens, _ = compute_density(cur, N, dmeasure)
    curr_order, max_order = 1, 1

    ## repeatedly remove all entries in Btable
    while has_remained_B(N, cur):
        ## select dimension to remove
        n_rm = select_dimension_mark(cur, N, policy, dmeasure)
        B_rm = ("B%d" % n_rm)

        ## entries that have mass smaller than the average will be removed
        card = get_parameter(cur, par=("card_B%d" % n_rm))
        B_mass = get_parameter(cur, par="B_mass")
        avg_mass = B_mass/float(card)

        ## repeatedly delete entries in B_rm
        curr_row = get_next_Brow_mark(B_rm, cur)
        while curr_row and curr_row[1] <= avg_mass:
            (curr_value, curr_mass) = curr_row

            ## record the delete order
            cur.execute("INSERT INTO rmOrder (value, dimension, r) " + 
                    "VALUES ('%s', %d, %d);" % (curr_value, n_rm, curr_order))
            curr_order += 1
            
            ## remove this entry
            cur.execute("UPDATE %s SET exists=0 WHERE value='%s';" % (B_rm, curr_value))
            cur.execute("UPDATE Btable SET %s=0 WHERE %s='%s';" % 
                        (X_name, col_names[n_rm], curr_value)) 

            ## update mass and cardinality to compute the density after deleting
            cur.execute("UPDATE parameters SET value=value-1 WHERE par='card_%s';" % B_rm)
            cur.execute("UPDATE parameters SET value=value-%f WHERE par='B_mass';" % curr_mass)
        
            ## density after deleting
            density, _ = compute_density(cur, N, dmeasure)
            if density > max_dens:
                max_dens, max_order = density, curr_order

            ## move to the next row
            curr_row = get_next_Brow_mark(B_rm, cur)

        ## because we have changed Btable, we need to re-compute the mass
        recompute_Bmass_mark(col_names, X_name, N, cur, Bn_index)
      
    ## reconstruct the dense block
    for n in range(N):
        cur.execute(("CREATE TABLE final_B%d AS SELECT R.value FROM R%d as R, rmOrder " % (n,n)) 
             + ("WHERE R.value=rmOrder.value and rmOrder.dimension=%d and rmOrder.r >= %d;" 
                % (n, max_order)))

    ## clean up: drop the temporary tables
    cur.execute("DROP TABLE Btable;")
    cur.execute("DROP TABLE rmOrder;")
    for n in range(N):
        cur.execute("DROP TABLE B%d;" % n)



def init_B_tables_mark(data_table, col_names, X_name, N, cur, 
            b_index, Bn_index):
    """
    Initialize and create the tables needed for find_single_block.
    Args:
        data_table: the table "R" in paper
        col_names: list of column names, with length N
        X_name: column name of the measure attribute
        N: number of dimensions
        cur: cursor for database connection
        b_index: create an index for the i-th attribute in Btable
        Bn_index: whether to create index on Bn
    """
    ## initialize the Btable; this table will be eliminated in the end
    cur.execute("CREATE TABLE Btable AS SELECT * FROM %s WHERE exists = 1;" % data_table)

    ## create index on the i-th attribute
    if b_index >= 0 and b_index < N:
        cur.execute("CREATE INDEX ON Btable (%s);" % (col_names[b_index]))
        # print ("\tcreated index on Btable (%s)" % col_names[b_index])

    ## total mass in Btable; initially equals to current total mass
    update_parameter(cur, 'B_mass', get_parameter(cur, 'total_mass'))

    ## a table to keep track of the removal order of each entry
    cur.execute("CREATE TABLE rmOrder (value varchar(80), dimension int, r int);")

    ## create tables Bn(value, mass) to store all values in Rn and the mass
    for n in range(N):
        cur.execute( ("CREATE TABLE B%d AS SELECT %s as value, exists, sum(%s) as mass " % 
            (n, col_names[n], X_name))
             + ("FROM Btable WHERE Btable.exists=1")
             + ("GROUP BY value, exists ORDER BY mass;"))
        ## record its cardinality
        update_parameter(cur, 'card_B%d' % n, compute_card_mark(cur, "B"+str(n)))
        ## create index
        if Bn_index:
            cur.execute("CREATE INDEX ON B%d (value);" % n)


def recompute_Bmass_mark(col_names, X_name, N, cur, Bn_index):
    """
    After updating Btable, we need to re-compute the mass for each Bn=a.
    Args:
        col_names: list of column names, with length N
        X_name: column name of the measure attribute
        N: number of dimensions
        cur: cursor for database connection
        Bn_index: whether to create index on Bn
    """
    for n in range(N):
        ## it's faster to re-create a new table than updating the old one
        cur.execute( ("CREATE TABLE new_B AS SELECT value, B%d.exists as exists, sum(%s) as mass " 
            % (n, X_name))
             + ("FROM Btable JOIN B%d ON Btable.%s=B%d.value WHERE Btable.exists=1" % 
                    (n, col_names[n], n))
             + ("GROUP BY value, B%d.exists ORDER BY mass;" % n))
        cur.execute("DROP TABLE B%d;" % n)
        cur.execute("CREATE TABLE B%d AS SELECT * FROM new_B;" % n)
        cur.execute("DROP TABLE new_B;")
        ## create index
        if Bn_index:
            cur.execute("CREATE INDEX ON B%d (value);" % n)


def init_dcube_tables_mark(data_table, col_names, X_name, K, N, cur, para_index):
    """
    Initialize the tables for D-cube.
    Args:
        data_table: name of the relational table storing data
        col_names: column names of the relation for the N dimensions
        X_name: column name of the measure attribute
        K: number of blocks
        N: number of dimension
        cur: cursor of database connection
        para_index: whether to create index for parameters
    """
    ## create tables Rn to store the unique values in each dimension
    for n in range(N):
        cur.execute("CREATE TABLE R%d AS SELECT DISTINCT %s as value FROM %s; " % 
                    (n, col_names[n], data_table))

    ## create a table for global parameters
    cur.execute("CREATE TABLE parameters (par varchar(80), value double precision);")
    ## total Mass M_R and M_B
    cur.execute("INSERT INTO parameters (par) VALUES ('total_mass');")
    cur.execute("INSERT INTO parameters (par) VALUES ('B_mass');")
    ## create fields to store cardinality for B_1 ... B_N and R_1 ... R_N
    for n in range(N):    
        cur.execute("INSERT INTO parameters (par) VALUES ('card_B%d');" % n)
        cur.execute("INSERT INTO parameters (par) VALUES ('card_R%d');" % n)

    ## create index
    if para_index:
        cur.execute("CREATE INDEX ON parameters (par);")
        # print "\tcreated hash index on parameters."

    ## store the cardinality of R_1 ... R_N; these remain unchanged.
    for n in range(N):
        cur.execute("SELECT count(*) FROM (SELECT 1 FROM R%d) As t;" % n)
        card = cur.fetchone()[0]
        update_parameter(cur, ('card_R%d' % n), card)


