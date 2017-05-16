#################################################
## D-CUBE
## using "Copy" impelemntation
##
## Dependency: Psycopg2 (Access PostgreSQL with Python)
##################################################

import psycopg2
import math
from dcube_utils import *


def dcube_copy(original_data_table, col_names, X_name, K, N, cur, 
            dmeasure="arithmetic", policy="cardinality", 
            outdir="out/", out_prefix="out",
            verbose=True,
            para_index=True, r_index=-1, b_index=0, Bn_index=True):
    """
    D-Cube algorithm. Find K dense blocks in a given tensor.
    Args:
        original_data_table: name of the data relation; it has N+1 columns, 
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
        verbose: whether to print the finded block to stdout
        para_index: whether to create a hash index for parameters
        r_index: create an index for the i-th attribute in data_table
        b_index: create an index for the i-th attribute in Btable
        Bn_index: whether to create index on Bn
    """
    ## initialization
    data_table = "mydata"
    init_dcube_tables(original_data_table, data_table, col_names, X_name, 
                        K, N, cur, para_index, r_index)
    # print ("\tStarted with %d entries." % compute_card(cur, data_table))

    ## repeatedly find dense sub-blocks
    for k in range(K):
        ## find single block
        print ("Start finding the %d-th block ..." % (k+1))
        find_single_block(data_table, col_names, X_name, N, cur, dmeasure, policy, 
                        b_index, Bn_index)

        save_block(original_data_table, col_names, X_name, 
                    k, N, cur, outdir, out_prefix, verbose)
   
        ## remove the entries in the found block from data table
        condition = " and ".join(("R.%s=B.%s" % (col_names[n], col_names[n])) for n in range(N))
        cur.execute(("DELETE FROM %s as R WHERE EXISTS " % data_table) + 
                ("(SELECT 1 FROM block%d as B WHERE %s);" % (k, condition)))
        ## update total mass
        cur.execute(("UPDATE parameters SET value=(SELECT sum(%s) FROM %s) " 
            % (X_name, data_table)) + " WHERE par='total_mass';" )

        ## if no entries or no mass are left in the table, stop the loop
        R_card = compute_card(cur, data_table)
        total_mass = get_parameter(cur, 'total_mass')
        # print ("\tAfter removing the block, %d entries are left." % R_card)
        if R_card == 0 or total_mass == 0:
            print ("Algorithm stopped after finding %d blocks because no mass is left." %
                (k+1))
            break

    ## clean up: drop the temporary tables
    clean_up(data_table, N, k, cur)


def find_single_block(data_table, col_names, X_name, N, cur, dmeasure, policy, 
            b_index=-1, Bn_index=True):
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
    init_B_tables(data_table, col_names, X_name, N, cur, b_index, Bn_index)
    (max_dens, avg_card) = compute_density(cur, N, dmeasure)
    curr_order, max_order, density = 1, 1, max_dens

    ## repeatedly remove all entries in Btable
    while has_remained_B(N, cur):
        ## select dimension to remove
        n_rm = select_dimension(cur, N, policy, dmeasure)
        B_rm = ("B%d" % n_rm)

        ## entries that have mass smaller than the average will be removed
        card = float(get_parameter(cur, par=("card_B%d" % n_rm)))
        B_mass = float(get_parameter(cur, par="B_mass"))
        avg_mass = B_mass/card

        ## "remove" these entries from Btable by setting mass=0
        cur.execute("UPDATE Btable SET %s=0 WHERE EXISTS " % (X_name)
            + ("(SELECT 1 FROM %s WHERE %s.mass <= %f and %s.value=Btable.%s);" % 
                (B_rm, B_rm, avg_mass, B_rm, col_names[n_rm])))

        ## repeatedly delete entries in B_rm
        curr_row = get_next_Brow(B_rm, cur)
        while curr_row and curr_row[1] <= avg_mass:
            (curr_value, curr_mass) = (curr_row[0], float(curr_row[1]))

            ## record the delete order
            cur.execute("INSERT INTO rmOrder (value, dimension, r) " + 
                    "VALUES ('%s', %d, %d);" % (curr_value, n_rm, curr_order))
            curr_order += 1
            
            ## remove this entry from B_rm
            cur.execute("DELETE FROM %s WHERE value='%s';" % (B_rm, curr_value))

            ## new density, cardinality, and B_mass
            (density, card, B_mass, avg_card) = update_density(density, N, cur, dmeasure, 
                                                    B_rm, B_mass, card, curr_mass, avg_card)
            ## update maximal density
            if density > max_dens:
                max_dens, max_order = density, curr_order
            ## move to the next row
            curr_row = get_next_Brow(B_rm, cur)

        ## update mass and cardinality in the "parameters" relation
        cur.execute("UPDATE parameters SET value=%f WHERE par='card_%s';" % (card, B_rm))
        cur.execute("UPDATE parameters SET value=%f WHERE par='B_mass';" % B_mass)

        ## because we have removed entries from Btable, we need to re-compute the mass
        recompute_Bmass(col_names, X_name, N, cur, Bn_index, n_rm)
   
    ## reconstruct the dense block
    for n in range(N):
        cur.execute(("INSERT INTO final_B%d (value) " % n) 
             + ("(SELECT R.value as value FROM R%d as R, rmOrder " % (n))
             + ("WHERE R.value=rmOrder.value and rmOrder.dimension=%d and rmOrder.r >= %d);" 
                % (n, max_order)))

    ## clean up: drop the temporary table
    cur.execute("DROP TABLE Btable;")


def update_density(density, N, cur, dmeasure, 
            B_rm, B_mass, card, curr_mass, avg_card):
    """
    Update the density after removing 1 element from B_rm.
    Args:
        density: current density
        N: number of dimension
        cur: cursor of database connection
        dmeasure: "arithmetic" or "geometric" or "suspicious", density measure
        B_rm: the current Bn that are removed
        B_mass: current total mass in B
        card: current cardinality of B
        curr_mass: the mass to be removed
        avg_card: used for arithmetic dmeasure, average cardinality of all Bn's
    """
    if dmeasure == "suspicious":
        ## update mass and cardinality to compute the density after deleting
        cur.execute("UPDATE parameters SET value=value-1 WHERE par='card_%s';" % B_rm)
        cur.execute("UPDATE parameters SET value=value-%f WHERE par='B_mass';" % curr_mass)
        ## density after deleting
        density, _ = compute_density(cur, N, dmeasure)
    ## for the other two density measures, we have simplier updating method
    elif dmeasure == "geometric":
        if B_mass == 0 or card <= 1:
            density = -1
        else:
            density *= (B_mass - curr_mass) / B_mass
            density *= math.pow(card, 1.0/N)/math.pow(card-1, 1.0/N)     
    elif dmeasure == "arithmetic":
        if B_mass == 0 or avg_card <= 1.0/N:
            density = -1
        else:
            density *= (B_mass - curr_mass) / B_mass
            density *= avg_card/(avg_card - 1.0/N)
        avg_card -= 1.0/N
    card -= 1
    B_mass -= curr_mass

    return (density, card, B_mass, avg_card)


def clean_up(data_table, N, k, cur):
    """
    Clean up the temporary tables.
    Args:
        data_table: the copy of data
        N: the dimension
        k: the number of found block - 1
    """
    cur.execute("DROP TABLE %s;" % data_table)
    cur.execute("DROP TABLE parameters;")
    cur.execute("DROP TABLE new_B;")
    cur.execute("DROP TABLE rmOrder;")
    for n in range(N):
        cur.execute("DROP TABLE R%d;" % n)
        cur.execute("DROP TABLE B%d;" % n)
        cur.execute("DROP TABLE final_B%d;" % n)
    for kk in range(k+1):
        cur.execute("DROP TABLE block%d;"%kk)


def save_block(original_data_table, col_names, X_name, 
            k, N, cur, outdir, out_prefix, verbose):
    """
    Construct and save the k-th block.
    Args:
        original_data_table: name of the data relation; it has N+1 columns, 
                where the first N columns for the N dimensions with names col_names,
                and the last column specifying the measure attribute with name X_name.
        col_names: column names of the relation for the N dimensions
        X_name: column name of the measure attribute
        k: the current block to construct
        N: number of dimension
        cur: cursor of database connection
        outdir: output directory
        out_prefix: prefix of the output results
        verbose: whether to print the finded block to stdout
    """
    columns = ",".join(col_names + [X_name])
    ## cunstruct the dense block
    condition = " and ".join([("R.%s=final_B%d.value" % (col_names[n], n)) for n in range(N)])
    final_Bs = ",".join(["final_B"+str(n) for n in range(N)])
    cur.execute(("CREATE TABLE block%d AS " % k) 
        + ("SELECT %s FROM %s as R, %s " %  (columns, original_data_table, final_Bs)) 
        + ("WHERE %s;" % condition))

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



def init_B_tables(data_table, col_names, X_name, N, cur, 
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
    cur.execute("CREATE TABLE Btable AS SELECT * FROM %s;" % data_table)
    if b_index >= 0 and b_index < N:
        cur.execute("CREATE INDEX ON Btable (%s);" % (col_names[b_index]))
        # print ("\tcreated index on Btable (%s)" % col_names[b_index])

    ## total mass in Btable; initially equals to current total mass
    update_parameter(cur, 'B_mass', get_parameter(cur, 'total_mass'))

    ## clean up temporary tables
    cur.execute("DELETE FROM rmOrder;")
    for n in range(N):
        cur.execute("DELETE FROM final_B%d;" % n)
        cur.execute("DELETE FROM B%d;" % n)
    ## Bn (value, mass)
    for n in range(N):
        cur.execute(("INSERT INTO B%d (value, mass) " % n)
            + ("(SELECT %s as value, sum(%s) as mass " % (col_names[n], X_name))
            + ("FROM Btable GROUP BY value ORDER BY mass);"))
        ## record its cardinality
        update_parameter(cur, 'card_B%d' % n, compute_card(cur, "B%d" % n))
        ## create index
        if Bn_index:
            cur.execute("CREATE INDEX ON B%d (value);" % n)


def recompute_Bmass(col_names, X_name, N, cur, Bn_index, n_rm):
    """
    After deleting some entries from Btable, 
    we need to re-compute the mass for each Bn=a.
    Args:
        col_names: list of column names, with length N
        X_name: column name of the measure attribute
        N: number of dimensions
        cur: cursor for database connection
        Bn_index: whether to create index on Bn
        n_rm: the current dimension that the algorithm is removing; 
              this Bn does not need to be updated. 
    """
    for n in range(N):
        if n != n_rm:
            ## it's faster to delete and re-insert than updating the old one
            cur.execute("DELETE FROM new_B;")
            cur.execute("INSERT INTO new_B (value, mass) " 
                 + ("(SELECT value, sum(%s) as mass " % (X_name))
                 + ("FROM Btable JOIN B%d ON Btable.%s=B%d.value " % (n, col_names[n], n))
                 + ("GROUP BY value ORDER BY mass);"))
            cur.execute("DELETE FROM B%d;" % n)
            cur.execute("INSERT INTO B%d (value, mass) (SELECT * FROM new_B);" % n)

            ## update its cardinality
            update_parameter(cur, 'card_B%d' % n, compute_card(cur, "B%d" % n))


def get_next_Brow(Bn, cur):
    """
    Get the top row in table Bn, which has the smallest mass.
    Args:
        Bn: table name, one of B0 ... B(N-1)
        cur: cursor for database connection
    """
    cur.execute("SELECT value, mass FROM %s LIMIT 1;" % Bn)
    return cur.fetchone()


def init_dcube_tables(original_data_table, data_table, col_names, X_name, 
                    K, N, cur, para_index, r_index):
    """
    Initialize the tables for D-cube.
    Args:
        original_data_table: name of the data relation; it has N+1 columns, 
                where the first N columns for the N dimensions with names col_names,
                and the last column specifying the measure attribute with name X_name.
        data_table: name of the relational table storing data
        col_names: column names of the relation for the N dimensions
        X_name: column name of the measure attribute
        K: number of blocks
        N: number of dimension
        cur: cursor of database connection
        para_index: whether to create index for the 'parameters' relation
        r_index: create an index for the i-th attribute in data_table
    """
    ## copy the original data table; the new data_table will be modified
    cur.execute("CREATE TABLE %s AS SELECT * FROM %s;" % (data_table, original_data_table))

    ## create index on the i-th attribute
    if r_index >= 0 and r_index < N:
        cur.execute("CREATE INDEX ON %s (%s);" % (data_table, col_names[r_index]))
        # print ("\tcreated index in Rtable on %s" % col_names[r_index])

    ## create a table for global parameters
    cur.execute("CREATE TABLE parameters (par varchar(40), value double precision);")
    cur.execute("INSERT INTO parameters (par) VALUES ('total_mass');")
    cur.execute("INSERT INTO parameters (par) VALUES ('B_mass');")
    for n in range(N):    
        cur.execute("INSERT INTO parameters (par) VALUES ('card_B%d');" % n)
        cur.execute("INSERT INTO parameters (par) VALUES ('card_R%d');" % n)
    ## create index
    if para_index:
        cur.execute("CREATE INDEX ON parameters (par);")
        # print "\tcreated index on parameters."
    ## total mass
    cur.execute(("UPDATE parameters SET value=(SELECT sum(%s) FROM %s) " 
                        % (X_name, data_table)) + " WHERE par='total_mass';" )

    ## create tables Rn to store the unique values in each dimension
    for n in range(N):
        cur.execute("CREATE TABLE R%d AS SELECT DISTINCT %s as value FROM %s;" % 
                    (n, col_names[n], data_table))
        update_parameter(cur, 'card_R%d' % n, compute_card(cur, "R"+str(n)))

    ## create temporary tables
    cur.execute("CREATE TABLE new_B (value varchar, mass double precision);")
    ## a table to keep track of the removal order of each entry
    cur.execute("CREATE TABLE rmOrder (value varchar, dimension int, r int);")
    ## the Bn(value, mass) used for find_single_block and final_Bn for found blocks
    for n in range(N):
        cur.execute("CREATE TABLE B%d (value varchar, mass double precision);" % n)
        cur.execute("CREATE TABLE final_B%d (value varchar);" % n)


