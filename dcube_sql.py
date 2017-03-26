## A PostgreSQL implementation of D-CUBE
##
## Dependency: Psycopg2 (Access PostgreSQL with Python)

import psycopg2
import math


def compute_density(cur, N, dmeasure):
    """
    Args:
        cur: cursor of database connection
        N: number of dimensions
        dmeasure: "arithmetic" or "geometric", density measure
    """
    if dmeasure == "arithmetic":
        ## arithmetic average of cardinalities of B_n's
        avg_card = 0
        for n in range(N):
            card = get_parameter(cur, par=("card_B%d" % n))
            avg_card += card / float(N)
    elif dmeasure == "geometric":
        ## geometric average of cardinalities of B_n's
        avg_card = 1
        for n in range(N):
            card = get_parameter(cur, par=("card_B%d" % n))
            avg_card *= card
        avg_card = math.pow(avg_card, 1.0/N)
        
    if avg_card == 0:
        return -1
    else:
        return get_parameter(cur, par="B_mass") / avg_card

#TODO 
def select_dimension(cur, N, policy):
    """
    Args:
        cur: cursor of database connection
        N: number of total dimensions
        policy: "cardinality" or "density"
    """

    if policy == "cardinality":
        ## policy 1: largest cardinality
        max_dim, max_card = -1, -1
        for n in range(N):
            ## cardinality of B_n
            card = get_parameter(cur, par=("card_B%d" % n))
            if card > max_card:
                max_dim, max_card = n, card
        return max_dim

    # TODO
    else:
        ## policy 2: by density
        return 0


def compute_card(cur, table_name):
    """
    Compute the cardinality of a table.
    """
    cur.execute("SELECT count(*) FROM (SELECT 1 FROM %s) As t;" % table_name)
    return cur.fetchone()[0]


def get_parameter(cur, par):
    """
    Extrat the global parameter from the table "parameters".
    Args:
        cur: cursor of database connection
        par: parameter name
    """
    cur.execute("SELECT value FROM parameters WHERE par='%s';" % par)
    return cur.fetchone()[0]


def has_remained_B(N, cur):
    """
    Check whether all B_1, ..., B_N are empty. Return True if some are non-empty.
    Args:
        N: number of dimensions
        cur: cursor for database connection
    """
    for n in range(N):
        ## cardinality of B_n
        card = get_parameter(cur, par=("card_B%d" % n))
        if card > 0:
            return True
    return False


def init_B_tables(table_curr, col_names, X_name, N, cur):
    """
    Initialize and create the tables needed for find_single_block.
    Args:
        table_curr: the table "R" in paper
        col_names: list of column names, with length N
        X_name: column name of the measure attribute
        N: number of dimensions
        cur: cursor for database connection
    """
    ## initialize the Btable; this table will be eliminated in the end
    cur.execute("CREATE TABLE Btable AS SELECT * FROM %s;" % table_curr)

    ## total mass in Btable; initially equals to current total mass
    cur.execute(("UPDATE parameters SET value=%f " % get_parameter(cur, 'total_mass'))+ 
                "WHERE par='B_mass';")

    ## a table to keep track of the removal order of each entry
    cur.execute("CREATE TABLE rmOrder (value varchar(20), dimension int, r int);")

    ## create tables Bn to store all values in Rn and their masses in each dimension
    ## note that if a value is missing in Btable, we treat its mass as zero
    for n in range(N):
        cur.execute(("CREATE TABLE B%d AS SELECT value, sum(measure) as mass FROM " % n) 
             + ("(SELECT R%d.value as value, COALESCE(Btable.%s, 0) as measure " % (n, X_name))
             + ("FROM Btable RIGHT JOIN R%d ON Btable.%s=R%d.value) AS t " % (n, col_names[n], n))
             + ("GROUP BY value;"))

        # print ("B%d" % n)
        # cur.execute("SELECT * FROM B%d;" % n)
        # print cur.fetchall()

        ## record its cardinality
        cur.execute(("UPDATE parameters SET value=%d " % compute_card(cur, "B"+str(n)))
                  + ("WHERE par='card_B%d';" % n))

    # cur.execute(("SELECT COALESCE(Btable.%s, 0) as Quantity, R%d.value as value " % (X_name, n))
    #          + ("FROM Btable RIGHT JOIN R%d ON Btable.%s=R%d.value " % (n, col_names[n], n)))
    # print cur.fetchall()



def recompute_Bmass(col_names, X_name, N, cur):
    """
    After updating Btable, we need to re-compute the mass for each Bn=a.
    Args:
        col_names: list of column names, with length N
        X_name: column name of the measure attribute
        N: number of dimensions
        cur: cursor for database connection
    """
    for n in range(N):
        # cur.execute(("CREATE TABLE new_B AS SELECT %s as value, sum(%s) as mass " 
        #                 % (col_names[n], X_name)) 
        #      + ("FROM Btable, B%d WHERE Btable.%s=B%d.value " % (n, col_names[n], n))
        #       + ("GROUP BY %s;" % col_names[n]))

        # print ("old B%d" % n)
        # cur.execute("SELECT * FROM B%d;" % n)
        # print cur.fetchall()

        ## it's faster to re-create a new table than updating the old one
        ## the cardinality is the same as the old Bn
        ## if a value is missing in Btable, we treat its mass as zero
        cur.execute("CREATE TABLE new_B AS SELECT value, sum(measure) as mass FROM " 
             + ("(SELECT B%d.value as value, COALESCE(Btable.%s, 0) as measure " % (n, X_name))
             + ("FROM Btable RIGHT JOIN B%d ON Btable.%s=B%d.value) AS t " % (n, col_names[n], n))
             + ("GROUP BY value;"))
        cur.execute("DROP TABLE B%d;" % n)
        cur.execute("CREATE TABLE B%d AS SELECT * FROM new_B;" % n)
        cur.execute("DROP TABLE new_B;")

        # print ("new B%d" % n)
        # cur.execute("SELECT * FROM B%d;" % n)
        # print cur.fetchall()


def get_next_Brow(Bn, cur):
    """
    Get the next row in table Bn with the smallest mass.
    Args:
        Bn: table name, one of B0 ... B(N-1)
        cur: cursor for database connection
    """
    cur.execute("SELECT value, mass FROM %s ORDER BY mass LIMIT 1;" % Bn)
    return cur.fetchone()


# def del_entry_from_Btable(col_names, N, n_rm, value_rm, cur):
#     """
#     When we delete the entries with Bn==value,
#     we need to update Btable, as well as other Bn's.
#     Note that Btable may not be the full span of the tensor,
#     so we need to do it more carefully.
#     """
#     print ("Deleting dimension %d, value %s" % (n_rm, value_rm))

#     ## delete from other Bn's
#     for n in range(N):
#         ## for debug
#         cur.execute("SELECT DISTINCT %s FROM Btable WHERE Btable.%s = '%s';" % 
#                     (col_names[n], col_names[n_rm], value_rm))
#         print ("delete following values from B%d" % n)
#         print cur.fetchall()

#         cur.execute(("DELETE FROM B%d AS B WHERE B.value IN " % n) +
#                     "(SELECT DISTINCT %s FROM Btable WHERE Btable.%s = '%s');" % 
#                     (col_names[n], col_names[n_rm], value_rm))

#         print ("B%d becomes:" % n)
#         cur.execute("SELECT * FROM B%d;" % n)
#         print cur.fetchall()

#     ## delete from Btable
#     cur.execute("DELETE FROM Btable WHERE %s='%s';" % (col_names[n_rm], value_rm))

#     ## update the cardinality
#     for n in range(N):
#         cur.execute("UPDATE parameters "+ 
#                 ("SET value=(%f) " % compute_card(cur, "B"+str(n))) + 
#                 ("WHERE par='card_B%d';"% n))

#     cur.execute("SELECT * FROM parameters;")
#     cur.fetchall()


def find_single_block(table_curr, col_names, X_name, N, cur, dmeasure, policy):
    """
    Args:
        table_curr: the current relation table (the relation "R" in paper)
        col_names: lisf of column names of the relation, with length N
        X_name: column name of the measure attribute
        N: number of dimension
        cur: cursor of database connection
        dmeasure: "arithmetic" or "geometric", density measure
        policy: "cardinality" or "density"
    """
    ## initialize
    init_B_tables(table_curr, col_names, X_name, N, cur)
    max_dens = compute_density(cur, N, dmeasure)
    curr_order, max_order = 1, 1

    # ## for debug
    # cur.execute("SELECT * FROM parameters;")
    # print "Initial parameters:"
    # print cur.fetchall()

    ## repeatedly remove all entries in Btable
    while has_remained_B(N, cur):
        ## select dimension to remove
        n_rm = select_dimension(cur, N, policy)
        B_rm = ("B%d" % n_rm)

        # print ("Removing dimension %d" % n_rm)
        # print ("current best order is %d" % max_order)

        ## entries that have mass smaller than the average will be removed
        card = get_parameter(cur, par=("card_B%d" % n_rm))
        B_mass = get_parameter(cur, par="B_mass")
        avg_mass = B_mass/float(card)
        # print "avg_mass=", avg_mass

        ## repeatedly delete entries in B_rm
        ## Note: this chunk of code is very inefficient!!!
        curr_row = get_next_Brow(B_rm, cur)
        while curr_row and curr_row[1] <= avg_mass:
            (curr_value, curr_mass) = curr_row

            ## record the delete order
            cur.execute("INSERT INTO rmOrder (value, dimension, r) " + 
                    "VALUES ('%s', %d, %d)" % (curr_value, n_rm, curr_order))
            curr_order += 1
            
            cur.execute("DELETE FROM %s WHERE value='%s'" % (B_rm, curr_value))
            cur.execute("UPDATE Btable SET %s=0 WHERE %s='%s';" % (X_name, col_names[n_rm], curr_value)) 

            ## update mass and cardinality to compute the density after deleting
            cur.execute("UPDATE parameters SET value=value-1 WHERE par='card_%s';" % B_rm)
            cur.execute("UPDATE parameters SET value=value-%f WHERE par='B_mass';" % curr_mass)
            
            ## density after deleting
            density = compute_density(cur, N, dmeasure)
            # print "density, max_dens =", (density, max_dens)
            if density > max_dens:
                max_dens, max_order = density, curr_order
                # print "updated max_dens, max_order: ", (max_dens, max_order)

            ## delete these entries from Btable as well as other Bn's
            # del_entry_from_Btable(col_names, N, n_rm, curr_value, cur)

            ## move to the next row
            # print ("Removed (value=%s, mass=%f)" % (curr_value, curr_mass))
            curr_row = get_next_Brow(B_rm, cur)
        

        # # for debug
        # print ("after removing dimension %d:" % n_rm)
        # cur.execute("SELECT * FROM parameters;")
        # print cur.fetchall()

        # print ("Now Btabls is:")
        # cur.execute("SELECT * FROM Btable;")
        # print cur.fetchall()

        ## because we have changed Btable, we need to re-compute the mass
        recompute_Bmass(col_names, X_name, N, cur)

        # ## for debug
        # print ("after updating Btable:")
        # cur.execute("SELECT * FROM parameters;")
        # print cur.fetchall()
      
    ## reconstruct the dense block
    # print "best order is ", max_order
    # print "table rmOrder:"
    # cur.execute("SELECT * FROM rmOrder;")
    # print cur.fetchall()

    for n in range(N):
        cur.execute(("CREATE TABLE final_B%d AS SELECT R.value FROM R%d as R, rmOrder " % (n,n)) 
             + ("WHERE R.value=rmOrder.value and rmOrder.dimension=%d and rmOrder.r >= %d;" 
                % (n, max_order)))

        # print ("table final_B%d" % n)
        # cur.execute("SELECT * FROM final_B%d" % n)
        # print cur.fetchall()

    ## clean up: drop the created temporary tables
    cur.execute("DROP TABLE Btable, rmOrder, %s;" % 
                    (",".join(["B"+str(n) for n in range(N)])))


def init_dcube_tables(table_name, table_curr, col_names, X_name, K, N, cur):

    ## make a copy of the original table to avoid changing the original data table; 
    ## the copied table will be changed.
    cur.execute("CREATE TABLE %s AS SELECT * FROM %s;" % (table_curr, table_name))

    ## create tables Rn to store the unique values in each dimension
    for n in range(N):
        cur.execute("CREATE TABLE R%d AS SELECT DISTINCT %s as value FROM %s " % 
                    (n, col_names[n], table_curr))
        ## debug
        # cur.execute("SELECT * FROM R%d;" % n)
        # print cur.fetchall()[:10]

    ## create a table for global parameters
    cur.execute("CREATE TABLE parameters (par varchar(20), value double precision);")
    ## total Mass M_R and M_B
    cur.execute("INSERT INTO parameters (par) VALUES ('total_mass');")
    cur.execute("INSERT INTO parameters (par) VALUES ('B_mass');")
    ## cardinality for B_1 ... B_N
    for n in range(N):    
        cur.execute("INSERT INTO parameters (par) VALUES ('card_B%d');" % n)


def dcube(table_name, col_names, X_name, K, N, cur, 
            dmeasure="arithmetic", policy="cardinality"):
    """
    Args:
        table_name: name of relation
        col_names: column names of the relation for the N dimensions
        X_name: column name of the measure attribute
        K: number of blocks
        N: number of dimension
        cur: cursor of database connection
        dmeasure: "arithmetic" or "geometric", density measure
        policy: "cardinality" or "density"
    """
    ## initialize needed tables
    table_curr = "ctable"
    init_dcube_tables(table_name, table_curr, col_names, X_name, K, N, cur)

    ## repeatedly find dense sub-blocks
    for k in range(K):
        ## update total mass according to current table
        cur.execute(("UPDATE parameters SET value=(SELECT sum(%s) FROM %s) " % (X_name, table_curr)) + 
                    " WHERE par='total_mass';" )
        # ## debug:
        # cur.execute("SELECT * FROM parameters;")
        # print cur.fetchall()

        ## find single block
        find_single_block(table_curr, col_names, X_name, N, cur, dmeasure, policy)

        ## the found block
        cur.execute(("CREATE TABLE block%d AS " % k) + 
                    ("SELECT %s FROM %s as R, %s " % 
                                (",".join(col_names), table_name, ## note: original R
                                ",".join(["final_B"+str(n) for n in range(N)]))) + 
                    ("WHERE %s;" % 
                    " and ".join([("R.%s=final_B%d.value" % (col_names[n], n)) for n in range(N)]))
                    ) 
        
        print ("Found block %d:" % k)
        for n in range(N):        
            print ("\tdimension %d:" % n)
            cur.execute("SELECT * FROM final_B%d" % n)
            print "\t", cur.fetchall()

        ## remove the entries in the found block from current table
        cur.execute(("DELETE FROM %s as R WHERE EXISTS " % table_curr) + 
            ("(SELECT 1 FROM block%d as B WHERE %s);" % 
                (k, " and ".join([("R.%s=B.%s" % (col_names[n], col_names[n])) 
                                    for n in range(N)]))))

        ## clean up: drop the final_Bn tables
        cur.execute("DROP TABLE %s;" % 
                        (",".join(["final_B"+str(n) for n in range(N)])))
        cur.execute("DROP TABLE block%d;"%k)


    ## clean up: drop the created temporary tables
    cur.execute("DROP TABLE %s, parameters;" % table_curr)
    cur.execute("DROP TABLE %s;" % ",".join(["R"+str(n) for n in range(N)]))


