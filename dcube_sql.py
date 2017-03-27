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
        dmeasure: "arithmetic" or "geometric" or "suspicious", density measure
    """
    if dmeasure == "arithmetic":
        return compute_density_ari(cur, N)

    elif dmeasure == "geometric":
        return compute_density_geo(cur, N)

    elif dmeasure == "suspicious":
        return compute_density_susp(cur, N)


def compute_density_susp(cur, N):
    """
    Compute density using the suspiciousness
    Args:
        cur: cursor of database connection
        N: number of dimensions
    """
    ## first compute \prod_n |B_n|/|R_n|
    product = 1
    for n in range(N):
        B_card = get_parameter(cur, par=("card_B%d" % n))
        R_card = get_parameter(cur, par=("card_R%d" % n))
        product *= (B_card / float(R_card))

    ## suspiciousness
    B_mass = get_parameter(cur, par="B_mass")
    R_mass = get_parameter(cur, par="total_mass")
    if product == 0 or B_mass == 0 or R_mass == 0: 
        return -1
    else:
        susp = (B_mass * (math.log(B_mass/float(R_mass)) - 1) + 
                R_mass * product - B_mass * math.log(product))
        return susp


def compute_density_ari(cur, N):
    """
    Compute density using the arithmetic average mass
    Args:
        cur: cursor of database connection
        N: number of dimensions
    """
    ## arithmetic average cardinality
    avg_card = 0
    for n in range(N):
        card = get_parameter(cur, par=("card_B%d" % n))
        avg_card += card / float(N)

    ## average mass
    if avg_card == 0:
        return -1
    else:
        return get_parameter(cur, par="B_mass") / avg_card


def compute_density_geo(cur, N):
    """
    Compute density using the geometric average mass
    Args:
        cur: cursor of database connection
        N: number of dimensions
    """
    ## geometric average cardinality
    avg_card = 1
    for n in range(N):
        card = get_parameter(cur, par=("card_B%d" % n))
        avg_card *= card
    avg_card = math.pow(avg_card, 1.0/N)

    ## average mass
    if avg_card == 0:
        return -1
    else:
        return get_parameter(cur, par="B_mass") / avg_card


def select_dim_by_card(cur, N):
    """
    Select the next dimension to remove with the largest cardinality.
    Args:
        cur: cursor of database connection
        N: number of total dimensions
    """
    max_dim, max_card = -1, -1
    for n in range(N):
        ## cardinality of B_n
        card = get_parameter(cur, par=("card_B%d" % n))
        if card > max_card:
            max_dim, max_card = n, card
    return max_dim


def select_dim_by_dens(cur, N, dmeasure):
    """
    Select the next dimension to remove by density.
    Args:
        cur: cursor of database connection
        N: number of total dimensions
        dmeasure: "arithmetic" or "geometric" or "suspicious", density measure
    """
    max_dens, max_dim = -10, 0 ## note: by construction density is always >= -1
    B_mass = get_parameter(cur, par="B_mass")

    for n in range(N):
        card = get_parameter(cur, par=("card_B%d" % n))
        if card > 0:
            ## what will the density be if we remove all values with mass <= average
            avg_mass = B_mass/float(card)
            cur.execute("SELECT sum(mass), count(*) FROM B%d WHERE mass <= %f;" % (n, avg_mass))
            (rm_mass, rm_card) = cur.fetchone()

            update_parameter(cur, ("card_B%d" % n), card - rm_card)
            update_parameter(cur, "B_mass", B_mass - rm_mass)
            density = compute_density(cur, N, dmeasure)
            if density > max_dens:
                max_dim, max_dens = n, density

            ## need to change the parameters back
            update_parameter(cur, ("card_B%d" % n), card)
            update_parameter(cur, "B_mass", B_mass)

    return max_dim



def select_dimension(cur, N, policy, dmeasure):
    """
    Args:
        cur: cursor of database connection
        N: number of total dimensions
        policy: "cardinality" or "density"
    """
    if policy == "cardinality":
        return select_dim_by_card(cur, N)
    elif policy == "density":
        return select_dim_by_dens(cur, N, dmeasure)
    else:
        raise ValueError("policy must be one of 'cardinality' or 'density'.")


def compute_card(cur, table_name):
    """
    Compute the cardinality of a table.
    Args:
        cur: cursor of database connection
        table_name: name of the target table
    """
    cur.execute("SELECT count(*) FROM (SELECT 1 FROM %s) As t;" % table_name)
    return cur.fetchone()[0]


def get_parameter(cur, par):
    """
    Extrat the global parameter from the table "parameters".
    Args:
        cur: cursor of database connection
        par: parameter name. Possible values include 'total_mass', 'B_mass', 'card_Bn'
    """
    cur.execute("SELECT value FROM parameters WHERE par='%s';" % par)
    return cur.fetchone()[0]


def update_parameter(cur, par, new_value):
    """
    Update the global parameter in the table "parameters".
    Args:
        cur: cursor of database connection
        par: parameter name. Possible values include 'total_mass', 'B_mass', 'card_Bn'
        new_value: new value, a double
    """
    cur.execute("UPDATE parameters SET value=%f WHERE par='%s';" % 
                    (new_value, par))


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
    update_parameter(cur, 'B_mass', get_parameter(cur, 'total_mass'))

    ## a table to keep track of the removal order of each entry
    cur.execute("CREATE TABLE rmOrder (value varchar(20), dimension int, r int);")

    ## create tables Bn(value, mass) to store all values in Rn and the mass
    ## note that if a value is missing in Btable, we treat its mass as zero
    for n in range(N):
        cur.execute(("CREATE TABLE B%d AS SELECT value, sum(measure) as mass FROM " % n) 
             + ("(SELECT R%d.value as value, COALESCE(Btable.%s, 0) as measure " % (n, X_name))
             + ("FROM Btable RIGHT JOIN R%d ON Btable.%s=R%d.value) AS t " % (n, col_names[n], n))
             + ("GROUP BY value;"))
        ## record its cardinality
        update_parameter(cur, 'card_B%d' % n, compute_card(cur, "B"+str(n)))



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


def get_next_Brow(Bn, cur):
    """
    Get the next row in table Bn with the smallest mass.
    Args:
        Bn: table name, one of B0 ... B(N-1)
        cur: cursor for database connection
    """
    cur.execute("SELECT value, mass FROM %s ORDER BY mass LIMIT 1;" % Bn)
    return cur.fetchone()


def find_single_block(table_curr, col_names, X_name, N, cur, dmeasure, policy):
    """
    Args:
        table_curr: the current relation table (the relation "R" in paper)
        col_names: lisf of column names of the relation, with length N
        X_name: column name of the measure attribute
        N: number of dimension
        cur: cursor of database connection
        dmeasure: "arithmetic" or "geometric" or "suspicious", density measure
        policy: "cardinality" or "density"
    """
    ## initialize
    init_B_tables(table_curr, col_names, X_name, N, cur)
    max_dens = compute_density(cur, N, dmeasure)
    curr_order, max_order = 1, 1

    ## repeatedly remove all entries in Btable
    while has_remained_B(N, cur):
        ## select dimension to remove
        n_rm = select_dimension(cur, N, policy, dmeasure)
        B_rm = ("B%d" % n_rm)

        ## entries that have mass smaller than the average will be removed
        card = get_parameter(cur, par=("card_B%d" % n_rm))
        B_mass = get_parameter(cur, par="B_mass")
        avg_mass = B_mass/float(card)

        ## repeatedly delete entries in B_rm
        ## Note: this chunk of code is very inefficient!!!
        curr_row = get_next_Brow(B_rm, cur)
        while curr_row and curr_row[1] <= avg_mass:
            (curr_value, curr_mass) = curr_row

            ## record the delete order
            cur.execute("INSERT INTO rmOrder (value, dimension, r) " + 
                    "VALUES ('%s', %d, %d)" % (curr_value, n_rm, curr_order))
            curr_order += 1
            
            ## remove this entry
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

            ## move to the next row
            curr_row = get_next_Brow(B_rm, cur)

        ## because we have changed Btable, we need to re-compute the mass
        recompute_Bmass(col_names, X_name, N, cur)

      
    ## reconstruct the dense block
    for n in range(N):
        cur.execute(("CREATE TABLE final_B%d AS SELECT R.value FROM R%d as R, rmOrder " % (n,n)) 
             + ("WHERE R.value=rmOrder.value and rmOrder.dimension=%d and rmOrder.r >= %d;" 
                % (n, max_order)))

    ## clean up: drop the created temporary tables
    cur.execute("DROP TABLE Btable, rmOrder, %s;" % 
                    (",".join(["B"+str(n) for n in range(N)])))


def init_dcube_tables(table_name, table_curr, col_names, X_name, K, N, cur):
    """
    Initialize the tables for D-cube.
    Args:
        table_name: name of the relational table storing data
        table_curr: name of the table that we will manipulate
        col_names: column names of the relation for the N dimensions
        X_name: column name of the measure attribute
        K: number of blocks
        N: number of dimension
        cur: cursor of database connection
    """
    ## make a copy of the original table to avoid changing the original data table; 
    ## the copied table will be changed.
    cur.execute("CREATE TABLE %s AS SELECT * FROM %s;" % (table_curr, table_name))

    ## create tables Rn to store the unique values in each dimension
    for n in range(N):
        cur.execute("CREATE TABLE R%d AS SELECT DISTINCT %s as value FROM %s " % 
                    (n, col_names[n], table_curr))

    ## create a table for global parameters
    cur.execute("CREATE TABLE parameters (par varchar(20), value double precision);")
    ## total Mass M_R and M_B
    cur.execute("INSERT INTO parameters (par) VALUES ('total_mass');")
    cur.execute("INSERT INTO parameters (par) VALUES ('B_mass');")
    ## create fields to store cardinality for B_1 ... B_N and R_1 ... R_N
    for n in range(N):    
        cur.execute("INSERT INTO parameters (par) VALUES ('card_B%d');" % n)
        cur.execute("INSERT INTO parameters (par) VALUES ('card_R%d');" % n)

    ## store the cardinality of R_1 ... R_N; these remain unchanged.
    for n in range(N):
        update_parameter(cur, 'card_R%d' % n, compute_card(cur, "R"+str(n)))


def dcube(table_name, col_names, X_name, K, N, cur, 
            dmeasure="arithmetic", policy="cardinality", 
            outdir="out/", out_prefix="out"):
    """
    D-Cube algorithm. Find K dense blocks in a given tensor.
    Args:
        table_name: name of the relation, it has N+1 columns, 
                where the first N columns for the N dimensions with names col_names,
                and the last column specifying the measure attribute with name X_name.
        col_names: column names of the relation for the N dimensions
        X_name: column name of the measure attribute
        K: number of blocks
        N: number of dimension
        cur: cursor of database connection
        dmeasure: "arithmetic" or "geometric" or "suspicious", density measure
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

        ## print result to stdout
        print ("Found block %d:" % (k+1))
        for n in range(N):        
            print ("\tdimension %d:" % n)
            cur.execute("SELECT * FROM final_B%d" % n)
            print "\t", cur.fetchall()
                
        ## save the block to disk
        outfile = outdir+"/"+out_prefix+"_block"+str(k+1)+".csv"
        with open(outfile, mode="wt") as fout:
            cur.copy_to(fout, ("block%d" % k), sep=',')
        print ("The %d-th block is written to file '%s'." % (k+1, outfile))
        
        ## remove the entries in the found block from current table
        cur.execute(("DELETE FROM %s as R WHERE EXISTS " % table_curr) + 
            ("(SELECT 1 FROM block%d as B WHERE %s);" % 
                (k, " and ".join([("R.%s=B.%s" % (col_names[n], col_names[n])) 
                                    for n in range(N)]))))
        ## clean up: drop the final_Bn and blockk tables
        cur.execute("DROP TABLE %s;" % 
                        (",".join(["final_B"+str(n) for n in range(N)])))
        cur.execute("DROP TABLE block%d;"%k)

        ## if nothing left, then stop
        R_card = compute_card(cur, table_curr)
        # print ("After removing the block, %d entries are left." % R_card)
        if R_card == 0:
            print ("Algorithm stopped after finding %d blocks because no entries are left." %
                (k+1))
            break

    ## clean up: drop the created temporary tables
    cur.execute("DROP TABLE %s, parameters;" % table_curr)
    cur.execute("DROP TABLE %s;" % ",".join(["R"+str(n) for n in range(N)]))


