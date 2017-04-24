## Utility functions for DCube
##

import math
import psycopg2

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
        return (-1, product)
    else:
        susp = (B_mass * (math.log(B_mass/float(R_mass)) - 1) + 
                R_mass * product - B_mass * math.log(product))
        return (susp, product)


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
        return (-1, 0)
    else:
        return (get_parameter(cur, par="B_mass") / avg_card, avg_card)


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
        return (-1, 0)
    else:
        return (get_parameter(cur, par="B_mass") / avg_card, avg_card)


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
            cur.execute("UPDATE parameters SET value=value-%f WHERE par='B_mass';" % rm_mass)
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


###################
## special utility functions for mark implementation
###################

def compute_card_mark(cur, table_name):
    """
    Compute the cardinality of a table using mark implementation.
    Args:
        cur: cursor of database connection
        table_name: name of the target table
    """
    cur.execute("SELECT count(exists) FROM %s;" % table_name)
    return cur.fetchone()[0]
    

def select_dimension_mark(cur, N, policy, dmeasure):
    """
    Args:
        cur: cursor of database connection
        N: number of total dimensions
        policy: "cardinality" or "density"
    """
    if policy == "cardinality":
        return select_dim_by_card(cur, N)
    elif policy == "density":
        return select_dim_by_dens_mark(cur, N, dmeasure)
    else:
        raise ValueError("policy must be one of 'cardinality' or 'density'.")


def select_dim_by_dens_mark(cur, N, dmeasure):
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
            cur.execute("SELECT sum(mass), count(*) FROM B%d WHERE mass <= %f and exists=1;" 
                        % (n, avg_mass))
            (rm_mass, rm_card) = cur.fetchone()

            update_parameter(cur, ("card_B%d" % n), card - rm_card)
            cur.execute("UPDATE parameters SET value=value-%f WHERE par='B_mass';" % rm_mass)
            density = compute_density(cur, N, dmeasure)
            if density > max_dens:
                max_dim, max_dens = n, density

            ## need to change the parameters back
            update_parameter(cur, ("card_B%d" % n), card)
            update_parameter(cur, "B_mass", B_mass)

    return max_dim


def get_next_Brow_mark(Bn, cur):
    """
    Get the next row in table Bn with the smallest mass.
    Args:
        Bn: table name, one of B0 ... B(N-1)
        cur: cursor for database connection
    """
    cur.execute("SELECT value, mass FROM %s WHERE exists=1 LIMIT 1;" % Bn)
    return cur.fetchone()

