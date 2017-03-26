## Before using PostgreSQL,
## initialize a database structure and create a database
## only have to do this once

## ssh lzhu1@ghc86.ghc.andrew.cmu.edu

export PGPORT=15748 # sets the PORT to be used by PostgreSQL
export PGHOST=/tmp # sets the directory for the socket files

# initializes a database structure on the folder $HOME/826prj
## initdb $HOME/826prj 

# starts the server on the port YYYYY, using $HOME/826prj as data folder
pg_ctl -D $HOME/826prj -o '-k /tmp' start

# creates a database, with your andrew id as its name
# createdb $USER


# When you're done, please stop the server (or add this line to your .logout file):
## pg_ctl -D $HOME/826prj stop