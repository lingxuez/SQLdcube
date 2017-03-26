## Before using PostgreSQL,
## initialize a database structure and create a database
## only have to do this once

export PGPORT=15748 # sets the PORT to be used by PostgreSQL
export PGHOST=/tmp # sets the directory for the socket files

# starts the server on the port YYYYY, using $HOME/826prj as data folder
pg_ctl -D $HOME/826prj -o '-k /tmp' start
