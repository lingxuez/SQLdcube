## Note: 
## change DBNAME and USERNAME accordingly; default is system $USER (andrew ID)
## and change PORT to your assigned port number.

DBNAME=$(USER)
USERNAME=$(USER)
PORT=15748

start:
	export PGPORT=$(PORT)
	export PGHOST=/tmp
	pg_ctl -D $(HOME)/826prj -o '-k /tmp' start
	
create:
	createdb $(DBNAME)

run: 
	python dcube.py -db $(DBNAME) -user $(USERNAME) -port $(PORT)

stop:
	pg_ctl -D $(HOME)/826prj stop


