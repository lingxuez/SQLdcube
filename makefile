########################################
## PostgreSQL implementation of D-CUBE
##
## Authors:
## Lingxue Zhu (lzhu1@andrew.cmu.edu)
## Jining Qin (jiningq@andrew.cmu.edu)
##
#########################################
## 
## Options:
##
## DBNAME: database name; default is system $USER (andrew ID)
## USERNAME: database user; default is system $USER (andrew ID)
## PORT: database port; change this to your assigned port number.
## DMEASURE: 'arithmetic' or 'geometric' or 'suspicious'
## POLICY: 'density' or 'cardinality'
## K: number of dense blocks
## DATA: dataset to use, 'darpa' or 'wiki' or 'amazon' or 'yelp' or 'airforce'
##
##############################################
##
## To run D-cube on DARPA data:
## $ make darpa
## 
## To run D-cube on user-specified input file (INFILE) with output to OUTDIR:
## $ make run
##
## If you have never set up PostgreSQL on your computer, try
## $ make setup
##
## To start the PostgreSQL server, try
## $ make start
##
## To stop the PostgreSQL after finishing, try
## $ make stop
##
#############################################

DBNAME=$(USER)
USERNAME=$(USER)
PORT=5432
K=1

## 'arithmetic' or 'geometric'
DMEASURE=arithmetic
POLICY=density

## 'darpa' or 'wiki' or 'amazon' or 'yelp' or 'airforce'
DATA=airforce
OUTDIR=airforce_out/
INFILE=data/mid_airforce.csv

# N=3
# OUTDIR=tests/
# INFILE=tests/test_data/test_data.csv

all: start tiny_darpa stop

setup:
	PGPORT=$(PORT) PGHOST=/tmp initdb $(HOME)/826prj2 
	PGPORT=$(PORT) PGHOST=/tmp pg_ctl -D $(HOME)/826prj2 -o '-k /tmp' start 
	createdb $(DBNAME)


start:
	PGPORT=$(PORT) PGHOST=/tmp pg_ctl -D $(HOME)/826prj2 -o '-k /tmp' start
	
run: 
	python dcube.py -db $(DBNAME) -user $(USERNAME) -port $(PORT) -data 'test' \
			-in $(INFILE) -K $(K) -N $(N) \
			-outdir $(OUTDIR) -dmeasure $(DMEASURE) -policy $(POLICY)

stop:
	pg_ctl -D $(HOME)/826prj2 stop


tiny_darpa: 
	python dcube.py -db $(DBNAME) -user $(USERNAME) -port $(PORT) \
			-in tests/tiny_darpa.csv -K $(K) -data darpa \
			-outdir tests/ -dmeasure $(DMEASURE) -policy $(POLICY)

testdata: 
	python dcube.py -db $(DBNAME) -user $(USERNAME) -port $(PORT) \
			-in tests/test_data/test_data_1.csv -K 2 -data test \
			-outdir tests/test_out -dmeasure suspicious -policy density
	python dcube.py -db $(DBNAME) -user $(USERNAME) -port $(PORT) \
			-in tests/test_data/test_data.csv -K 2 -data test \
			-outdir tests/test_out -dmeasure arithmetic -policy cardinality

realdata: 
	python dcube.py -db $(DBNAME) -user $(USERNAME) -port $(PORT) \
			-in $(INFILE) -K $(K) -data $(DATA) -outdir $(OUTDIR) \
			-dmeasure $(DMEASURE) -policy $(POLICY)

clean:
	@rm *.pyc

all.tar:
	@tar -zcvf dcube.tar.gz makefile *.py



