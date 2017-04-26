##############################################
## PostgreSQL implementation of D-CUBE.
##
## Authors:
## Lingxue Zhu (lzhu1@andrew.cmu.edu)
## Jining Qin (jiningq@andrew.cmu.edu)
##
##############################################
## 
## Variables:
##
## DBNAME: database name; default is system $USER (andrew ID)
## USERNAME: database user; default is system $USER (andrew ID)
## PORT: database port; default is 5432
## K: number of dense blocks to detect
## N: number of dimensions of attributes
## INFILE: input .csv file with N+1 columns where the last column is the mass
## OUTIDR: output directory; the results will be saved under this directory
## DMEASURE: density measure method, 'arithmetic' or 'geometric' or 'suspicious'
## POLICY: dimension selection policy, 'density' or 'cardinality'
##
##############################################
##
## Usage:
##
## First, initialize a new database under $(HOME)/826prj called $(DBNAME).
## You only need to do this once:
## 	$ make setup
##
## To run a demo on 10,000 entries of Airforce data:
## 	$ make
##
## To run D-cube on your own input data, do the following: 
## 	1. change INFILE to be path to your data 
## 	2. set DATA=custom
## 	3. specify your desired output directory with OUTDIR
## 	4. specify the algorithm parameters K, N, DMEASURE, POLICY
## 	5. start the PostgreSQL server if you haven't:
##		$ make start
## 	6. run the algorithm:
## 		$ make run
## 	7. stop the PostgreSQL server after you're done:
## 		$ make stop
##  
## To eliminate all the derived files *.pyc:
## 	$ make clean
##
## To create a .tar file for distribution:
## 	$ make all.tar
##
##############################################
.PHONY: all start stop demo

## one of the 5 built-in data: 
## 'darpa' or 'wiki' or 'amazon' or 'yelp' or 'airforce'
## or 'custom' for other customized datasets
DATA=airforce

## PostgreSQL setting
DBNAME=$(USER)
USERNAME=$(USER)
PORT=5432

## number of blocks to detect
K=20
## number of dimension
N=3
## input file
INFILE=demo/mid_airforce.csv
## output directory
OUTDIR=demo/demo_out
## density measure: 'arithmetic' or 'geometric'
DMEASURE=arithmetic
## dimension selection policy: 'density' of 'cardinality'
POLICY=density


##############################################

all: start demo stop

setup:
	PGPORT=$(PORT) PGHOST=/tmp initdb $(HOME)/826prj
	PGPORT=$(PORT) PGHOST=/tmp pg_ctl -D $(HOME)/826prj -o '-k /tmp' start 
	createdb $(DBNAME)
	pg_ctl -D $(HOME)/826prj stop

start:
	PGPORT=$(PORT) PGHOST=/tmp pg_ctl -D $(HOME)/826prj -o '-k /tmp' start
	

stop:
	pg_ctl -D $(HOME)/826prj stop


demo:
	@python dcube.py -db $(DBNAME) -user $(USERNAME) -port $(PORT) \
			-in demo/mid_airforce.csv -K 2 -data airforce \
			-outdir demo/demo_out -dmeasure arithmetic -policy density

realdata: 
	@time python dcube.py -db $(DBNAME) -user $(USERNAME) -port $(PORT) \
			-in $(INFILE) -K $(K) -N $(N) -data $(DATA) -outdir $(OUTDIR)

run: 
	@time python dcube.py -db $(DBNAME) -user $(USERNAME) -port $(PORT) \
			-in $(INFILE) -outdir $(OUTDIR) -K $(K) -N $(N) \
			-dmeasure $(DMEASURE) -policy $(POLICY)

clean:
	@rm *.pyc

all.tar:
	@tar -zcvf dcube.tar makefile *.py doc/*.pdf demo/*.csv



