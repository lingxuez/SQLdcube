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
## DBNAME: database name; default is system $USER
## USERNAME: database user; default is system $USER
## PORT: database port number; default is 5432
##
##############################################
##
## Usage:
##
## First, initialize a new database under $(HOME)/826prj called $(DBNAME).
## You only need to do this once:
## 	$ make setup
##
## To start the PostgreSQL server, run the small, and then stop the server:
## 	$ make
##
## To run the small demo, assuming the server has been started:
##  $ make demo
## 
## To start the PostgreSQL server:
##	$ make start
##
## To stop the PostgreSQL server after you're done:
## 	$ make stop
##  
## To eliminate all the derived files:
## 	$ make clean
##
## To create a .tar file for distribution:
## 	$ make all.tar
##
## To re-complie LaTeX to re-produce doc/final_report.pdf:
##  $ make paper.pdf
##
##############################################
.PHONY: all setup start stop demo clean all.tar

DBNAME=$(USER)
USERNAME=$(USER)
PORT=5432

all: start demo stop

setup:
	PGPORT=$(PORT) PGHOST=/tmp initdb $(HOME)/826prj
	PGPORT=$(PORT) PGHOST=/tmp pg_ctl -D $(HOME)/826prj -o '-k /tmp' start 
	createdb $(DBNAME)
	pg_ctl -D $(HOME)/826prj stop

start:
	@echo ""
	@echo "************** Starting PostgreSQL server **************"
	PGPORT=$(PORT) PGHOST=/tmp pg_ctl -D $(HOME)/826prj -o '-k /tmp' start
	@echo "************** PostgreSQL server started **************"

stop:
	@echo ""
	@echo "************** Stoping PostgreSQL server **************"
	pg_ctl -D $(HOME)/826prj stop
	@echo "************** PostgreSQL server stopped **************"

demo:
	@echo ""
	@echo "************** Starting Demo **************"
	@python dcube.py -db $(DBNAME) -user $(USERNAME) -port $(PORT) \
			-in demo/demo_data.csv -K 1 -N 3 -opt mark \
			-outdir demo/demo_out -dmeasure arithmetic -policy density
	@echo "************** Demo Finished **************"

clean:
	@rm -f *.pyc
	@rm -f doc/paper_src/*.aux doc/paper_src/*.log \
			doc/paper_src/*.bbl doc/paper_src/*.blg


all.tar: clean
	@tar -zcvf dcube.tar makefile *.py *.txt demo/*.csv \
		doc/*.pdf doc/paper_src/*.tex doc/paper_src/*.bib doc/paper_src/plots

paper.pdf:
	@echo ""
	@echo "************** Re-compiling pdfTeX **************"
	@cd doc/paper_src; \
	pdflatex --interaction=batchmode final_report.tex; \
	bibtex final_report.aux; \
	pdflatex --interaction=batchmode final_report.tex; \
	pdflatex --interaction=batchmode final_report.tex
	@mv doc/paper_src/final_report.pdf doc/
	@echo "************** Finished doc/final_report.pdf **************"

