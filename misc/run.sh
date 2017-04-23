
PORT=15748
K=3
DMEASURE=arithmetic
POLICY=density
N=3
OUTDIR=out/
INFILE=tests/test_data.csv

for DMEASURE in "arithmetic" "geometric"
do
	python dcube.py -db $USER -user $USER -port ${PORT} \
		-in ${INFILE} -K ${K} -N ${N} \
		-outdir ${OUTDIR} -dmeasure ${DMEASURE} -policy ${POLICY}  
done

