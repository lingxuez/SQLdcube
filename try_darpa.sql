
CREATE TABLE darpa (src varchar(20), dst varchar(20), linkdate varchar(40), label varchar(20));

## load from file
\COPY darpa FROM 'darpa_with_label.csv' DELIMITER ',' CSV;

## bucketize
UPDATE darpa SET linkdate=to_char(to_date(linkdate, 'MM/DD/YYYY HH24:MI'), 'MM/DD/YYYY');

## save to file
\COPY darpa TO 'darpa_with_label.csv' DELIMITER ',' CSV

-- CREATE TABLE darpa AS
-- SELECT src, dst, to_date(linkdate, 'MM/DD/YYYY HH24:MI') as linkdate FROM tmp;

