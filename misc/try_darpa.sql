
CREATE TABLE tmp (src varchar(20), dst varchar(20), linkdate varchar(40));

\COPY tmp FROM 'tiny_darpa.csv' DELIMITER ',' CSV;

CREATE TABLE darpa AS
SELECT src, dst, to_date(linkdate, 'MM/DD/YYYY HH24:MI') as linkdate FROM tmp;

