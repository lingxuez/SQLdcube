
CREATE TABLE darpa (src varchar(20), dst varchar(20), linkdate varchar(40));

\COPY darpa FROM 'misc/tiny_darpa.csv' DELIMITER ',' CSV;

UPDATE darpa SET linkdate=to_char(to_date(linkdate, 'MM/DD/YYYY HH24:MI'), 'MM/DD/YYYY');

ALTER TABLE darpa ADD COLUMN measure int;
UPDATE darpa SET measure = 1;

-- CREATE TABLE darpa AS
-- SELECT src, dst, to_date(linkdate, 'MM/DD/YYYY HH24:MI') as linkdate FROM tmp;

