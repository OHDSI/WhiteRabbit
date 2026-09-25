COPY INTO wr_test.wr_test.cost BY POSITION FROM '/tmp/whiterabbit_test/cost-no-header.csv' FILEFORMAT = CSV;
COPY INTO wr_test.wr_test.person BY POSITION FROM '/tmp/whiterabbit_test/person-no-header.csv' FILEFORMAT = CSV;
