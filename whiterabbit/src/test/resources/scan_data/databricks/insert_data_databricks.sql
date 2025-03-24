COPY INTO hive_metastore.whiterabbit_test.cost BY POSITION FROM '/tmp/whiterabbit_test/cost-no-header.csv' FILEFORMAT = CSV;
COPY INTO hive_metastore.whiterabbit_test.person BY POSITION FROM '/tmp/whiterabbit_test/person-no-header.csv' FILEFORMAT = CSV;
