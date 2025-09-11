#!/usr/bin/env bash

#
# this script can be used to prepare a databricks instance for use with the integration tests of WhiteRabbit
#
# prerequisites:
# - a Databricks instance with a catalog/schema with write permissions
# - a Personal Access Token for the Databricks instance
#	(see https://docs.databricks.com/aws/en/dev-tools/auth/pat)
# - databricks cli installed
#	(see: https://docs.databricks.com/aws/en/dev-tools/cli/install)
# - dbsqlcli installed
#	(see https://docs.databricks.com/aws/en/dev-tools/databricks-sql-cli)
#

#
# copy the local testfiles to the databricks instance
#
databricks fs mkdir dbfs:/tmp/whiterabbit_test
databricks fs cp cost-no-header.csv  dbfs:/tmp/whiterabbit_test/
databricks fs cp person-no-header.csv  dbfs:/tmp/whiterabbit_test/

#
# create the tables for the test data; might ask the user to confirm destuctive operations (drop the tables)
#
dbsqlcli -e create_data_databricks.sql

#
# insert the data from the files in the databricks instance
#
dbsqlcli -e insert_data_databricks.sql
