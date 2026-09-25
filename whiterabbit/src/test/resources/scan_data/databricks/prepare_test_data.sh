#!/usr/bin/env bash

#
# this script can be used to prepare a databricks instance for use with the integration tests of WhiteRabbit
#
# prerequisites:
# - a Databricks instance with a catalog/schema with write permissions (wr_test is assumed as the name for both)
# - a Personal Access Token for the Databricks instance
#	(see https://docs.databricks.com/aws/en/dev-tools/auth/pat)
# - databricks cli installed
#	(see: https://docs.databricks.com/aws/en/dev-tools/cli/install)

#
# copy the local testfiles to the databricks instance
#
databricks fs cp cost-no-header.csv  dbfs:/Volumes/wr_test/wr_test/wr_test
databricks fs cp person-no-header.csv  dbfs:/Volumes/wr_test/wr_test/wr_test

#
# Excute the sql scripts below by copying them in an SQL window in your Databricks environment and execute them
#
# To (re)create the tables:
#
#   create_data_databricks.sql
#
#
# To insert the uploaded csv's into the tables:
#
#   insert_data_databricks.sql
