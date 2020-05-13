---
title: Rabbit in a Hat testing framework
---

# Introduction
Rabbit in a Hat can generate a framework for creating a set of [unit tests](https://en.wikipedia.org/wiki/Unit_testing).
The framework consists of a set of R functions tailored to the source and target schema in your ETL.
These functions can then be used to define the unit tests.

Unit testing assumes that you have your data in source format somewhere in a database.
You should already have created an ETL process that will extract from the source database, transform it into CDM format, and load it into a CDM schema.
The unit test framework can be used to make sure that your ETL process is doing what it is supposed to do.
For this you will need to create a new, empty database with exactly the same structure as your source database, and a new empty database where a test CDM database will live.
The testing framework can be used to insert test data into the empty source schema.
Next, you can run your ETL process on the test data to populate the test CDM database.
Finally, you can use the framework to verify that the output of the ETL in the test CDM database is what you'd expect given the test source data.

# Process Overview
These are the steps to perform unit testing:

1. Create the testing framework for your source and target database schemas
2. Using the framework in R, define a set of unit tests
3. Use the framework to generate testing data in the source data schema
4. Run your ETL on the test data to produce data in the CDM schema
5. Use the framework to evaluate whether the CDM data meets the defined expectations

It is advised to use [R-Studio](https://www.rstudio.com/) for defining your unit tests.
One reason is that RStudio will automatically prompt you with possible function and argument names after you've only typed the first few characters.

# Creating the Testing Framework

In Rabbit in a Hat, have your ETL specifications open.
The source data schema should be loaded from the White-Rabbit scan report, and the target data schema should be selected (usually the OMOP CDM v5).
Go to _File → Generate ETL Test Framework_, and use a file name with the .R extension, for example `MyTestFrameWork.R`.


# Using the Testing Framework

Next, create an empty R script, and start by sourcing the R file that was just created:

```R
source("MyTestFrameWork.R")
```

Be sure to run this command immediately to make the function definitions available to R-Studio.


## Available functions

The test framework defines the following functions **for each table in the source schema**:

* `get_defaults_<table name>` shows the default field values that will be used when creating a record in the table. At the start, these default values have been taken from the White-Rabbit scan report, using the most frequent value.
* `set_defaults_<table name>` can be used to change the default values of one or more fields in the table. For example `set_defaults_enrollment(enrollment_date = "2000-01-01")`.
* `add_<table name>` can be used to specify that a record should be created in the table. The arguments can be used to specify field values. For fields where the user doesn't specify a value, the default value is used. For example `add_enrollment(member_id = "M00000001")`.

The following functions are defined **for each table in the CDM schema**:

* `expect_<table name>` can be used to state the expectation that at least one record with the defined properties should exist in the table. For example `expect_person(person_id = 1, person_source_value = "M00000001")`.
* `expect_no_<table name>` can be used to state the expectation that no record with the defined properties should exist in the table. For example `expect_no_condition_occurrence(person_id = 1)`.
* `expect_count_<table name>` can be used to state the expectation that a specific number of records with the defined properties should exist in the table. For example `expect_count_condition_occurrence(person_id = 1, rowCount = 3)`.
* `lookup_<table name>` can be used to get a specific value from another table. For example to get the `person_id` by `person_source_value`.

One further function is available:

* `declareTest` is used to group multiple statements under a single identifier. For example `declareTest(id = 1, description = "Test person ID")`.


## Defining unit tests

Using these functions, we can define tests.
Here is an example unit test:

```R
declareTest(101, "Person gender mappings")
add_enrollment(member_id = "M000000101", gender_of_member = "male")
add_enrollment(member_id = "M000000102", gender_of_member = "female")
expect_person(person_id = 101, gender_concept_id = 8507, gender_source_value = "male")
expect_person(person_id = 102, gender_concept_id = 8532, gender_source_value = "female")
```

In this example, we define a test for gender mappings.
We specify that two records should be created in the `enrollment` table in the source schema, and we specify different values for the `member_id` field and `gender_of_member` field.
Note that the `enrollment` table might have many other fields, for example defining the start and end of enrollment, but that we don't have to specify these in this example because these fields will take their default values, typically taken from the White-Rabbit scan report.

In this example we furthermore describe what we expect to see in the CDM data schema.
In this case we formulate expectations for the `person` table.

We can add many such tests to our R script.
For examples of a full set of test definitions, see:

* [Synthea unit tests](https://github.com/OHDSI/ETL-Synthea/tree/master/extras).
* [HCUP unit tests](https://github.com/OHDSI/JCdmBuilder/blob/master/tests/HCUPETLToV5/HcupTests.R)
* [JMDC unit tests](https://github.com/OHDSI/ETL-CDMBuilder/blob/master/man/JMDC/TEST_CASES/JmdcTests.R)

### Lookup functions

For some tests you need unknown values from other cdm tables.
In this case you can use the lookup function for the required table.
This creates where conditions on the other cdm table in the test sql.
In the example below, we do not know which person_id got assigned to this test person, so we lookup the id by source value:

```R
declareTest(101, "Person gender mappings")
add_enrollment(member_id = "M000000103")
add_diagnosis(member_id = "M000000103", code="I10")
expect_condition_occurrence(person_id=lookup_person("person_id", person_source_value="M000000103"), condition_concept_id = 320128)
```

These lookups can also be nested.


## Test Coverage (_v0.9.0_)

The framework also contains a function to show statistics on how well your tests cover your mappings.
Note that this does not show information on whether the tests passed.
Only how many source and target tables are covered by your defined tests.

A summary can be printed by running:

```R
summaryTestFramework()
```

which displays the following statistics
```
statistic                   summary
n_tests                        6.00
n_cases                        3.00
n_source_fields_tested         3.00
n_source_fields_mapped_from    8.00
source_coverage (%)           37.50
n_target_fields_tested         1.00
n_target_fields_mapped_to     45.00
target_coverage (%)            2.22
```

Statistics:

 * `n_tests`: total number of expects, expect_no's or expect_counts are defined
 * `n_cases`: total number of cases defined with `declareTest` function.
 * `n_source_fields_tested`: number of source fields for which a test data is defined
 * `n_source_fields_mapped_from`: number of source fields for which a mapping was defined in Rabbit in a Hat
 * `source_coverage`: percentage of mapped source fields for which a test has been defined
 * `n_target_fields_tested`: number of target fields for which one or more expects, expect_no's or expect_counts have been defined
 * `n_target_fields_mapped_to`: number of target fields for which a mapping was defined in Rabbit in a Hat
 * `target_coverage`: percentage of mapped target fields for which a test has been defined

Note that the mapping coverages depends on the mappings defined in Rabbit in a Hat.
If this mapping is incomplete or adjusted in the meantime, the `target_coverage` is possibly incorrect.
In this case, please update the mappings in Rabbit in a Hat and regenerate the testing framework.

You can get all source and target field for which no test has been defined with the following functions:
```R
getUntestedSourceFields()
getUntestedTargetFields()
```

## Generate test data
There are two ways to generate test data, either as SQL insert statements or as csv files.
Please choose the format that is appropriate for your ETL application.

### SQL
After we have defined all our tests we need to run
```R
insertSql <- generateInsertSql(databaseSchema = "nativeTestSchema")
testSql <- generateTestSql(databaseSchema = "cdmTestSchema")
```
to generate the SQL for inserting the test data in the database (insertSql), and for running the tests on the ETL-ed data (testSql). The insertion SQL assumes that the data schema already exists in `nativeTestSchema`, and will first remove any records that might be in the tables. We can execute the SQL in any SQL client, or we can use OHDSI's [DatabaseConnector package](https://github.com/OHDSI/DatabaseConnector). For example:

```R
library(DatabaseConnector)
connectionDetails <- createConnectionDetails(user = "joe",
                                             password = "secret",
                                             dbms = "sql server",
                                             server = "my_server.domain.org")
connection <- connect(connectionDetails)

executeSql(connection, paste(insertSql, collapse = "\n"))
```

### CSV (_v0.9.0_)

In case the source data are csv files rather than database tables, we use this function:
```R
generateSourceCsv(directory = "test_data", separator = ",")
```

And point the ETL to the given directory with test data.


## Run your ETL on the test data

Now that the test source data is populated, you can run the ETL process you would like to test.
The ETL should transform the data in `nativeTestSchema`, or in the csv directory, to CDM data in `cdmTestSchema`.


## Test the CDM expectations

The test SQL will create a table called `test_results` in `cdmTestSchema`, and populate it with the results of the tests.
If the table already exists it will first be dropped.
Again, we could use any SQL client to run this SQL, or we could use DatabaseConnector:

```R
executeSql(connection, paste(testSql, collapse = "\n"))
```

Afterwards, we can query the results table to see the results for each test:

```R
querySql(connection, "SELECT * FROM test_results")
```

Which could return this table:

| ID  | DESCRIPTION TEST       | STATUS |
|:----|:-----------------------|:-------|
| 101 | Person gender mappings | PASS   |
| 101 | Person gender mappings | PASS   |

In this case we see there were two expect statements under test 101 (Person gender mappings), and both expectations were met so the test passed.

The testing framework also contains a convenience function to display your test results:

```R
outputTestResultsSummary(connection, 'cdmTestSchema')
```

which either displays a success message `All 178 tests PASSED` or the failed unit tests:
```
FAILED unit tests: 1/178 (0.6%)
  ID                   DESCRIPTION          TEST STATUS
2  1 RMG-PD1 is assigned person_id Expect person   FAIL
```

## Export tests (_v0.9.0_)

We can create an overview of defined tests and export it, for example if you want to list the tests separately.

```R
getTestsOverview()
exportTestsOverviewToFile(filename = "all_test_cases.csv")
```

The first function produces a table like below and the second writes it to a csv file.
The output contains the following columns:

| testId | testDescription    | testType  | testTable          |
|:-------|:-------------------|:----------|:-------------------|
| 1      | My Expectations    | Expect    | person             |
| 1      | My Expectations    | Expect    | observation_period |
| 2      | My No Expectations | Expect No | person             |
