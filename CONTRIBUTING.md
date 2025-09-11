## Development

White Rabbit and Rabbit in a Hat are structured as a Maven package. Contributions are welcome.

While the software in the project can be executed with Java 8 (1.8), for development Java 17 (or higher, currently tested upto version 21) is needed.
This has to do with test and verification dependencies that are not available in a version compatible with Java 8 .

Please note that when using an IDE for development, source and target release must still be Java 8 (1.8) . This is enforced
in the maven build file (pom.xml). You can use any IDE you prefer.

Please note that historically, all files have CRLF line endings. Please configure your IDE and local git to keep line endings as is. This avoids merge conflicts.

To generate the files ready for distribution, run `mvn install`.

When creating a pull request, please make sure the verification phase (`mvn verify`) does not fail.

When contributing code, please make sure that `mvn verify` runs without errors. 

### Testing

A number of unit and integration tests exist. The integration tests run only in the maven verification phase,
(`mvn verify`) and depend on Docker being available to the user running the verification. If Docker is not available, the
integration tests will fail. 

When adding test, please follow these conventions:

- **unit tests:** tests that do not depend on any external component, and do not take a long time to run should be named `Test<Classname>.java`;
  These test will be automatically run it the mave test phase. (a few seconds is short, more than a minute is definitely too long)
- **integration tests:** tests that do take a long time to run, or depend on external components (e.g. docker for TestContainers, or another component like an external database)
  should be named `<Classname>IT.java`. These will be automatically run in the maven verify phase;
- **optional tests:** for any test that depends on an external component that is not necessarily always available, or requires a local setup, make
  sure that the test uses a junit assumption to verify that the setup will enable the test. See the Snowflake integration tests
  (and the information on Snowflake below) for an example of how to set that up. This way, these tests get skipped, but do not
  fail the build.

Also, GitHub actions have been configured to run the test suite automatically on Github for branches
that have been configured for this (typically the master branch, and pull requests to the master branch).
##### Checking for dependency updates

Use the `versions-maven-plugin` to check for dependency updates. This can be done by running `mvn versions:display-dependency-updates`.
Keep in mind that major version updates can break the build, so it is recommended to update dependencies one by one, and run the tests after each update.
Also, consult the parent pom.xml for dependencies marked with comments that indicate that they should not be updated.

##### Checking for potential vulnerabilities

The project uses the OWASP dependency-check plugin to check for potential vulnerabilities in the dependencies. Because this
check can take a long time (for downloading/updating the vulnerabilities database), it is disabled by default, 
but can be enabled by running `mvn dependency-check:check`. This will report vulnerabilities in the standard maven output,
and write them to `target/dependency-check-report.html` for the parent project and each of the subprojects.

##### Checking for compatible licenses

Since the Rabbit tools are open source, it is important that the dependencies are also open source.
The verification phase ('mvn verify') will also check for compatible licenses in the dependencies. This is done by the maven-license-plugin.
When dependencies are updated, sometimes the license changes, and this can cause the verification build to fail. In that case, one can extend the
list of allowed licenses in the `pom.xml` file.

### Example in- and output

The file `examples.zip` contains a set of input and output examples for White Rabbit and Rabbit in a Hat.
These are used for testing of the main White Rabbit and Rabbit in a Hat features. An overview is given in below table.

| Folder         | Description                                                                                                                                                                                                                                                                                                                                              |
|:---------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `wr_input_csv` | csv files to test scanning on different data types and long table names.                                                                                                                                                                                                                                                                                 |
| `wr_input_sas` | sas7bdat files to test sas input                                                                                                                                                                                                                                                                                                                         |
| `wr_output`    | Scan reports created from files in `wr_input_csv`, `wr_input_sas` and [native a Synthea database loaded in Postgres](https://github.com/ohdsi/ETL-Synthea). All with default scan options.<br> This folder also includes fake data generated from the csv scan report. The csv scan report is used to test the opening a Scan Report in Rabbit in a Hat. |
| `riah_input`   | An example mapping file used to create the Rabbit in a Hat outputs.                                                                                                                                                                                                                                                                                      |
| `riah_output`  | All export formats created by Rabbit in a Hat: as word, html, markdown, sql skeleton and the R TestFramework.<br> These are all generated from `riah_input/riah_mapping_example.gz`.                                                                                                                                                                     |

### Database support

If you are considering adding support for a new type of database, it is recommended to follow the pattern as used
by the SnowflakeHandler class, which extends the JdbcStorageHandler interface. This way, the brand/database specific code
is isolated into one class, instead of through the code paths that implement support for the 
databases that were added earlier. This will lead to clearer code, that will also be easier to test and debug.

For a number of (cloud) databases, integration tests are available. These tests will only run if the necessary
instance and authentication information is provided through environment files (currently `snowflake.env` and 
`databricks.env`). These files, if provided, should exist in the root directory of the project. If they do not exist,
the related tests will be skipped during the maven verify phase. Also, these files should never be committed to git,
and they are excluded in the `.gitignore` file.

Please note that for Snowflake and Databricks, a browser flow authentication can be used optionally. These flows
are not automatically tested, they should be verified manually from the WhiteRabbit GUI.

### Snowflake

There are automated tests for Snowflake, but since it is not (yet?) possible to have a local
Snowflake instance in a Docker container, these test will only run if the following information
is provided through system properties, in a file named `snowflake.env` in the root directory of the project:

    SNOWFLAKE_WR_TEST_ACCOUNT=<snowflake account>
    SNOWFLAKE_WR_TEST_USER=<snowflake username>
    SNOWFLAKE_WR_TEST_PASSWORD=<snowflake password>
    SNOWFLAKE_WR_TEST_WAREHOUSE=<snowflake warehouse>
    SNOWFLAKE_WR_TEST_DATABASE=<database name>
    SNOWFLAKE_WR_TEST_SCHEMA=<schema name>

You will need to set up database and schema (and wharehouse) as documented in `whiterabbit/src/test/resources/scan_data/create_data_snowflake.sql` 

It is recommended that user, password, database and schema are created for these tests only,
and do not relate in any way to any production environment.
The schema should not contain any tables when the test is started.

### Databricks

Like Snowflake, Databricks is a cloud-based service for which it is not (yet?) possible to have a local
test instance. The Databricks tests will only run if the following information
is provided through system properties, in a file named `databricks.env` in the root directory of the project:

    DATABRICKS_WR_TEST_SERVER=<Databricks instance server url>
    DATABRICKS_WR_TEST_HTTP_PATH=<Databricks Http Path for the instance>
    DATABRICKS_WR_TEST_PERSONAL_ACCESS_TOKEN=<Personal Access Token for the instance>
    DATABRICKS_WR_TEST_CATALOG=<Catalog to use for the tests>
    DATABRICKS_WR_TEST_SCHEMA=<Schema to use for the tests>

There is no automated upload of test data (yet). The folder `whiterabbit/src/test/resources/scan_data/databricks` 
contains the scripts to create the tables and data for the tests. These scripts need to be run before the 
Databricks integration tests can be run. For a short outline of the tools needed, and the steps to perform. see
`whiterabbit/src/test/resources/scan_data/databricks/prepare_test_data.sh`.

It is recommended that token, catalog and schema are created for these tests only, and do not relate in any way to 
any production environment.
