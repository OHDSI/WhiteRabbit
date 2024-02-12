## Development

White Rabbit and Rabbit in a Hat are structured as a Maven package. Contributions are welcome.

While the software in the project can be executed with Java 8 (1.8), for development Java 17 is needed.
This has to do with test and verification dependencies that are not available in a version compatible with Java 8 .

Please note that when using an IDE for development, source and target release must still be Java 8 (1.8) . This is enforced
in the maven build file (pom.xml). You can use any IDE you prefer.

Please note that historically, all files have CRLF line endings. Please configure your IDE and local git to keep line endings as is. This avoids merge conflicts.

To generate the files ready for distribution, run `mvn install`.

When creating a pull request, please make sure the verification phase (`mvn verify`) does not fail.

### Testing

A number of unit and integration tests exist. The integration tests run only in the maven verification phase,
(`mvn verify`) and depend on docker being available to the user running the verification. If docker is not available, the
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

It is possible to skip the Snowflake tests without failing the build by passing
`-Dohdsi.org.whiterabbit.skip_snowflake_tests=1` to maven.