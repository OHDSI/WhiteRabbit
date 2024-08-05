//
// To be able to use the configured snowflake test environment, make sure that the role and grant
// statements below have been exectuded, using the correct snowflake username for <<snowflake user>>
//
//create or replace warehouse compute_wh warehouse_size=xsmall initially_suspended=true auto_suspend=60;
//use warehouse compute_wh
//create database test;
//create schema test.wr_test;
//create role if not exists testrole;
//grant usage on database test to role testrole;
//grant usage on schema test.wr_test to role testrole;
//grant ALL PRIVILEGES on schema test.wr_test to role testrole;
//grant role testrole to user <<snowflake user>>;

//use schema test.wr_test;

//CREATE OR REPLACE WAREHOUSE wr_test_compute_wh WAREHOUSE_SIZE=xsmall AUTO_SUSPEND = 60 INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE wr_test_compute_wh;

DROP TABLE IF EXISTS wr_test.wr_test.person;
DROP TABLE IF EXISTS wr_test.wr_test.cost;

DROP SCHEMA IF EXISTS wr_test.wr_test;
DROP DATABASE IF EXISTS wr_test;


CREATE DATABASE wr_test;
CREATE SCHEMA wr_test.wr_test;
CREATE SCHEMA wr_test.wr_test_dupl;
create role if not exists testrole;
grant usage on database wr_test to role testrole;
grant usage on schema wr_test.wr_test to role testrole;
grant usage on schema wr_test.wr_test_dupl to role testrole;
grant ALL PRIVILEGES on schema wr_test.wr_test to role testrole;
grant ALL PRIVILEGES on schema wr_test.wr_test_dupl to role testrole;
grant role testrole to user JANSFBLOM;

CREATE TABLE wr_test.wr_test.cost (cost_id BIGINT, cost_event_id BIGINT, cost_domain_id STRING, cost_type_concept_id BIGINT, currency_concept_id BIGINT, total_charge NUMERIC, total_cost NUMERIC, total_paid NUMERIC, paid_by_payer NUMERIC, paid_by_patient NUMERIC, paid_patient_copay NUMERIC, paid_patient_coinsurance NUMERIC, paid_patient_deductible NUMERIC, paid_by_primary NUMERIC, paid_ingredient_cost NUMERIC, paid_dispensing_fee NUMERIC, payer_plan_period_id BIGINT, amount_allowed NUMERIC, revenue_code_concept_id BIGINT, reveue_code_source_value STRING, drg_concept_id BIGINT, drg_source_value STRING);

// table with same name in different schema will trigger error if JDBC metadata is not handled correctly (see https://github.com/OHDSI/WhiteRabbit/issues/409)
CREATE TABLE wr_test.wr_test_dupl.cost (cost_id_dupl BIGINT, cost_event_id_dupl BIGINT, cost_domain_id_dupl STRING);

CREATE TABLE wr_test.wr_test.person (person_id BIGINT, gender_concept_id BIGINT, year_of_birth BIGINT, month_of_birth BIGINT, day_of_birth BIGINT, birth_datetime TIMESTAMP, race_concept_id BIGINT, ethnicity_concept_id BIGINT, location_id BIGINT, provider_id BIGINT, care_site_id BIGINT, person_source_value STRING, gender_source_value STRING, gender_source_concept_id BIGINT, race_source_value STRING, race_source_concept_id BIGINT, ethnicity_source_value STRING, ethnicity_source_concept_id BIGINT);

REMOVE @~ pattern=".*csv.gz";

put file:///scan_data/cost-no-header.csv @~;

put file:///scan_data/person-no-header.csv @~;

CREATE OR REPLACE FILE FORMAT my_csv_format TYPE = 'csv' FIELD_DELIMITER = ',';

COPY INTO wr_test.wr_test.cost from @~/cost-no-header.csv.gz FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

COPY INTO wr_test.wr_test.person from @~/person-no-header.csv.gz FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

REMOVE @~ pattern=".*csv.gz";