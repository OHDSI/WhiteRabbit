DROP TABLE IF EXISTS hive_metastore.whiterabbit_test.cost;
DROP TABLE IF EXISTS hive_metastore.whiterabbit_test.person;

CREATE TABLE hive_metastore.whiterabbit_test.cost (cost_id BIGINT, cost_event_id BIGINT, cost_domain_id STRING, cost_type_concept_id BIGINT, currency_concept_id BIGINT, total_charge bigint, total_cost bigint, total_paid bigint, paid_by_payer bigint, paid_by_patient bigint, paid_patient_copay bigint, paid_patient_coinsurance bigint, paid_patient_deductible bigint, paid_by_primary bigint, paid_ingredient_cost bigint, paid_dispensing_fee bigint, payer_plan_period_id BIGINT, amount_allowed bigint, revenue_code_concept_id BIGINT, reveue_code_source_value STRING, drg_concept_id BIGINT, drg_source_value STRING);

CREATE TABLE hive_metastore.whiterabbit_test.person (person_id BIGINT, gender_concept_id BIGINT, year_of_birth BIGINT, month_of_birth BIGINT, day_of_birth BIGINT, birth_datetime TIMESTAMP_NTZ, race_concept_id BIGINT, ethnicity_concept_id BIGINT, location_id BIGINT, provider_id BIGINT, care_site_id BIGINT, person_source_value STRING, gender_source_value STRING, gender_source_concept_id BIGINT, race_source_value STRING, race_source_concept_id BIGINT, ethnicity_source_value STRING, ethnicity_source_concept_id BIGINT);

