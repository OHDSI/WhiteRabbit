/* This is a stripped version (only the tables needed are created) of OMOP CDM V5.2 */

CREATE TABLE person
    (
     person_id						INTEGER		NOT NULL , 
     gender_concept_id				INTEGER		NOT NULL , 
     year_of_birth					INTEGER		NOT NULL , 
     month_of_birth					INTEGER		NULL, 
     day_of_birth					INTEGER		NULL, 
	 birth_datetime					TIMESTAMP 	NULL,
     race_concept_id				INTEGER		NOT NULL, 
     ethnicity_concept_id			INTEGER		NOT NULL, 
     location_id					INTEGER		NULL, 
     provider_id					INTEGER		NULL, 
     care_site_id					INTEGER		NULL, 
     person_source_value			VARCHAR(50) NULL, 
     gender_source_value			VARCHAR(50) NULL,
	 gender_source_concept_id		INTEGER		NULL, 
     race_source_value				VARCHAR(50) NULL, 
	 race_source_concept_id			INTEGER		NULL, 
     ethnicity_source_value			VARCHAR(50) NULL,
	 ethnicity_source_concept_id	INTEGER		NULL
    ) 
;

CREATE TABLE cost
    (
     cost_id					INTEGER	    NOT NULL , 
     cost_event_id       INTEGER     NOT NULL ,
     cost_domain_id       VARCHAR(20)    NOT NULL ,
     cost_type_concept_id       INTEGER     NOT NULL ,
     currency_concept_id			INTEGER			NULL ,
     total_charge						NUMERIC			NULL , 
     total_cost						NUMERIC			NULL , 
     total_paid						NUMERIC			NULL , 
     paid_by_payer					NUMERIC			NULL , 
     paid_by_patient						NUMERIC			NULL , 
     paid_patient_copay						NUMERIC			NULL , 
     paid_patient_coinsurance				NUMERIC			NULL , 
     paid_patient_deductible			NUMERIC			NULL , 
     paid_by_primary						NUMERIC			NULL , 
     paid_ingredient_cost				NUMERIC			NULL , 
     paid_dispensing_fee					NUMERIC			NULL , 
     payer_plan_period_id			INTEGER			NULL ,
     amount_allowed		NUMERIC			NULL , 
     revenue_code_concept_id		INTEGER			NULL , 
     reveue_code_source_value    VARCHAR(50)   NULL ,
	 drg_concept_id			INTEGER		NULL,
     drg_source_value		VARCHAR(3)		NULL
    ) 
;

-- The MySQL LOAD command is net very flexible with NULL values when they are empty in the csv file, so
-- some value patching is needed when LOADing to insert real NULL values
LOAD DATA INFILE '/var/lib/mysql-files/cost-no-header.csv' INTO TABLE cost FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
(
     cost_id,
     cost_event_id,
     cost_domain_id,
     cost_type_concept_id,
     currency_concept_id,
     @v_total_charge,
     @v_total_cost,
     @v_total_paid,
     @v_paid_by_payer,
     @v_paid_by_patient,
     @v_paid_patient_copay,
     @v_paid_patient_coinsurance,
     @v_paid_patient_deductible,
     @v_paid_by_primary,
     @v_paid_ingredient_cost,
     @v_paid_dispensing_fee,
     @v_payer_plan_period_id,
     @v_amount_allowed,
     @v_revenue_code_concept_id,
     reveue_code_source_value,
     drg_concept_id,
     drg_source_value
)
SET
    total_charge = NULLIF(@v_total_charge, ''),
    total_cost = NULLIF(@v_total_cost, ''),
    total_paid = NULLIF(@v_total_paid, ''),
    paid_by_payer = NULLIF(@v_paid_by_payer, ''),
    paid_by_patient = NULLIF(@v_paid_by_patient, ''),
    paid_patient_copay = NULLIF(@v_paid_patient_copay, ''),
    paid_patient_coinsurance = NULLIF(@v_paid_patient_coinsurance, ''),
    paid_patient_deductible = NULLIF(@v_paid_patient_deductible, ''),
    paid_by_primary = NULLIF(@v_paid_by_primary, ''),
    paid_ingredient_cost = NULLIF(@v_paid_ingredient_cost, ''),
    paid_dispensing_fee = NULLIF(@v_paid_dispensing_fee, ''),
    payer_plan_period_id = NULLIF(@v_payer_plan_period_id, ''),
    amount_allowed = NULLIF(@v_amount_allowed, ''),
    revenue_code_concept_id = NULLIF(@v_revenue_code_concept_id, '');

LOAD DATA   INFILE '/var/lib/mysql-files/person-no-header.csv' INTO TABLE person FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
(
      person_id,
      gender_concept_id,
      year_of_birth,
      month_of_birth,
      day_of_birth,
      @v_birth_datetime,
      race_concept_id,
      ethnicity_concept_id,
      location_id,
      @v_provider_id,
      @v_care_site_id,
      person_source_value,
      gender_source_value,
      @v_gender_source_concept_id,
      race_source_value,
      @v_race_source_concept_id,
      ethnicity_source_value,
      @v_ethnicity_source_concept_id
    )
SET
    birth_datetime = NULLIF(@v_birth_datetime, ''),
    provider_id = NULLIF(@v_provider_id, ''),
    care_site_id = NULLIF(@v_care_site_id, ''),
    gender_source_concept_id = NULLIF(@v_gender_source_concept_id, ''),
    race_source_concept_id = NULLIF(@v_race_source_concept_id, ''),
    ethnicity_source_concept_id = NULLIF(@v_ethnicity_source_concept_id, '')
;

