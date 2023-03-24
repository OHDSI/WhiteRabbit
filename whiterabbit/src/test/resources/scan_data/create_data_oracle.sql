/* This is a stripped (only the tables needed are created) of OMOP CDM V5.2 */


CREATE TABLE test_user.person
(
    person_id						INTEGER		NOT NULL ,
    gender_concept_id				INTEGER		NOT NULL ,
    year_of_birth					INTEGER		NOT NULL ,
    month_of_birth					INTEGER		NULL,
    day_of_birth					INTEGER		NULL,
    birth_datetime					TIMESTAMP WITH TIME ZONE	NULL,
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

CREATE TABLE test_user.cost
(
    cost_id					INTEGER	    NOT NULL ,
    cost_event_id       INTEGER     NOT NULL ,
    cost_domain_id       VARCHAR2(20)    NOT NULL ,
    cost_type_concept_id       INTEGER     NOT NULL ,
    currency_concept_id			INTEGER			NULL ,
    total_charge						FLOAT			NULL ,
    total_cost						FLOAT			NULL ,
    total_paid						FLOAT			NULL ,
    paid_by_payer					FLOAT			NULL ,
    paid_by_patient						FLOAT			NULL ,
    paid_patient_copay						FLOAT			NULL ,
    paid_patient_coinsurance				FLOAT			NULL ,
    paid_patient_deductible			FLOAT			NULL ,
    paid_by_primary						FLOAT			NULL ,
    paid_ingredient_cost				FLOAT			NULL ,
    paid_dispensing_fee					FLOAT			NULL ,
    payer_plan_period_id			INTEGER			NULL ,
    amount_allowed		FLOAT			NULL ,
    revenue_code_concept_id		INTEGER			NULL ,
    reveue_code_source_value    VARCHAR2(50)   NULL ,
    drg_concept_id			INTEGER		NULL,
    drg_source_value		VARCHAR(3)		NULL
)
;

