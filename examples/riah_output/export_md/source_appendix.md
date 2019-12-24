# Appendix: source tables

### Table: patients.csv

| Field | Type | Most freq. value | Comment |
| --- | --- | --- | --- |
| ﻿id | varchar | List truncated... |  |
| birthdate | date | 1908-08-17 |  |
| deathdate | date |  |  |
| ssn | varchar | List truncated... |  |
| drivers | varchar |  |  |
| passport | varchar |  |  |
| prefix | varchar | Mr. |  |
| first | varchar | List truncated... |  |
| last | varchar | Haag279 |  |
| suffix | varchar |  |  |
| maiden | varchar |  |  |
| marital | varchar | M |  |
| race | varchar | white |  |
| ethnicity | varchar | irish |  |
| gender | varchar | M |  |
| birthplace | varchar | Boston |  |
| address | varchar | List truncated... |  |
| city | varchar | Boston |  |
| state | varchar | Massachusetts |  |
| zip | int | 02108 |  |

### Table: allergies.csv

| Field | Type | Most freq. value | Comment |
| --- | --- | --- | --- |
| start | date | 2011-01-09 |  |
| stop | date |  |  |
| patient | varchar | 85a25223-0837-40a4-9bf1-932bdd64016b |  |
| encounter | varchar | fa3fe7ba-bea9-4aa4-963c-391ff0fa34a4 |  |
| code | int | 419474003 |  |
| description | varchar | Allergy to mould |  |

### Table: careplans.csv

| Field | Type | Most freq. value | Comment |
| --- | --- | --- | --- |
| id | varchar | List truncated... |  |
| start | date | List truncated... |  |
| stop | date |  |  |
| patient | varchar | 7f2d94ac-081b-43b7-8cb5-2d2fd9552899 |  |
| encounter | varchar | List truncated... |  |
| code | int | 53950000 |  |
| description | varchar | Respiratory therapy |  |
| reasoncode | int | 10509002 |  |
| reasondescription | varchar | Acute bronchitis (disorder) |  |

### Table: conditions.csv

| Field | Type | Most freq. value | Comment |
| --- | --- | --- | --- |
| start | date | 2013-01-10 |  |
| stop | date |  |  |
| patient | varchar | c7751bda-c4bb-4334-a578-3314685f41b8 |  |
| encounter | varchar | 3b44ae5b-077c-4b83-a377-8c4cdc449569 |  |
| code | int | 444814009 |  |
| description | varchar | Viral sinusitis (disorder) |  |

### Table: encounters.csv

| Field | Type | Most freq. value | Comment |
| --- | --- | --- | --- |
| id | varchar | List truncated... |  |
| start | varchar | List truncated... |  |
| stop | varchar | List truncated... |  |
| patient | varchar | 18d59307-54ab-42d7-b998-e76ac1ee923f |  |
| provider | varchar | 340f836c-8b1f-45e7-91f8-498de5fbb320 |  |
| encounterclass | varchar | ambulatory |  |
| code | int | 185349003 |  |
| description | varchar | Encounter for check up (procedure) |  |
| cost | real | 129.16 |  |
| reasoncode | int |  |  |
| reasondescription | varchar |  |  |

### Table: imaging_studies.csv

| Field | Type | Most freq. value | Comment |
| --- | --- | --- | --- |
| id | varchar | List truncated... |  |
| date | date | List truncated... |  |
| patient | varchar | a9edebda-6b2d-43d0-bcce-2ecbb83cae9a |  |
| encounter | varchar | List truncated... |  |
| bodysite_code | int | 51185008 |  |
| bodysite_description | varchar | Thoracic structure (body structure) |  |
| modality_code | varchar | DX |  |
| modality_description | varchar | Digital Radiography |  |
| sop_code | varchar | 1.2.840.10008.5.1.4.1.1.1.1 |  |
| sop_description | varchar | Digital X-Ray Image Storage |  |

### Table: immunizations.csv

| Field | Type | Most freq. value | Comment |
| --- | --- | --- | --- |
| date | date | 2014-01-18 |  |
| patient | varchar | 74b85400-824b-4fda-8a3b-d6e20de4599d |  |
| encounter | varchar | aaecdacd-361e-42bb-9a4c-c2999b6b38f4 |  |
| code | int | 140 |  |
| description | varchar | Influenza  seasonal  injectable  preservative free |  |
| cost | real | 140.52 |  |

### Table: medications.csv

| Field | Type | Most freq. value | Comment |
| --- | --- | --- | --- |
| start | date | 2014-04-04 |  |
| stop | date |  |  |
| patient | varchar | d3814387-d926-4d1b-acb1-55dce114b3d5 |  |
| encounter | varchar | dec7038a-8d28-4f45-9f2f-0dd71c5a7d86 |  |
| code | int | 316672 |  |
| description | varchar | Simvistatin 10 MG |  |
| cost | real | 263.49 |  |
| dispenses | int | 1 |  |
| totalcost | real | 3161.88 |  |
| reasoncode | int |  |  |
| reasondescription | varchar |  |  |

### Table: observations.csv

| Field | Type | Most freq. value | Comment |
| --- | --- | --- | --- |
| date | date | 2011-07-12 |  |
| patient | varchar | a9edebda-6b2d-43d0-bcce-2ecbb83cae9a |  |
| encounter | varchar | 855e14bc-c81a-40e8-aa2d-327466ba6517 |  |
| code | varchar | 72514-3 |  |
| description | varchar | Pain severity - 0-10 verbal numeric rating [Score] - Reported |  |
| value | varchar | Never smoker |  |
| units | varchar | mg/dL |  |
| type | varchar | numeric |  |

### Table: organizations.csv

| Field | Type | Most freq. value | Comment |
| --- | --- | --- | --- |
| id | varchar | List truncated... |  |
| name | varchar | List truncated... |  |
| address | varchar | List truncated... |  |
| city | varchar | List truncated... |  |
| state | varchar | MA |  |
| zip | int | List truncated... |  |
| phone | varchar | List truncated... |  |
| utilization | int | 1 |  |

### Table: procedures.csv

| Field | Type | Most freq. value | Comment |
| --- | --- | --- | --- |
| date | date | 2012-12-17 |  |
| patient | varchar | 18d59307-54ab-42d7-b998-e76ac1ee923f |  |
| encounter | varchar | e044ca0e-ff73-4e34-b557-bb301ea930d1 |  |
| code | varchar | 428191000124101 |  |
| description | varchar | Documentation of current medications |  |
| cost | real | 516.65 |  |
| reasoncode | int | 72892002 |  |
| reasondescription | varchar | Normal pregnancy |  |

### Table: providers.csv

| Field | Type | Most freq. value | Comment |
| --- | --- | --- | --- |
| id | empty |  |  |
| organization | empty |  |  |
| name | empty |  |  |
| gender | empty |  |  |
| speciality | empty |  |  |
| address | empty |  |  |
| city | empty |  |  |
| state | empty |  |  |
| zip | empty |  |  |
| utilization | empty |  |  |

### Table: stem_table

| Field | Type | Most freq. value | Comment |
| --- | --- | --- | --- |
| domain_id | CHARACTER VARYING |  |  |
| person_id | INTEGER |  |  |
| visit_occurrence_id | INTEGER |  |  |
| provider_id | INTEGER |  |  |
| id | INTEGER |  |  |
| concept_id | INTEGER |  |  |
| source_value | CHARACTER VARYING |  |  |
| source_concept_id | INTEGER |  |  |
| type_concept_id | INTEGER |  |  |
| start_date | DATE |  |  |
| start_datetime | DATETIME |  |  |
| end_date | DATE |  |  |
| end_datetime | DATETIME |  |  |
| verbatim_end_date | DATE |  |  |
| days_supply | INTEGER |  |  |
| dose_unit_source_value | CHARACTER VARYING |  |  |
| lot_number | CHARACTER VARYING |  |  |
| modifier_concept_id | INTEGER |  |  |
| operator_concept_id | INTEGER |  |  |
| modifier_concept_id | INTEGER |  |  |
| modifier_source_value | CHARACTER VARYING |  |  |
| quantity | INTEGER |  |  |
| range_high | FLOAT |  |  |
| range_low | FLOAT |  |  |
| refills | INTEGER |  |  |
| route_concept_id | INTEGER |  |  |
| route_source_value | CHARACTER VARYING |  |  |
| sig | CHARACTER VARYING |  |  |
| stop_reason | CHARACTER VARYING |  |  |
| unique_device_id | CHARACTER VARYING |  |  |
| unit_concept_id | INTEGER |  |  |
| unit_source_value | CHARACTER VARYING |  |  |
| value_as_concept_id | INTEGER |  |  |
| value_as_number | DECIMAL |  |  |
| value_as_string | CHARACTER VARYING |  |  |
| value_source_value | CHARACTER VARYING |  |  |
| anatomic_site_concept_id | INTEGER |  |  |
| disease_status_concept_id | INTEGER |  |  |
| specimen_source_id | INTEGER |  |  |
| anatomic_site_source_value | CHARACTER VARYING |  |  |
| disease_status_source_value | CHARACTER VARYING |  |  |
| condition_status_concept_id | CHARACTER VARYING |  |  |
| condition_status_source_value | INTEGER |  |  |
| qualifier_concept_id | INTEGER |  |  |
| qualifier_source_value | CHARACTER VARYING |  |  |

