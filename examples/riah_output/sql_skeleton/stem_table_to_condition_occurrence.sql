
INSERT INTO condition_occurrence
(
	condition_occurrence_id,
	person_id,
	condition_concept_id,
	condition_start_date,
	condition_start_datetime,
	condition_end_date,
	condition_end_datetime,
	condition_type_concept_id,
	stop_reason,
	provider_id,
	visit_occurrence_id,
	visit_detail_id,
	condition_source_value,
	condition_source_concept_id,
	condition_status_source_value,
	condition_status_concept_id
)
SELECT
	stem_table.id	AS	condition_occurrence_id,

	stem_table.person_id	AS	person_id,

	stem_table.concept_id	AS	condition_concept_id,

	stem_table.start_date	AS	condition_start_date,

	stem_table.start_datetime	AS	condition_start_datetime,

	stem_table.end_date	AS	condition_end_date,

	stem_table.end_datetime	AS	condition_end_datetime,

	stem_table.type_concept_id	AS	condition_type_concept_id,

	stem_table.stop_reason	AS	stop_reason,

	stem_table.provider_id	AS	provider_id,

	stem_table.visit_occurrence_id	AS	visit_occurrence_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	visit_detail_id,

	stem_table.source_value	AS	condition_source_value,

	stem_table.source_concept_id	AS	condition_source_concept_id,

	stem_table.condition_status_source_value	AS	condition_status_source_value,

	stem_table.condition_status_concept_id	AS	condition_status_concept_id

FROM stem_table
;