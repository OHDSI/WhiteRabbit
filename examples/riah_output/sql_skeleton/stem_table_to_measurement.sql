
INSERT INTO measurement
(
	measurement_id,
	person_id,
	measurement_concept_id,
	measurement_date,
	measurement_datetime,
	measurement_time,
	measurement_type_concept_id,
	operator_concept_id,
	value_as_number,
	value_as_concept_id,
	unit_concept_id,
	range_low,
	range_high,
	provider_id,
	visit_occurrence_id,
	visit_detail_id,
	measurement_source_value,
	measurement_source_concept_id,
	unit_source_value,
	value_source_value
)
SELECT
	stem_table.id	AS	measurement_id,

	stem_table.person_id	AS	person_id,

	stem_table.concept_id	AS	measurement_concept_id,

	stem_table.start_date	AS	measurement_date,

	stem_table.start_datetime	AS	measurement_datetime,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	measurement_time,

	stem_table.type_concept_id	AS	measurement_type_concept_id,

	stem_table.operator_concept_id	AS	operator_concept_id,

	stem_table.value_as_number	AS	value_as_number,

	stem_table.value_as_concept_id	AS	value_as_concept_id,

	stem_table.unit_concept_id	AS	unit_concept_id,

	stem_table.range_low	AS	range_low,

	stem_table.range_high	AS	range_high,

	stem_table.provider_id	AS	provider_id,

	stem_table.visit_occurrence_id	AS	visit_occurrence_id,

 -- [!WARNING!] no source column found. See possible comment at the INSERT INTO
	NULL	AS	visit_detail_id,

	stem_table.source_value	AS	measurement_source_value,

	stem_table.source_concept_id	AS	measurement_source_concept_id,

	stem_table.unit_source_value	AS	unit_source_value,

	stem_table.value_source_value	AS	value_source_value

FROM stem_table
;