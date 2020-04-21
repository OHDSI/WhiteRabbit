-- SELECT vocabulary_version from @vocab.vocabulary WHERE vocabulary_id = 'None';

WITH concept_hints AS (
    SELECT 'person'            AS omop_cdm_table,
           'gender_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Gender'
    UNION ALL
    SELECT 'person'               AS omop_cdm_table,
           'ethnicity_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Ethnicity'
    UNION ALL
    SELECT 'person'          AS omop_cdm_table,
           'race_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Race'
    UNION ALL
    SELECT 'condition_occurrence'      AS omop_cdm_table,
           'condition_type_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Type Concept'
      AND concept_class_id = 'Condition Type'
    UNION ALL
    SELECT 'cost'                 AS omop_cdm_table,
           'cost_type_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Type Concept'
      AND concept_class_id = 'Cost Type'
    UNION ALL
    SELECT 'death'                 AS omop_cdm_table,
           'death_type_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Type Concept'
      AND concept_class_id = 'Death Type'
    UNION ALL
    SELECT 'device_exposure'        AS omop_cdm_table,
           'device_type_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Type Concept'
      AND concept_class_id = 'Device Type'
    UNION ALL
    SELECT 'drug_exposure'        AS omop_cdm_table,
           'drug_type_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Type Concept'
      AND concept_class_id = 'Drug Type'
    UNION ALL
    SELECT 'episode'                 AS omop_cdm_table,
           'episode_type_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Type Concept'
      AND concept_class_id = 'Episode Type'
    UNION ALL
    SELECT 'measurement'                 AS omop_cdm_table,
           'measurement_type_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Type Concept'
      AND concept_class_id = 'Meas Type'
    UNION ALL
    SELECT 'note'                 AS omop_cdm_table,
           'note_type_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Type Concept'
      AND concept_class_id = 'Note Type'
    UNION ALL
    SELECT 'observation_period'     AS omop_cdm_table,
           'period_type_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Type Concept'
      AND concept_class_id = 'Obs Period Type'
    UNION ALL
    SELECT 'observation'                 AS omop_cdm_table,
           'observation_type_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Type Concept'
      AND concept_class_id = 'Observation Type'
    UNION ALL
    SELECT 'procedure_occurrence'      AS omop_cdm_table,
           'procedure_type_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Type Concept'
      AND concept_class_id = 'Procedure Type'
    UNION ALL
    SELECT 'specimen'                 AS omop_cdm_table,
           'specimen_type_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Type Concept'
      AND concept_class_id = 'Specimen Type'
    UNION ALL
    SELECT 'visit_occurrence'      AS omop_cdm_table,
           'visit_type_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Type Concept'
      AND concept_class_id = 'Visit Type'
    UNION ALL
    SELECT 'visit_occurrence' AS omop_cdm_table,
           'visit_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Visit'
    UNION ALL
    SELECT 'cost'            AS omop_cdm_table,
           'cost_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Cost'
    UNION ALL
    SELECT 'payer_plan_period' AS omop_cdm_table,
           'plan_concept_id'   AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Plan'
    UNION ALL
    SELECT 'drug_exposure'    AS omop_cdm_table,
           'route_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Route'
    UNION ALL
    SELECT 'measurement'         AS omop_cdm_table,
           'operator_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Meas Value Operator'
)
SELECT *
FROM concept_hints
ORDER BY omop_cdm_table, omop_cdm_field, standard_concept, concept_name, domain_id, concept_class_id, vocabulary_id
;
