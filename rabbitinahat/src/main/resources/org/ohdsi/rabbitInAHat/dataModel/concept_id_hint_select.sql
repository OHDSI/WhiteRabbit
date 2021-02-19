SELECT vocabulary_version from @vocab.vocabulary WHERE vocabulary_id = 'None';

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
    WHERE domain_id = 'Gender'  -- Also include invalid concepts 'UNKNOWN' and 'OTHER'
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
      AND invalid_reason IS NULL
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
      AND invalid_reason IS NULL
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
    WHERE vocabulary_id = 'Type Concept'
      AND invalid_reason IS NULL
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
    WHERE vocabulary_id = 'Type Concept'
      AND invalid_reason IS NULL
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
    WHERE vocabulary_id = 'Type Concept'
      AND invalid_reason IS NULL
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
    WHERE vocabulary_id = 'Type Concept'
      AND invalid_reason IS NULL
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
    WHERE vocabulary_id = 'Type Concept'
      AND invalid_reason IS NULL
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
    WHERE vocabulary_id = 'Type Concept'
      AND invalid_reason IS NULL
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
    WHERE vocabulary_id = 'Type Concept'
      AND invalid_reason IS NULL
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
    WHERE vocabulary_id = 'Type Concept'
      AND invalid_reason IS NULL
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
    WHERE vocabulary_id = 'Type Concept'
      AND invalid_reason IS NULL
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
    WHERE vocabulary_id = 'Type Concept'
      AND invalid_reason IS NULL
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
    WHERE vocabulary_id = 'Type Concept'
      AND invalid_reason IS NULL
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
    WHERE vocabulary_id = 'Type Concept'
      AND invalid_reason IS NULL
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
    WHERE vocabulary_id = 'Type Concept'
      AND invalid_reason IS NULL
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
      AND invalid_reason IS NULL
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
      AND invalid_reason IS NULL
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
      AND invalid_reason IS NULL
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
      AND invalid_reason IS NULL
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
      AND invalid_reason IS NULL
    UNION ALL
    SELECT 'note'                AS omop_cdm_table,
           'language_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept_ancestor
        JOIN vocab.concept ON concept_id = descendant_concept_id
    WHERE ancestor_concept_id = 4182347  -- World languages
      AND invalid_reason IS NULL
    UNION ALL
    SELECT 'condition_occurrence'        AS omop_cdm_table,
           'condition_status_concept_id' AS omop_cdm_field,
           concept_id,
           concept_name,
           domain_id,
           vocabulary_id,
           concept_class_id,
           standard_concept
    FROM @vocab.concept
    WHERE domain_id = 'Condition Status'
      AND invalid_reason IS NULL
)
SELECT *
FROM concept_hints
ORDER BY omop_cdm_table, omop_cdm_field, standard_concept, concept_name, domain_id, concept_class_id, vocabulary_id, concept_id ASC
;
