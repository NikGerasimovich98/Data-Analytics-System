SELECT
      db_source_name,
      schema_name_source,
      table_name_source,
      db_target_name,
      schema_name_target,
      table_name_target
FROM config.configuration
WHERE reload = 1 and schema_name_source = {schema_parsing}