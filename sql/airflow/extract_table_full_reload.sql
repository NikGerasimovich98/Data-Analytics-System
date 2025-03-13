TRUNCATE {schema_name_target}.{table_name_target};

INSERT INTO {schema_name_target}.{table_name_target}
SELECT * FROM {schema_name_source}.{table_name_source};