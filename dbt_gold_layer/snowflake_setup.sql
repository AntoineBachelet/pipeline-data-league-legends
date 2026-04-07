-- Create role and user for dbt project, replace your-warehouse, your-schema and your-password with your own Snowflake configuration
CREATE ROLE dbt_role;

GRANT USAGE ON WAREHOUSE your-warehouse TO ROLE dbt_role;

GRANT USAGE ON DATABASE your-schema TO ROLE dbt_role;

GRANT USAGE ON SCHEMA your-schema.SILVER TO ROLE dbt_role;
GRANT SELECT ON ALL TABLES IN SCHEMA your-schema.SILVER TO ROLE dbt_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA your-schema.SILVER TO ROLE dbt_role;

CREATE SCHEMA your-schema.GOLD;

GRANT USAGE ON SCHEMA your-schema.GOLD TO ROLE dbt_role;
GRANT CREATE TABLE ON SCHEMA your-schema.GOLD TO ROLE dbt_role;
GRANT CREATE VIEW ON SCHEMA your-schema.GOLD TO ROLE dbt_role;

CREATE USER dbt_user
  PASSWORD = 'your-password!'
  DEFAULT_ROLE = dbt_role
  DEFAULT_WAREHOUSE = your-warehouse;

GRANT ROLE dbt_role TO USER dbt_user;
