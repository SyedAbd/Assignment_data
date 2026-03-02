-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

CREATE OR REFRESH MATERIALIZED VIEW sample_users_assignment_marketing_silver AS
SELECT
    user_id,
    email,
    name,
    user_type
FROM samples.wanderbricks.users;
