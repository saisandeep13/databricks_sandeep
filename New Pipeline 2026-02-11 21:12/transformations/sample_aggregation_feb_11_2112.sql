-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

CREATE MATERIALIZED VIEW sample_aggregation_feb_11_2112 AS
SELECT
    user_type,
    COUNT(user_type) AS total_count
FROM sample_users_feb_11_2112
GROUP BY user_type;
