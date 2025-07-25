WITH /*PLACEHOLDER*/ /*pull-request-creators-map.sql*/ group_by_area AS (
    SELECT
        UPPER(gu.country_code) AS country_or_area,
        COUNT(DISTINCT ge.actor_login) as cnt
    FROM github_events ge
    LEFT JOIN github_users gu ON ge.actor_login = gu.login
    WHERE
        ge.repo_id IN (41986369)
        AND ge.type = 'PullRequestEvent'
        AND ge.action = 'opened'
        AND gu.country_code IS NOT NULL  -- TODO: remove
        AND gu.country_code != ''
        AND gu.country_code != 'N/A'
        AND gu.country_code != 'UND'
    GROUP BY country_or_area
    ORDER BY cnt DESC
), summary AS (
    SELECT SUM(cnt) AS total FROM group_by_area
)
SELECT /*PLACEHOLDER*/ /*pull-request-creators-map.sql*/ 
    country_or_area,
    cnt AS count,
    cnt / summary.total AS percentage
FROM group_by_area, summary
;
