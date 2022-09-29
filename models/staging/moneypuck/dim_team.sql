WITH cte_teams AS (
    SELECT DISTINCT
          team,
          season
    FROM `nhl-analysis-dev.data_lake.moneypuck_teams`
ORDER BY season,
         team
)

  SELECT generate_uuid(),
         team,
         season AS year
    FROM cte_teams