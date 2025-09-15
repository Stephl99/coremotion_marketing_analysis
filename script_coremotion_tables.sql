-- drop tables 
DROP TABLE IF EXISTS  `coremotion_data.regions`;
DROP TABLE IF EXISTS `coremotion_data.calendar_weeks`;
DROP TABLE IF EXISTS `coremotion_data.sales_r`;
DROP TABLE IF EXISTS `coremotion_data.macroeconomic_week`;
DROP TABLE IF EXISTS `coremotion_data.banner_r`;
DROP TABLE IF EXISTS `coremotion_data.olv_pivot`;
DROP TABLE IF EXISTS `coremotion_data.social_r`;
DROP TABLE IF EXISTS `coremotion_data.search_r`;
DROP TABLE IF EXISTS `coremotion_data.ooh_agg`;
DROP TABLE IF EXISTS `coremotion_data.tv_pivot`;
DROP TABLE IF EXISTS `coremotion_data.competitors_pivot`;

DROP TABLE IF EXISTS  `coremotion_data.main_week_table`;

-- regions
CREATE OR REPLACE TABLE `coremotion_data.regions` AS
SELECT DISTINCT Region AS region
FROM `coremotion_data.sales`;

-- weeks
CREATE OR REPLACE TABLE `coremotion_data.calendar_weeks` AS
SELECT
  DISTINCT DATE_TRUNC(Week, WEEK(SUNDAY)) AS week_start,
  EXTRACT(YEAR FROM DATE_TRUNC(Week, WEEK(SUNDAY))) AS year,
  EXTRACT(ISOWEEK FROM DATE_TRUNC(Week, WEEK(SUNDAY))) AS week_of_year
FROM `coremotion_data.sales`;

-- sales - rename and order
CREATE OR REPLACE TABLE `coremotion_data.sales_r` AS
SELECT
  Week   AS week_start,
  Region AS region,
  Dollar_Sales,
  Units_Sales
FROM `coremotion_data.sales`
ORDER BY region, week_start;

-- Variables macro de mensual a semanal - step hold para tomar datos mensuales por cada semana
CREATE OR REPLACE TABLE `coremotion_data.macroeconomic_week` AS
SELECT
  w.week_start,
  m.PIB_Growth   AS gdp_growth,
  m.Inflation    AS inflation,
  m.Unemployment AS unemployment
FROM `coremotion_data.calendar_weeks` w
LEFT JOIN `coremotion_data.macroeconomic` m
  ON DATE_TRUNC(m.Month, MONTH) = DATE_TRUNC(w.week_start, MONTH)
ORDER BY w.week_start;

-- banner - rename cols
CREATE OR REPLACE TABLE `coremotion_data.banner_r` AS
SELECT
  Date AS week_start,
  Impressions AS banner_impressions,
  ` Cost` AS banner_cost
FROM `coremotion_data.banner`
ORDER BY week_start;

-- olv - pivot Variable field
CREATE OR REPLACE TABLE `coremotion_data.olv_pivot` AS
SELECT
  Period as week_start,
  MAX(CASE WHEN Variable = 'OLV Impressions' THEN Value END) AS olv_impressions,
  MAX(CASE WHEN TRIM(Variable) = 'OLV Spend'       THEN Value END) AS spend_olv
FROM `coremotion_data.olv`   
GROUP BY 1
ORDER BY week_start;

-- social - rename cols
CREATE OR REPLACE TABLE `coremotion_data.social_r` AS
SELECT
  week as week_start,
  `Activity: Impressions` AS social_impressions,
  Investment AS social_investment
FROM `coremotion_data.social`
ORDER BY week_start;

-- search - rename cols
CREATE OR REPLACE TABLE `coremotion_data.search_r` AS
SELECT
  Week AS week_start,
  Clicks AS search_clicks,
  `Search Cost` AS search_cost
FROM `coremotion_data.search`
ORDER BY week_start;

-- ooh - agg per week - region, cast impressions n spend correctly
CREATE OR REPLACE TABLE `coremotion_data.ooh_agg` AS
SELECT
  DATE_TRUNC(CAST(Week AS DATE), WEEK(SUNDAY)) AS week_start,
  Region AS region,
  SUM(CAST(REGEXP_REPLACE(CAST(`OOH Impressions` AS STRING), r'[^0-9]', '') AS INT64)) AS ooh_impressions,
  SUM(CAST(REGEXP_REPLACE(CAST(` Spend ` AS STRING),          r'[^0-9]', '') AS INT64)) AS ooh_spend
FROM `coremotion_data.ooh`
GROUP BY week_start, region
ORDER BY region, week_start;

-- TV - pivot field Metric
CREATE OR REPLACE TABLE `coremotion_data.tv_pivot` AS
SELECT
  Week AS week_start,
  MAX(CASE WHEN Metric = 'Investment AD 18-54' THEN Values END) AS tv_investment,
  MAX(CASE WHEN Metric = 'GRPs AD 18-54'       THEN Values END) AS tv_GRPs_AD
FROM `coremotion_data.tv`   
GROUP BY week_start
ORDER BY week_start;

-- Competitors- pivot competitor field
CREATE OR REPLACE TABLE `coremotion_data.competitors_pivot` AS
SELECT
  Region AS region,
  Week AS week_start,
  SUM(CASE WHEN competitor = 'Competitor A' THEN spend ELSE 0 END) AS compA_spend,
  SUM(CASE WHEN competitor = 'Competitor B' THEN spend ELSE 0 END) AS compB_spend
FROM `coremotion_data.competitors`
GROUP BY 1,2
ORDER BY Region, Week;

-- main table
CREATE OR REPLACE TABLE `coremotion_data.main_week_table` AS
SELECT  
  s.week_start, -- keys
  s.region,
  s.Dollar_Sales, -- sales
  s.Units_Sales,
  m_week.gdp_growth, -- macro
  m_week.inflation,
  m_week.unemployment,
  banner_r.banner_impressions,   -- banners
  banner_r.banner_cost,
  olv_pivot.olv_impressions, -- olv
  olv_pivot.spend_olv,
  social_r.social_impressions, -- social
  social_r.social_investment,
  search_r.search_clicks, -- search
  search_r.search_cost,
  ooh_agg.ooh_impressions, -- ooh
  ooh_agg.ooh_spend,
  tv_pivot.tv_investment, -- tv
  tv_pivot.tv_GRPs_AD,
  competitors_pivot.compA_spend, -- competitors
  competitors_pivot.compB_spend
FROM `coremotion_data.sales_r` s
LEFT JOIN `coremotion_data.macroeconomic_week` m_week
  ON s.week_start = m_week.week_start
LEFT JOIN `coremotion_data.banner_r` banner_r
  ON s.week_start = banner_r.week_start
LEFT JOIN `coremotion_data.olv_pivot` olv_pivot
  ON s.week_start = olv_pivot.week_start
LEFT JOIN `coremotion_data.social_r` social_r
  ON s.week_start = social_r.week_start
LEFT JOIN `coremotion_data.search_r` search_r
  ON s.week_start = search_r.week_start
LEFT JOIN `coremotion_data.ooh_agg` ooh_agg
  ON s.week_start = ooh_agg.week_start
LEFT JOIN `coremotion_data.tv_pivot` tv_pivot
  ON s.week_start = tv_pivot.week_start
LEFT JOIN `coremotion_data.competitors_pivot` competitors_pivot
  ON s.week_start = competitors_pivot.week_start
  AND s.region = competitors_pivot.region
ORDER BY s.region, s.week_start;