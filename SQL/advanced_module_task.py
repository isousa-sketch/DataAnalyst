WITH account_country AS (
  SELECT
    acs.account_id,
    ANY_VALUE(sp.country) AS country
  FROM `DA.account_session` acs
  JOIN `DA.session_params` sp
    ON acs.ga_session_id = sp.ga_session_id
  GROUP BY acs.account_id
),


-- MÉTRICAS DE CONTA
account_metrics AS (
  SELECT
    DATE(se.date) AS date,
    ac.country,
    a.send_interval,
    a.is_verified,
    a.is_unsubscribed,
    COUNT(DISTINCT a.id) AS account_cnt,
    0 AS sent_msg,
    0 AS open_msg,
    0 AS visit_msg
  FROM `DA.account` a
  LEFT JOIN account_country ac
    ON a.id = ac.account_id
  LEFT JOIN `DA.account_session` acs
    ON acs.account_id = a.id
  LEFT JOIN `DA.session` se
    ON acs.ga_session_id = se.ga_session_id
  GROUP BY
    se.date,
    ac.country,
    a.send_interval,
    a.is_verified,
    a.is_unsubscribed
),


-- MÉTRICAS DE E-MAIL (baseado na data de envio)
email_metrics AS (
  SELECT
    DATE_ADD(se.date, INTERVAL es.sent_date DAY) AS date,
    ac.country,
    a.send_interval,
    a.is_verified,
    a.is_unsubscribed,
    0 AS account_cnt,
    COUNT(DISTINCT es.id_message) AS sent_msg,
    COUNT(DISTINCT eo.id_message) AS open_msg,
    COUNT(DISTINCT ev.id_message) AS visit_msg
  FROM `DA.email_sent` es
  JOIN `DA.account` a
    ON es.id_account = a.id
  LEFT JOIN account_country ac
    ON a.id = ac.account_id
  LEFT JOIN `DA.email_open` eo
    ON es.id_message = eo.id_message
  LEFT JOIN `DA.email_visit` ev
    ON es.id_message = ev.id_message
  LEFT JOIN `DA.account_session` acs
    ON acs.account_id = a.id
  LEFT JOIN `DA.session` se
    ON acs.ga_session_id = se.ga_session_id
  GROUP BY
    date,
    ac.country,
    a.send_interval,
    a.is_verified,
    a.is_unsubscribed
),


-- UNION DAS MÉTRICAS
base_metrics AS (
  SELECT * FROM account_metrics
  UNION ALL
  SELECT * FROM email_metrics
),


final_groups AS (
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    SUM(account_cnt) AS account_cnt,
    SUM(sent_msg) AS sent_msg,
    SUM(open_msg) AS open_msg,
    SUM(visit_msg) AS visit_msg
  FROM base_metrics
  GROUP BY
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed
),


-- TOTAIS POR PAÍS (já usando a base agregada)
country_totals AS (
  SELECT
    country,
    SUM(account_cnt) AS total_country_account_cnt,
    SUM(sent_msg) AS total_country_sent_cnt
  FROM final_groups
  WHERE country IS NOT NULL
  GROUP BY country
),


-- RANKINGS
country_ranks AS (
  SELECT
    country,
    total_country_account_cnt,
    total_country_sent_cnt,
    RANK() OVER (ORDER BY total_country_account_cnt DESC)
      AS rank_total_country_account_cnt,
    RANK() OVER (ORDER BY total_country_sent_cnt DESC)
      AS rank_total_country_sent_cnt
  FROM country_totals
)


-- RESULTADO FINAL
SELECT
  fg.date,
  fg.country,
  fg.send_interval,
  fg.is_verified,
  fg.is_unsubscribed,
  fg.account_cnt,
  fg.sent_msg,
  fg.open_msg,
  fg.visit_msg,
  cr.total_country_account_cnt,
  cr.total_country_sent_cnt,
  cr.rank_total_country_account_cnt,
  cr.rank_total_country_sent_cnt
FROM final_groups fg
LEFT JOIN country_ranks cr
  ON fg.country = cr.country
WHERE
  cr.country IS NOT NULL
  AND (
    cr.rank_total_country_account_cnt <= 10
    OR cr.rank_total_country_sent_cnt <= 10
  )
ORDER BY
  COALESCE(cr.rank_total_country_account_cnt, 999),
  COALESCE(cr.rank_total_country_sent_cnt, 999),
  fg.country,
  fg.date;
