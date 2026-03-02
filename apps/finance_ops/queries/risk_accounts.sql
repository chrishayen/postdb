SELECT customer_id, risk_score, dso_days
FROM collections_risk
WHERE snapshot_date = CURRENT_DATE;
