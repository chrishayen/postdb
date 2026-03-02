SELECT customer_id, risk_bucket
FROM churn_model_output
WHERE snapshot_date = CURRENT_DATE;
