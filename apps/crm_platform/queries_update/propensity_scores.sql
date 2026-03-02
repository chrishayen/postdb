SELECT account_id, propensity_to_buy
FROM propensity_model_output
WHERE snapshot_date = CURRENT_DATE;
