SELECT account_id, score, model_version
FROM lead_scores
WHERE active = true
  AND model_version = 'v2';
