SELECT account_id, onboarding_stage
FROM onboarding_pipeline
WHERE is_open = true;
