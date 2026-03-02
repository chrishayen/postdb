SELECT account_id, onboarding_stage, owner_user_id
FROM onboarding_pipeline
WHERE is_open = true;
