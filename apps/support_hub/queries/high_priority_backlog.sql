SELECT ticket_id, created_at, priority, assigned_team
FROM support_tickets
WHERE status IN ('new', 'open')
  AND priority IN ('p0', 'p1');
