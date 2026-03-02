SELECT ticket_id, minutes_to_sla_breach, priority
FROM ticket_sla_projection
WHERE minutes_to_sla_breach <= 60;
