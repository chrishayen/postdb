SELECT invoice_id, customer_id, due_date, outstanding_amount
FROM invoices
WHERE status = 'open'
  AND due_date < CURRENT_DATE;
