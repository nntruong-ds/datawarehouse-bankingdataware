select
    entry_id,
    journal_id,
    posting_date,
    branch_id,
    gl_account,
    upper(currency) as currency,
    coalesce(debit_amount, 0) as debit_amount,
    coalesce(credit_amount, 0) as credit_amount,
    source_txn_id,
    ingested_at
from {{ source('core', 'gl_entries') }}
