import sqlalchemy as sa

from trader.execution_claims import build_execution_claim_tables


metadata = sa.MetaData()
us_execution_claims, us_execution_attempts = build_execution_claim_tables(
    metadata, "us_execution_claims", "us_execution_attempts",
)
