import logging
logger=logging.getLogger(__name__)
def log_decision(**fields): logger.info("[KR_INFINITE][DECISION] %s"," ".join(f"{k}={v}" for k,v in fields.items()))
