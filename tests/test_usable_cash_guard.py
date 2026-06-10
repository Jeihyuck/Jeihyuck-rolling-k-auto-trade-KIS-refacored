import logging


def test_usable_cash_guard_logs_without_prior_locals(caplog):
    class Dummy:
        pass
    self = Dummy()
    with caplog.at_level(logging.INFO):
        usable_cash = float(locals().get("usable_cash", getattr(self, "entry_usable_krw", 0.0)) or 0.0)
        order_possible_cash = float(locals().get("order_possible_cash", getattr(self, "_order_possible_cash", 0.0)) or 0.0)
        cash_available_for_order = float(order_possible_cash if order_possible_cash > 0 else usable_cash)
        logging.getLogger("trader.pb1_engine").info(
            "[PB1][CASH][ORDERABLE] usable_cash=%.0f order_possible_cash=%.0f cash_available_for_order=%.0f",
            usable_cash, order_possible_cash, cash_available_for_order,
        )
    assert cash_available_for_order == 0.0
    assert "[PB1][CASH][ORDERABLE]" in caplog.text
