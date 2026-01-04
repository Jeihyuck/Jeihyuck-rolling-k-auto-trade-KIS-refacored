import numpy as np


def get_best_k_meta(backtest_data_y, backtest_data_q, backtest_data_m):
    """
    연도/분기/월 시뮬레이션 결과 중 Sharpe Ratio가 가장 높은 K값을 선택
    """
    all_sets = {
        "year": backtest_data_y,
        "quarter": backtest_data_q,
        "month": backtest_data_m,
    }

    best_option = None
    best_score = -np.inf

    for label, dataset in all_sets.items():
        if not dataset:
            continue
        best_result = max(dataset, key=lambda x: x["sharpe"])
        if best_result["sharpe"] > best_score:
            best_score = best_result["sharpe"]
            best_option = best_result

    return best_option["k"] if best_option else 0.5
