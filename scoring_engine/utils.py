import pandas as pd
import numpy as np

def calculate_relative_percentile(series: pd.Series, score_range=(0, 10)) -> pd.Series:
    """
    將一組原始數據轉換為指定範圍內的相對百分位分數。
    
    邏輯：
    1. 計算排名 (rank)
    2. 將排名縮放至 0-1
    3. 映射到 score_range (例如 0-10)
    """
    if series.empty:
        return series
        
    if series.nunique() <= 1:
        # 如果所有馬匹的值都一樣，回傳中間分 (例如 5 分)
        return pd.Series((score_range[0] + score_range[1]) / 2, index=series.index)
    
    # 使用 rank (pct=True) 得到 0 到 1 的百分位
    # pct=True 會處理重複值並給出平均百分比
    percentiles = series.rank(pct=True, method='average')
    
    # 映射到指定範圍
    min_score, max_score = score_range
    return min_score + (percentiles * (max_score - min_score))

def estimate_win_probability(total_scores: pd.Series) -> pd.Series:
    """
    根據總分估計勝出概率 (使用 Softmax 概念)
    """
    # 進行特徵縮放以避免數值不穩定
    x = total_scores - total_scores.mean()
    exp_x = np.exp(x / total_scores.std() if total_scores.std() > 0 else 0)
    return exp_x / exp_x.sum()
