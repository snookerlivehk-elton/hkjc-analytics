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

def estimate_win_probability(total_scores: pd.Series, temperature: float = 1.0) -> pd.Series:
    if total_scores is None or len(total_scores) == 0:
        return pd.Series([], dtype=float)
    t = float(temperature) if temperature is not None else 1.0
    if t <= 0:
        t = 1.0
    x = total_scores - total_scores.mean()
    std = float(total_scores.std() or 0.0)
    if std > 0:
        z = x / std
    else:
        z = x * 0.0
    z = z / t
    z = z - float(z.max() or 0.0)
    exp_x = np.exp(z)
    denom = float(exp_x.sum() or 0.0)
    if denom <= 0:
        return pd.Series(np.ones(len(total_scores)) / float(len(total_scores) or 1), index=total_scores.index)
    return exp_x / denom
