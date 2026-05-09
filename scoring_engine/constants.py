DISABLED_FACTORS = {
    "gear_change",
    "going_specialty",
    "morning_trial_perf",
    "odds_movement",
    "pace_analysis",
    "recent_running_style",
    "vet_rest_days",
}

SCORING_ENGINE_VERSION = "2026-05-10.1"

FACTOR_ALGO_VERSIONS = {
    "draw_stats": "2",
    "style_trkprof_edge": "3",
}


def factor_algo_version(factor_name: str) -> str:
    fn = str(factor_name or "").strip()
    return str(FACTOR_ALGO_VERSIONS.get(fn) or "1")
