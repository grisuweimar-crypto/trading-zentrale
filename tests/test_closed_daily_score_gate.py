import pandas as pd
from scanner.app import score_step

def test_scoring_refuses_unverified_bar_even_when_engine_could_score(monkeypatch):
    monkeypatch.setenv("SCANNER_CLOSED_DAILY_ONLY", "1")
    def fake_engine(*args, **kwargs):
        return {"score": 80, "opportunity_score": 90, "risk_score": 10,
                "confidence_score": 80}
    monkeypatch.setattr(score_step, "calculate_scores_v6_from_row", fake_engine)
    df=pd.DataFrame([{"Ticker":"UEC","Akt. Kurs":10.0,
                      "price_bar_status":"NO_ELIGIBLE_CLOSED_BAR"}])
    result=score_step.apply_scoring(df)
    assert pd.isna(result.loc[0, "Score"])
    assert pd.isna(result.loc[0, "RiskScore"])
    assert result.loc[0,"ScoreError"]=="PRICE_BAR_NOT_VERIFIED"

def test_scoring_refuses_absent_bar_provenance(monkeypatch):
    monkeypatch.setenv("SCANNER_CLOSED_DAILY_ONLY", "1")
    monkeypatch.setattr(score_step, "calculate_scores_v6_from_row",
                        lambda *a, **k: {"score": 80})
    df=pd.DataFrame([{"Ticker":"UEC","Akt. Kurs":10.0}])
    result=score_step.apply_scoring(df)
    assert pd.isna(result.loc[0, "Score"])
    assert result.loc[0,"ScoreError"]=="PRICE_BAR_NOT_VERIFIED"
