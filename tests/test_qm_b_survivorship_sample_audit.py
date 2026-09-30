from pathlib import Path
import json
from scanner.research.governance.qm_b_survivorship_sample_audit import scan_repository


def test_candidate_requires_historical_and_current_markers(tmp_path: Path):
    (tmp_path/'src').mkdir()
    (tmp_path/'scripts').mkdir()
    (tmp_path/'configs').mkdir()
    contract=json.loads(Path('configs/qm_b_survivorship_sample_audit_v1.json').read_text())
    contract['known_safe_paths']={}
    cp=tmp_path/'contract.json'; cp.write_text(json.dumps(contract))
    (tmp_path/'src'/'safe.py').write_text("x='history_analysis.csv'")
    (tmp_path/'src'/'candidate.py').write_text("a='history_recent.csv'; b='universe_master.csv'")
    result=scan_repository(tmp_path, contract_path=cp)
    assert result['candidate_count']==1
    assert result['candidates'][0]['path']=='src/candidate.py'
    assert result['candidates'][0]['classification']=='REVIEW_REQUIRED'
    assert result['defect_claims_performed'] is False


def test_current_repo_scan_is_reproducible():
    result=scan_repository(Path('.'))
    assert result['files_scanned'] > 0
    assert result['historical_retrojection_permitted'] is False
    assert all(row['classification'] in {'SAFE','REVIEW_REQUIRED'} for row in result['candidates'])
