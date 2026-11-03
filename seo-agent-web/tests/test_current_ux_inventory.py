"""A fresh inventory must describe current code without replacing the historical audit."""
import runpy
import subprocess
import sys
from pathlib import Path

WEB = Path(__file__).resolve().parents[1]
SCRIPT = WEB / 'ops' / 'ux_inventory.py'


def test_current_routes_helper_templates_and_translated_articles_are_inventoried():
    module=runpy.run_path(str(SCRIPT))
    text=module['inventory']()
    assert '/projects/add' in text and '/api/assistant/meta' in text
    for path in ('/settings/system/seo-knowledge', '/settings/system/seo-knowledge/refresh',
                 '/settings/system/seo-knowledge/{key}/review', '/settings/system/seo-knowledge/{key}/withdraw',
                 '/settings/system/seo-knowledge/{key}/pause', '/cron/seo-watch'):
        assert '`'+path+'`' in text
    assert 'settings_seo_knowledge.html' in text
    assert '`/projects/{slug}/corrections/decisions`' in text
    assert '`/api/projects/{slug}/corrections/decisions`' in text
    assert '`/projects/{slug}/corrections/decisions/refresh`' in text
    assert 'correction_decisions.html' in text
    assert 'terms_public_en.html' in text and 'privacy_public_en.html' in text
    assert '/en/docs/lancer-un-crawl' in text
    assert '/en/ressources-seo/' in text
    assert module['DESTINATION'].name=='UX_SCREEN_INVENTORY_CURRENT.md'
    assert str(len(list((WEB/'templates').glob('*.html'))))+' templates HTML' in text
    assert '196 routes HTTP' not in text


def test_cli_writes_a_separate_current_inventory_and_leaves_history_intact(tmp_path):
    history=WEB/'docs'/'UX_SCREEN_INVENTORY.md';before=history.read_bytes()
    output=tmp_path/'current.md'
    result=subprocess.run([sys.executable,str(SCRIPT),'--output',str(output)],capture_output=True,text=True)
    assert result.returncode==0 and output.is_file()
    assert 'Révision du code' in output.read_text(encoding='utf-8') and history.read_bytes()==before


def test_cli_refuses_to_replace_the_historical_inventory():
    history=WEB/'docs'/'UX_SCREEN_INVENTORY.md';before=history.read_bytes()
    result=subprocess.run([sys.executable,str(SCRIPT),'--output',str(history)],capture_output=True,text=True)
    assert result.returncode!=0 and history.read_bytes()==before
