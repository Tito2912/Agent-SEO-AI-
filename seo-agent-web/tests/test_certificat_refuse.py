# -*- coding: utf-8 -*-
"""Un certificat refuse se nomme, et ne fait pas passer les ressources du site pour cassees.

14/10/2026, elevenlabs-avis.com servait le certificat par defaut de Netlify (`*.netlify.app`).
Ahrefs n'y voyait que les 3 redirections habituelles ; Noyaru y voyait huit familles d'ERREURS —
JavaScript, CSS et images « casses », jusqu'a 31 occurrences — et pas un mot du certificat :
- les pages sont chargees par un navigateur qui ignore les erreurs de certificat, les ressources
  par `requests`, qui les refuse : chaque ressource du site echouait ;
- l'audit TLS ne lisait le nom du certificat qu'APRES une poignee de main reussie, impossible
  justement quand le nom ne correspond pas : `certificate_name_mismatch` etait inatteignable.

La tolerance ne couvre que le certificat NOMME dans le rapport, sur son hote : un sous-domaine,
une redirection ailleurs ou un refus que l'audit ne sait pas nommer restent casses — sinon le
defaut ne serait plus signale nulle part.
"""
from __future__ import annotations

import importlib.util
import inspect
import ssl
import sys
import types
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_certificat", REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py")
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_certificat"] = seo_audit
_SPEC.loader.exec_module(seo_audit)

NOMME = frozenset({"site.test"})


class _Prise:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _poignee_refusee(monkeypatch, code: int, message: str) -> dict:
    erreur = ssl.SSLCertVerificationError(1, "[SSL: CERTIFICATE_VERIFY_FAILED] " + message)
    erreur.verify_code = code
    erreur.verify_message = message

    class _Contexte:
        def wrap_socket(self, sock, server_hostname=None):
            raise erreur

    monkeypatch.setattr(seo_audit.socket, "create_connection", lambda *a, **k: _Prise())
    monkeypatch.setattr(seo_audit.ssl, "create_default_context", lambda: _Contexte())
    return seo_audit._audit_tls("site.test", timeout_s=5)


def test_un_nom_qui_ne_correspond_pas_est_nomme(monkeypatch) -> None:
    tls = _poignee_refusee(monkeypatch, 62, "Hostname mismatch, certificate is not valid for 'site.test'.")

    assert tls["ok"] is False and tls["hostname_ok"] is False
    assert list(seo_audit._familles_certificat(tls, "site.test")) == ["certificate_name_mismatch"]


def test_un_certificat_expire_est_nomme(monkeypatch) -> None:
    tls = _poignee_refusee(monkeypatch, 10, "certificate has expired")

    familles = seo_audit._familles_certificat(tls, "site.test")
    assert list(familles) == ["certificate_expiration"]
    assert familles["certificate_expiration"]["days_left"] is None


def test_un_autre_refus_ne_se_deguise_pas_en_nom_ni_en_expiration(monkeypatch) -> None:
    tls = _poignee_refusee(monkeypatch, 20, "unable to get local issuer certificate")

    assert seo_audit._familles_certificat(tls, "site.test") == {}


@pytest.mark.parametrize("tls, attendu", [
    ({"ok": True, "hostname_ok": True, "days_left": 80, "validity_days": 90}, []),
    ({"ok": True, "hostname_ok": True, "days_left": 5, "validity_days": 90}, ["certificate_expiration"]),
])
def test_une_poignee_reussie_garde_son_comportement(tls, attendu) -> None:
    assert list(seo_audit._familles_certificat(tls, "site.test")) == attendu


# --- L'audit du site : quelles familles, et quels hotes deviennent tolerables ---

_REFUS_NOM = {"ok": False, "hostname_ok": False, "verify_code": 62, "hostname_error": "Hostname mismatch"}
_REFUS_INCONNU = {"ok": False, "verify_code": 18, "error": "self-signed certificate"}
_PROCHE_EXPIRATION = {"ok": True, "hostname_ok": True, "days_left": 5, "validity_days": 90}


def _site(monkeypatch, base_url: str, tls: dict) -> tuple[dict, frozenset, list]:
    ports: list = []

    def _faux_audit(host, *, timeout_s, port=443):
        ports.append(port)
        return dict(tls)

    monkeypatch.setattr(seo_audit, "_audit_tls", _faux_audit)
    _, familles, nommes = seo_audit._audit_certificat_du_site(urlsplit(base_url), timeout_s=5)
    return familles, nommes, ports


def test_le_refus_nomme_rend_l_hote_tolerable(monkeypatch) -> None:
    familles, nommes, ports = _site(monkeypatch, "https://site.test/", _REFUS_NOM)

    assert list(familles) == ["certificate_name_mismatch"] and nommes == NOMME and ports == [443]


def test_un_refus_que_l_audit_ne_sait_pas_nommer_ne_tolere_rien(monkeypatch) -> None:
    familles, nommes, _ = _site(monkeypatch, "https://site.test/", _REFUS_INCONNU)

    assert familles == {} and nommes == frozenset()


def test_un_site_en_http_n_a_pas_de_certificat_a_se_reprocher(monkeypatch) -> None:
    # Son port 8080 ne parle pas TLS : c'est le 443 qu'on sonde, comme avant.
    familles, nommes, ports = _site(monkeypatch, "http://site.test:8080/", _REFUS_NOM)

    assert familles == {} and nommes == frozenset() and ports == [443]


def test_un_certificat_lisible_n_ouvre_aucune_tolerance(monkeypatch) -> None:
    familles, nommes, _ = _site(monkeypatch, "https://site.test/", _PROCHE_EXPIRATION)

    assert list(familles) == ["certificate_expiration"] and nommes == frozenset()


def test_le_port_explicite_est_celui_qu_on_audite(monkeypatch) -> None:
    _, _, ports = _site(monkeypatch, "https://site.test:8443/", _REFUS_NOM)

    assert ports == [8443]


# --- La ressource : un second essai, seulement sur l'hote nomme ---

class _Reponse:
    status_code = 200
    headers = {"Content-Type": "text/css", "Content-Length": "12"}

    def __init__(self, url: str, history: list | None = None):
        self.url = url
        self.history = history or []
        self.fermee = False

    def close(self):
        self.fermee = True


class _Session:
    def __init__(self, final_url: str | None = None):
        self.appels: list = []
        self.final_url = final_url
        self.reponses: list = []

    def get(self, url, **kw):
        self.appels.append(kw.get("verify", True))
        if kw.get("verify", True):
            raise requests.exceptions.SSLError("certificate verify failed: Hostname mismatch")
        if self.final_url:
            reponse = _Reponse(self.final_url, history=[_Reponse(url)])
        else:
            reponse = _Reponse(url)
        self.reponses.append(reponse)
        return reponse


def _ressource(monkeypatch, url: str, nommes: frozenset, final_url: str | None = None):
    session = _Session(final_url)
    monkeypatch.setattr(seo_audit, "_get_session", lambda *a, **k: session)
    return seo_audit._fetch_resource(url, timeout_s=5, user_agent="t", certificats_nommes=nommes), session


def test_une_ressource_de_l_hote_nomme_n_est_pas_cassee(monkeypatch) -> None:
    ligne, session = _ressource(monkeypatch, "https://site.test/style.css", NOMME)

    assert ligne["status_code"] == 200 and ligne["error"] is None and ligne["tls_ignore"] is True
    assert session.appels == [True, False]


def test_un_autre_hote_du_site_reste_casse(monkeypatch) -> None:
    ligne, session = _ressource(monkeypatch, "https://cdn.site.test/app.js", NOMME)

    assert ligne["status_code"] is None and "SSLError" in ligne["error"] and session.appels == [True]


def test_sans_certificat_nomme_le_refus_reste_une_erreur(monkeypatch) -> None:
    ligne, session = _ressource(monkeypatch, "https://site.test/style.css", frozenset())

    assert ligne["status_code"] is None and "SSLError" in ligne["error"] and session.appels == [True]


def test_une_redirection_hors_de_l_hote_nomme_garde_l_erreur(monkeypatch) -> None:
    ligne, session = _ressource(monkeypatch, "https://site.test/a.js", NOMME,
                                final_url="https://cdn.ailleurs.example/a.js")

    assert ligne["status_code"] is None and "SSLError" in ligne["error"]
    assert session.appels == [True, False] and session.reponses[0].fermee


def test_une_redirection_sur_l_hote_nomme_reste_toleree(monkeypatch) -> None:
    ligne, _ = _ressource(monkeypatch, "https://site.test/a.js", NOMME, final_url="https://site.test/b.js")

    assert ligne["status_code"] == 200 and ligne["redirect_hops"] == 1 and ligne["error"] is None


# --- Le cablage : les hotes nommes vont de l'audit aux ressources du site, pas ailleurs ---

def test_les_ressources_du_site_recoivent_les_hotes_nommes(monkeypatch) -> None:
    recus: list = []

    def _faux_fetch(url, **kw):
        recus.append(kw.get("certificats_nommes"))
        return {"url": url, "status_code": 200, "error": None}

    monkeypatch.setattr(seo_audit, "_fetch_resource", _faux_fetch)
    page = seo_audit.PageData(url="https://site.test/", content_type="text/html", status_code=200,
                              css_urls=["https://site.test/style.css"])
    config = types.SimpleNamespace(allow_subdomains=False, max_resources=10, workers=1, timeout_s=5,
                                   user_agent="t", connection_close=True)
    seo_audit._fetch_internal_resources([page], config, urlsplit("https://site.test/"),
                                        certificats_nommes=NOMME)

    assert recus == [NOMME]


def test_main_transmet_les_hotes_nommes_et_les_externes_n_en_ont_pas() -> None:
    assert "certificats_nommes=certificats_nommes" in inspect.getsource(seo_audit.main)
    assert "certificats_nommes" not in inspect.getsource(seo_audit._fetch_external_resources)
