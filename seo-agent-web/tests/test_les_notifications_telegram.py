# -*- coding: utf-8 -*-
"""Les notifications Telegram du propriétaire : les événements qui comptent, une fois chacun.

Demande du propriétaire, 02/11/2026 : être prévenu d'un compte créé, d'un abonnement souscrit, etc.
Ce que ces tests défendent : non configuré, rien ne part ; une notification ne fait jamais échouer
l'action qui la porte ; un événement Stripe rejoué n'annonce rien deux fois ; un robot refusé
n'est pas annoncé ; le jeton du bot n'apparaît jamais dans un message d'erreur ; le texte est échappé.
"""
from __future__ import annotations

import re
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_les_consommateurs_ont_leurs_droits as _conso  # noqa: E402
from _inscription_humaine import jeton_humain  # noqa: E402

m = _conso.m
billing = _conso.billing
TestClient = _conso.TestClient
stripe_simule = _conso.stripe_simule
_client, _evenement, PRO = _conso._client, _conso._evenement, _conso.PRO
from backend import telegram  # noqa: E402

JETON = "123456:SECRET-du-bot"


@pytest.fixture()
def recus(monkeypatch):
    """Ce que la plateforme envoie à Telegram, capturé sans fil ni réseau."""
    vus: list[str] = []
    monkeypatch.setattr(m.telegram, "notifier", lambda texte: vus.append(texte))
    return vus


# ── Le module ────────────────────────────────────────────────────────────────────────────────

def test_NON_configure_rien_ne_part(monkeypatch) -> None:
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    monkeypatch.setattr(telegram.requests, "post", lambda *a, **k: pytest.fail("rien ne doit partir"))
    telegram.notifier("bonjour")
    assert telegram.envoyer_maintenant("x") == (False, "jeton ou identifiant de conversation manquant")


def test_un_envoi_vise_la_conversation_en_HTML(monkeypatch) -> None:
    vu = {}

    class _Rep:
        status_code, content = 200, b"{}"

        def json(self):
            return {"ok": True}

    def _post(url, json, timeout):
        vu.update(url=url, corps=json)
        return _Rep()

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", JETON)
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "42")
    monkeypatch.setattr(telegram.requests, "post", _post)
    assert telegram.envoyer_maintenant("<b>ok</b>") == (True, "")
    assert vu["url"].endswith("/bot%s/sendMessage" % JETON)
    assert vu["corps"]["chat_id"] == "42" and vu["corps"]["parse_mode"] == "HTML"


def test_le_JETON_n_apparait_jamais_dans_une_erreur(monkeypatch) -> None:
    def _panne(url, **kw):
        raise ConnectionError("Max retries exceeded with url: %s" % url)

    monkeypatch.setattr(telegram.requests, "post", _panne)
    monkeypatch.setattr(telegram.requests, "get", _panne)
    reussi, raison = telegram.envoyer_maintenant("x", jeton=JETON, conversation="42")
    assert not reussi and JETON not in raison and "SECRET" not in raison
    _liste, raison = telegram.conversations_recentes(JETON)
    assert JETON not in raison and "SECRET" not in raison


def test_le_texte_est_ECHAPPE() -> None:
    assert telegram.echapper("<script>&") == "&lt;script&gt;&amp;"


def test_les_conversations_recentes_se_lisent(monkeypatch) -> None:
    class _Rep:
        status_code, content = 200, b"{}"

        def json(self):
            return {"ok": True, "result": [
                {"message": {"chat": {"id": 42, "type": "private", "first_name": "Tony", "last_name": "C"}}},
                {"message": {"chat": {"id": 42, "type": "private", "first_name": "Tony", "last_name": "C"}}}]}

    monkeypatch.setattr(telegram.requests, "get", lambda url, timeout: _Rep())
    assert telegram.conversations_recentes(JETON) == ([{"id": "42", "nom": "Tony C"}], "")


# ── Les comptes ──────────────────────────────────────────────────────────────────────────────

def _inscrire(client, email, **en_plus):
    page = client.get("/auth/signup")
    csrf = re.search(r'name="_csrf"\s+value="([^"]*)"', page.text).group(1)
    donnees = {"email": email, "password": "un-mot-de-passe-assez-long", "next": "/",
               "country": "FR", "country_requested": "yes", "_csrf": csrf}
    donnees.update(en_plus)
    return client.post("/auth/signup", data=donnees, follow_redirects=False)


def test_un_compte_CREE_est_annonce_avec_sa_fiche(recus, monkeypatch) -> None:
    monkeypatch.setattr(m, "_email_verification_enabled", lambda: False)
    email = "nouveau-%s@exemple.fr" % uuid.uuid4().hex[:8]
    with TestClient(m.app) as client:
        assert _inscrire(client, email, formulaire_ouvert=jeton_humain()).status_code == 303
    (texte,) = recus
    assert "Nouveau compte" in texte and email in texte and "pays : FR" in texte
    assert "/settings/activation/accounts/" in texte


def test_un_ROBOT_refuse_n_est_pas_annonce(recus) -> None:
    with TestClient(m.app) as client:
        _inscrire(client, "robot-%s@exemple.fr" % uuid.uuid4().hex[:8], site_web="spam")
    assert recus == []


def test_une_adresse_est_echappee_dans_le_message(recus) -> None:
    m._notifier_compte_cree(email="<b>x</b>@exemple.fr", user_id="u1", pays="", via="email")
    assert "&lt;b&gt;x&lt;/b&gt;@exemple.fr" in recus[0] and "pays : non renseigné" in recus[0]


def test_une_suppression_de_compte_est_annoncee(recus) -> None:
    client, _uid, _sid = _client()
    client.get("/settings/accounts")
    jeton = client.cookies.get(m._CSRF_COOKIE_NAME)
    client.post("/settings/account/delete", data={"confirm": "DELETE", "_csrf": jeton}, follow_redirects=False)
    assert any("Compte supprimé par son titulaire" in t for t in recus), recus


# ── Les abonnements, par le webhook ──────────────────────────────────────────────────────────

def _webhook(monkeypatch, evenement) -> None:
    monkeypatch.setattr(billing, "construct_webhook_event", lambda **_kw: evenement)
    rep = TestClient(m.app).post("/stripe/webhook", content=b"{}", headers={"stripe-signature": "t=1,v1=x"})
    assert rep.status_code == 200, rep.text


def _titres(recus) -> list[str]:
    return [re.search(r"<b>([^<]+)</b>", t).group(1) for t in recus]


def test_un_NOUVEL_abonnement_est_annonce_UNE_fois(recus, monkeypatch) -> None:
    _c, uid, _sid = _client()
    sid = "sub_%s" % uuid.uuid4().hex[:8]
    evenement = _evenement(sid, uid)
    evenement["type"] = "customer.subscription.created"
    _webhook(monkeypatch, evenement)
    _webhook(monkeypatch, evenement)
    assert _titres(recus) == ["Nouvel abonnement"], "Stripe rejoue : une seule annonce"
    assert "conso-" in recus[0] and "Pro" in recus[0]


def test_un_CHANGEMENT_de_forfait_dit_d_ou_vers_ou(recus, monkeypatch) -> None:
    _c, uid, sid = _client(abonnement="active")
    _webhook(monkeypatch, _evenement(sid, uid, items={"data": [{"price": {"id": "price_business"}}]}))
    assert _titres(recus) == ["Changement de forfait"]
    assert re.search(r"Pro .*→ Business", recus[0])


def test_un_IMPAYE_puis_sa_regularisation(recus, monkeypatch) -> None:
    _c, uid, sid = _client(abonnement="active")
    _webhook(monkeypatch, _evenement(sid, uid, status="past_due"))
    _webhook(monkeypatch, _evenement(sid, uid, status="active"))
    assert _titres(recus) == ["Paiement en échec", "Paiement régularisé"]


def test_une_RESILIATION_dit_sa_date_de_fin_puis_son_ANNULATION(recus, monkeypatch) -> None:
    _c, uid, sid = _client(abonnement="active")
    _webhook(monkeypatch, _evenement(sid, uid, cancel_at_period_end=True))
    _webhook(monkeypatch, _evenement(sid, uid, cancel_at_period_end=False))
    assert _titres(recus) == ["Résiliation programmée", "Résiliation annulée"]
    assert "fin de l'abonnement : 30/10/2026" in recus[0]


def test_un_abonnement_arrete_pour_IMPAYE_est_dit_TERMINE(recus, monkeypatch) -> None:
    _c, uid, sid = _client(abonnement="past_due")
    _webhook(monkeypatch, _evenement(sid, uid, status="canceled",
                                     cancellation_details={"reason": "payment_failed"}))
    assert _titres(recus) == ["Abonnement terminé"]


def test_une_notification_en_PANNE_ne_fait_pas_echouer_le_webhook(monkeypatch) -> None:
    def _panne(_texte):
        raise RuntimeError("telegram down")

    monkeypatch.setattr(m.telegram, "notifier", _panne)
    _c, uid, sid = _client(abonnement="active")
    _webhook(monkeypatch, _evenement(sid, uid, status="past_due"))


# ── La page Système ──────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def proprietaire(monkeypatch):
    client, uid, _sid = _client()
    with m.DB.session() as db:
        email = db.get(_conso.User, uid).email
    monkeypatch.setattr(m, "_system_settings_owner_email", lambda: email)
    monkeypatch.setattr(m, "_write_env_key", lambda chemin, cle, valeur: ecrits.append((cle, valeur)))
    ecrits: list = []
    client.ecrits = ecrits
    client.get("/settings/system")
    return client


def _poster(client, chemin):
    return client.post(chemin, data={"_csrf": client.cookies.get(m._CSRF_COOKIE_NAME)}, follow_redirects=False)


def test_la_page_systeme_a_sa_section_et_ses_deux_boutons(proprietaire) -> None:
    page = proprietaire.get("/settings/system").text
    assert 'id="telegram-system"' in page and "TELEGRAM_BOT_TOKEN" in page and "TELEGRAM_CHAT_ID" in page
    assert 'action="/settings/system/telegram/conversation"' in page
    assert 'action="/settings/system/telegram/test"' in page


def test_trouver_la_conversation_l_ENREGISTRE_quand_il_n_y_en_a_qu_une(proprietaire, monkeypatch) -> None:
    monkeypatch.setattr(m.telegram, "conversations_recentes", lambda: ([{"id": "42", "nom": "Tony"}], ""))
    rep = _poster(proprietaire, "/settings/system/telegram/conversation")
    assert "msg=" in rep.headers["location"] and proprietaire.ecrits == [("TELEGRAM_CHAT_ID", "42")]


@pytest.mark.parametrize("retour, attendu", [
    (([], ""), "envoie-lui"),
    (([{"id": "1", "nom": "A"}, {"id": "2", "nom": "B"}], ""), "Plusieurs"),
], ids=["aucune", "plusieurs"])
def test_trouver_la_conversation_ne_DEVINE_pas(proprietaire, monkeypatch, retour, attendu) -> None:
    from urllib.parse import unquote_plus
    monkeypatch.setattr(m.telegram, "conversations_recentes", lambda: retour)
    rep = _poster(proprietaire, "/settings/system/telegram/conversation")
    assert attendu in unquote_plus(rep.headers["location"]) and proprietaire.ecrits == []


def test_le_message_de_TEST_dit_s_il_est_parti(proprietaire, monkeypatch) -> None:
    from urllib.parse import unquote_plus
    monkeypatch.setattr(m.telegram, "envoyer_maintenant", lambda texte: (False, "Telegram refuse : chat not found"))
    rep = _poster(proprietaire, "/settings/system/telegram/test")
    assert "chat not found" in unquote_plus(rep.headers["location"])


@pytest.mark.parametrize("valeur, accepte", [
    ("noyaru_bot", False), ("@NoyaruNotifBot", False), ("abc", False), ("12", False),
    ("123456789", True), ("-1001234567890", True), ("@ma_chaine_publique", True),
], ids=["nom-du-bot", "arobase-bot", "texte", "trop-court", "personne", "groupe", "chaine"])
def test_la_CONVERSATION_est_un_identifiant_jamais_le_nom_du_bot(valeur, accepte) -> None:
    """Premier réglage en production (02/11/2026) : « …_bot » collé ici, et « chat not found »."""
    erreur = m._validate_settings_env_value("TELEGRAM_CHAT_ID", valeur)
    assert (erreur is None) is accepte, erreur
    if valeur.endswith(("bot", "Bot")):
        assert "nom de ton bot" in erreur and "Trouver ma conversation" in erreur


def test_le_JETON_a_la_forme_de_BotFather() -> None:
    assert m._validate_settings_env_value("TELEGRAM_BOT_TOKEN", "pas-un-jeton")
    assert m._validate_settings_env_value("TELEGRAM_BOT_TOKEN", "123456789:" + "A" * 35) is None


def test_chat_not_found_dit_QUOI_faire(proprietaire, monkeypatch) -> None:
    from urllib.parse import unquote_plus
    monkeypatch.setattr(m.telegram, "envoyer_maintenant",
                        lambda texte: (False, "Telegram refuse : Bad Request: chat not found"))
    rep = _poster(proprietaire, "/settings/system/telegram/test")
    assert "Trouver ma conversation" in unquote_plus(rep.headers["location"])


def test_seul_le_PROPRIETAIRE_du_systeme_peut_les_utiliser(monkeypatch) -> None:
    client, _uid, _sid = _client()
    monkeypatch.setattr(m, "_system_settings_owner_email", lambda: "autre@exemple.fr")
    client.get("/billing")
    for chemin in ("/settings/system/telegram/conversation", "/settings/system/telegram/test"):
        assert _poster(client, chemin).status_code == 403
