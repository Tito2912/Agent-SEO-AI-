# -*- coding: utf-8 -*-
"""Inviter quelqu'un dans son compte : le lien, les places, et ce qu'un lien transfere ne fait pas.

Cinquieme et derniere pierre des comptes d'equipe. Les quatre precedentes ouvraient l'acces aux
projets, a leurs rapports, reglaient qui paie et pretaient les connexions du compte — mais
AUCUNE adhesion ne pouvait naitre : il fallait ecrire une ligne en base a la main. C'est ce qui
rendait tout le chantier inoffensif jusqu'ici, et c'est ce qui change avec ce fichier.

DECISIONS DU PROPRIETAIRE, appliquees ici :
  - places forfaitaires par plan — Gratuit 0, Solo 0, Pro 2, Business 5 — donc aucun appel
    Stripe et aucun changement de tarification ;
  - invitation par LIEN ENVOYE PAR EMAIL, a usage unique et daté.

LA GARDE QUI COMPTE, et le test qui la tient : le lien est LIE A L'ADRESSE INVITEE. Un lien
d'invitation circule — transfere, colle dans une conversation, retrouve dans une boite
partagee. S'il suffisait de le detenir, n'importe qui entrerait dans le compte d'une agence
avec, depuis l'etape precedente, l'usage de son jeton GitHub. Detenir le lien ne suffit donc
pas : il faut porter l'adresse.

LES PLACES SE COMPTENT AVEC LES INVITATIONS EN ATTENTE, pas seulement avec les membres en
place. Sinon dix invitations partent sur deux places, les premieres personnes a cliquer
entrent, et le refus tombe sur celles d'apres — un message d'erreur pour quelqu'un qui n'a rien
fait de mal.

ET TOUT EST REVERIFIE A L'ACCEPTATION. Le lien vit une semaine : entre-temps le forfait a pu
baisser, d'autres invitations ont pu etre acceptees, la personne a pu rejoindre un autre compte.
Ne verifier qu'a l'emission laisserait passer tout ce qui s'est produit depuis.
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
import uuid
from datetime import timedelta
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-invites-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth, billing  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import AccountInvite, AccountMember, BillingSubscription, User  # noqa: E402


def _utilisateur(prefixe: str, plan: str = "") -> tuple[str, str]:
    """Rend (identifiant, adresse). L'adresse compte ici : c'est elle qui lie l'invitation."""
    m.DB.create_tables()
    adresse = "%s-%s@exemple.fr" % (prefixe, uuid.uuid4().hex[:8])
    with m.DB.session() as db:
        u = User(email=adresse, password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        uid = str(u.id)
    if plan:
        with m.DB.session() as db:
            db.add(BillingSubscription(
                user_id=uid, plan_key=plan, status="active",
                stripe_customer_id="cus_%s" % uuid.uuid4().hex[:10],
                stripe_subscription_id="sub_%s" % uuid.uuid4().hex[:10],
                stripe_price_id="price_%s" % uuid.uuid4().hex[:10]))
            db.commit()
        with m.DB.session() as db:
            obtenu = billing.effective_plan_key(db, user_id=uid)
        assert obtenu == plan, "le forfait pose n'est pas celui que lit l'application (%r)" % obtenu
    return uid, adresse


def _client(user_id: str) -> TestClient:
    c = TestClient(app)
    c.cookies.set(auth.SESSION_COOKIE_NAME,
                  auth.make_session_token(user_id=user_id, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return c


def _jeton_csrf(client: TestClient, page: str = "/settings/team") -> str:
    rep = client.get(page)
    assert rep.status_code == 200, "%s -> %d" % (page, rep.status_code)
    trouve = re.search(r'name="_csrf"\s+value="([^"]*)"', rep.text)
    assert trouve, "pas de jeton CSRF sur %s" % page
    return trouve.group(1)


@pytest.fixture()
def emails(monkeypatch) -> list[dict[str, str]]:
    """Capture les emails REELLEMENT composes : le lien teste est celui que la personne recoit.

    On intercepte l'envoi et non la fabrication du message, pour que le corps, le sujet et
    l'adresse soient ceux que le code produit vraiment.
    """
    recus: list[dict[str, str]] = []

    def _faux(*, to_addr: str, subject: str, body: str, html_body: str = "") -> None:
        recus.append({"to": to_addr, "subject": subject, "body": body, "html": html_body})

    monkeypatch.setattr(m, "_send_email", _faux)
    return recus


def _lien_recu(emails: list[dict[str, str]]) -> str:
    assert emails, "aucun email envoye"
    trouve = re.search(r"http\S*/team/accept\?token=\S+", emails[-1]["body"])
    assert trouve, "pas de lien d'acceptation dans l'email :\n%s" % emails[-1]["body"]
    return trouve.group(0)


# --- le parcours complet -----------------------------------------------------------------------

def test_le_parcours_entier_par_le_LIEN_recu(emails) -> None:
    """De l'invitation a l'acces, en passant par l'email — c'est le seul chemin d'un client."""
    patron, mail_patron = _utilisateur("agence", plan="pro")
    consultant, mail_consultant = _utilisateur("consultant")

    client = _client(patron)
    rep = client.post("/settings/team/invite",
                      data={"email": mail_consultant, "_csrf": _jeton_csrf(client)},
                      follow_redirects=False)
    assert rep.status_code == 303, rep.status_code
    assert emails[-1]["to"] == mail_consultant
    assert mail_patron in emails[-1]["subject"] or mail_patron in emails[-1]["body"]

    assert m._db_project(consultant, "peu-importe") is None    # rien avant
    with m.DB.session() as db:
        assert db.scalar(select(AccountMember).where(
            AccountMember.member_user_id == consultant)) is None

    lien = _lien_recu(emails)
    rep = _client(consultant).get(lien.replace("http://testserver", ""), follow_redirects=False)
    assert rep.status_code == 303, rep.status_code

    with m.DB.session() as db:
        adhesion = db.scalar(select(AccountMember).where(
            AccountMember.member_user_id == consultant))
    assert adhesion is not None and str(adhesion.owner_user_id) == patron


# --- ce qu'un lien transfere ne fait PAS ---------------------------------------------------------

def test_un_lien_TRANSFERE_n_ouvre_rien(emails) -> None:
    """La garde qui empeche un lien qui circule d'ouvrir le compte d'une agence.

    Depuis l'etape precedente, entrer dans un compte donne l'usage de son jeton GitHub. Detenir
    le lien ne peut donc pas suffire : il faut porter l'adresse invitee.
    """
    patron, _ = _utilisateur("agence", plan="pro")
    _consultant, mail_consultant = _utilisateur("consultant")
    inconnu, _mail_inconnu = _utilisateur("inconnu")

    client = _client(patron)
    client.post("/settings/team/invite",
                data={"email": mail_consultant, "_csrf": _jeton_csrf(client)},
                follow_redirects=False)
    lien = _lien_recu(emails).replace("http://testserver", "")

    rep = _client(inconnu).get(lien, follow_redirects=False)
    assert rep.status_code == 303
    with m.DB.session() as db:
        assert db.scalar(select(AccountMember).where(
            AccountMember.member_user_id == inconnu)) is None, "un lien transfere a ouvert le compte"


def test_un_lien_ne_sert_QU_UNE_FOIS() -> None:
    patron, _ = _utilisateur("agence", plan="pro")
    consultant, mail_consultant = _utilisateur("consultant")
    invitation, jeton, erreur = m._creer_invitation(
        owner_user_id=patron, email=mail_consultant, invited_by=patron)
    assert invitation is not None, erreur

    class _P:
        id = consultant
    ok, _msg = m._accepter_invitation(token=jeton, user=_P())
    assert ok
    m._retirer_membre(owner_user_id=patron, member_user_id=consultant)
    encore, message = m._accepter_invitation(token=jeton, user=_P())
    assert not encore, "le meme lien a resservi apres un retrait"
    assert "utilisée" in message


def test_un_lien_PERIME_est_refuse() -> None:
    patron, _ = _utilisateur("agence", plan="pro")
    consultant, mail_consultant = _utilisateur("consultant")
    invitation, jeton, erreur = m._creer_invitation(
        owner_user_id=patron, email=mail_consultant, invited_by=patron)
    assert invitation is not None, erreur
    with m.DB.session() as db:
        ligne = db.get(AccountInvite, str(invitation.id))
        ligne.expires_at = m._utc_now_naive() - timedelta(minutes=1)
        db.commit()

    class _P:
        id = consultant
    ok, message = m._accepter_invitation(token=jeton, user=_P())
    assert not ok and "expiré" in message


# --- les places --------------------------------------------------------------------------------

def test_un_forfait_SANS_place_ne_peut_pas_inviter() -> None:
    """Gratuit et Solo n'ont pas d'equipe, et le message doit le dire plutot que d'echouer sec."""
    for plan in ("", "solo"):
        patron, _ = _utilisateur("seul", plan=plan)
        _cible, mail = _utilisateur("cible")
        invitation, _jeton, erreur = m._creer_invitation(
            owner_user_id=patron, email=mail, invited_by=patron)
        assert invitation is None, "un forfait %r a pu inviter" % (plan or "gratuit")
        assert "forfait" in erreur


def test_une_invitation_EN_ATTENTE_occupe_une_place() -> None:
    """Sans ce comptage, dix invitations partent sur deux places et le refus tombe sur la
    troisieme personne a cliquer — quelqu'un qui n'y est pour rien."""
    patron, _ = _utilisateur("agence", plan="pro")          # deux places
    assert m._sieges_du_plan(patron) == 2
    for i in range(2):
        _c, mail = _utilisateur("consultant%d" % i)
        invitation, _j, erreur = m._creer_invitation(
            owner_user_id=patron, email=mail, invited_by=patron)
        assert invitation is not None, erreur
    _c, mail3 = _utilisateur("consultant3")
    trop, _j, erreur = m._creer_invitation(owner_user_id=patron, email=mail3, invited_by=patron)
    assert trop is None, "une troisieme invitation est passee sur deux places"
    assert "places" in erreur


def test_annuler_une_invitation_LIBERE_la_place() -> None:
    patron, _ = _utilisateur("agence", plan="pro")
    _c1, mail1 = _utilisateur("consultant1")
    _c2, mail2 = _utilisateur("consultant2")
    premiere, _j, _e = m._creer_invitation(owner_user_id=patron, email=mail1, invited_by=patron)
    m._creer_invitation(owner_user_id=patron, email=mail2, invited_by=patron)
    assert m._sieges_occupes(patron) == 2

    ok, _msg = m._revoquer_invitation(owner_user_id=patron, invite_id=str(premiere.id))
    assert ok
    assert m._sieges_occupes(patron) == 1
    _c3, mail3 = _utilisateur("consultant3")
    encore, _j, erreur = m._creer_invitation(owner_user_id=patron, email=mail3, invited_by=patron)
    assert encore is not None, erreur


def test_les_places_sont_REVERIFIEES_a_l_acceptation() -> None:
    """Le lien vit une semaine : ce qui etait vrai a l'emission ne l'est plus forcement.

    Deux invitations partent sur deux places. Le forfait retombe ensuite a Solo — zero place.
    Le premier lien clique doit etre refuse, sinon un compte se retrouve avec des membres que
    son forfait ne couvre plus.
    """
    patron, _ = _utilisateur("agence", plan="pro")
    consultant, mail = _utilisateur("consultant")
    invitation, jeton, erreur = m._creer_invitation(
        owner_user_id=patron, email=mail, invited_by=patron)
    assert invitation is not None, erreur

    with m.DB.session() as db:
        abonnement = db.scalar(select(BillingSubscription).where(
            BillingSubscription.user_id == patron))
        abonnement.plan_key = "solo"
        db.commit()
    assert m._sieges_du_plan(patron) == 0

    class _P:
        id = consultant
    ok, message = m._accepter_invitation(token=jeton, user=_P())
    assert not ok, "l'invitation a ete acceptee alors que le forfait n'a plus de place"
    assert "place" in message


# --- une seule adhesion par personne ---------------------------------------------------------

def test_inviter_quelqu_un_DEJA_dans_une_autre_equipe_est_refuse_TOT() -> None:
    """Le refus est prononce a l'emission et pas seulement au clic.

    Le schema interdit la seconde adhesion de toute facon. Mais laisser partir un lien qui ne
    pourra jamais aboutir occuperait une place pour rien et ferait tomber l'erreur sur la
    personne invitee, qui n'a aucun moyen de comprendre.
    """
    premiere_agence, _ = _utilisateur("agence-a", plan="pro")
    seconde_agence, _ = _utilisateur("agence-b", plan="pro")
    consultant, mail = _utilisateur("consultant")
    with m.DB.session() as db:
        db.add(AccountMember(owner_user_id=premiere_agence, member_user_id=consultant))
        db.commit()

    invitation, _j, erreur = m._creer_invitation(
        owner_user_id=seconde_agence, email=mail, invited_by=seconde_agence)
    assert invitation is None
    assert "autre compte" in erreur
    assert m._sieges_occupes(seconde_agence) == 0, "la place a ete consommee pour rien"


def test_quitter_un_compte_permet_d_en_rejoindre_un_autre() -> None:
    """Sans le depart volontaire, une seule adhesion par personne devient une impasse."""
    agence_a, _ = _utilisateur("agence-a", plan="pro")
    agence_b, _ = _utilisateur("agence-b", plan="pro")
    consultant, mail = _utilisateur("consultant")

    _inv, jeton_a, _e = m._creer_invitation(owner_user_id=agence_a, email=mail, invited_by=agence_a)

    class _P:
        id = consultant
    assert m._accepter_invitation(token=jeton_a, user=_P())[0]

    client = _client(consultant)
    rep = client.post("/settings/team/leave", data={"_csrf": _jeton_csrf(client)},
                      follow_redirects=False)
    assert rep.status_code == 303

    _inv, jeton_b, _e = m._creer_invitation(owner_user_id=agence_b, email=mail, invited_by=agence_b)
    ok, message = m._accepter_invitation(token=jeton_b, user=_P())
    assert ok, message
    with m.DB.session() as db:
        adhesion = db.scalar(select(AccountMember).where(
            AccountMember.member_user_id == consultant))
    assert str(adhesion.owner_user_id) == agence_b


# --- ce qu'un proprietaire ne peut pas faire chez un autre ---------------------------------------

def test_on_n_annule_pas_l_invitation_d_un_AUTRE_compte() -> None:
    """L'identifiant d'une invitation ne doit pas suffire a la detruire."""
    agence_a, _ = _utilisateur("agence-a", plan="pro")
    agence_b, _ = _utilisateur("agence-b", plan="pro")
    _c, mail = _utilisateur("consultant")
    invitation, _j, erreur = m._creer_invitation(
        owner_user_id=agence_a, email=mail, invited_by=agence_a)
    assert invitation is not None, erreur

    client = _client(agence_b)
    rep = client.post("/settings/team/invite/%s/revoke" % invitation.id,
                      data={"_csrf": _jeton_csrf(client)}, follow_redirects=False)
    assert rep.status_code == 303
    assert m._sieges_occupes(agence_a) == 1, "l'invitation d'un autre compte a ete annulee"


def test_un_proprietaire_ne_s_invite_pas_lui_meme() -> None:
    patron, mail_patron = _utilisateur("agence", plan="pro")
    invitation, _j, erreur = m._creer_invitation(
        owner_user_id=patron, email=mail_patron, invited_by=patron)
    assert invitation is None
    assert "propre adresse" in erreur


# --- l'ecran ------------------------------------------------------------------------------------

def test_l_ecran_montre_les_places_et_le_rattachement() -> None:
    patron, mail_patron = _utilisateur("agence", plan="business")   # cinq places
    consultant, mail = _utilisateur("consultant")
    _inv, jeton, _e = m._creer_invitation(owner_user_id=patron, email=mail, invited_by=patron)

    class _P:
        id = consultant
    assert m._accepter_invitation(token=jeton, user=_P())[0]

    vue_patron = _client(patron).get("/settings/team")
    assert vue_patron.status_code == 200
    assert mail in vue_patron.text, "le membre n'apparait pas chez le proprietaire"
    assert "1 / 5" in vue_patron.text, "le compteur de places n'est pas juste"

    vue_membre = _client(consultant).get("/settings/team")
    assert vue_membre.status_code == 200
    assert mail_patron in vue_membre.text, "le membre ne voit pas a quel compte il est rattache"


def test_un_forfait_sans_place_voit_l_invitation_DESACTIVEE() -> None:
    """Un formulaire qui accepte puis refuse vaut moins qu'un formulaire qui dit non tout de suite."""
    seul, _ = _utilisateur("seul", plan="solo")
    vue = _client(seul).get("/settings/team")
    assert vue.status_code == 200
    assert "0 / 0" in vue.text
    assert "Pro" in vue.text and "Business" in vue.text


# --- le jeton ------------------------------------------------------------------------------------

def test_le_jeton_n_est_pas_stocke_en_clair() -> None:
    """Une fuite de la base ne doit rendre aucune invitation utilisable."""
    patron, _ = _utilisateur("agence", plan="pro")
    _c, mail = _utilisateur("consultant")
    invitation, jeton, erreur = m._creer_invitation(
        owner_user_id=patron, email=mail, invited_by=patron)
    assert invitation is not None, erreur
    assert jeton and len(jeton) >= 32
    with m.DB.session() as db:
        ligne = db.get(AccountInvite, str(invitation.id))
        stocke = str(ligne.token_hash)
    assert jeton not in stocke
    assert stocke == m._invite_token_hash(jeton)
    assert stocke != m._password_reset_token_hash(jeton), (
        "invitation et reinitialisation produisent la meme empreinte : "
        "une empreinte qui ne dit pas a quoi elle sert finit comparee au mauvais endroit")
