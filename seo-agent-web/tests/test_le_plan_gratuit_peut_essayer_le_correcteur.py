# -*- coding: utf-8 -*-
"""Un compte gratuit doit pouvoir voir UNE pull request Noyaru sur son propre depot.

Decision du 18/09/2026. PageSpeed donne deja un avant-gout (5 URLs) parce que c'est ce qui vend
l'abonnement ; le correcteur n'en donnait aucun. Or c'est lui la difference du produit face a un
Site Audit classique : un client qui n'a jamais vu de pull request ne peut pas savoir ce qu'il
acheterait.

IL FALLAIT DEUX VALEURS, PAS UNE, et c'est tout l'interet de ce fichier. Le plan gratuit n'etait
pas « a zero correction » : il etait ferme EN AMONT par `correction.max_files = 0`, que
`_correction_gate` lit avant de regarder le moindre quota. Relever `ai_corrections_month` seul
n'aurait rien change, et le refus serait tombe avec le message « non incluses dans ton forfait »
— indiscernable d'une regression pour qui n'a pas lu le code.

CE QUE LE GRATUIT OBTIENT EN PLUS, sans que cela nous coute : une reecriture DETERMINISTE
n'appelle aucun modele, donc `_correction_charge` ne la facture pas. Tant qu'il n'a pas consomme
ses deux appels au modele, un compte gratuit peut obtenir les corrections mecaniques — celles qui
se mergent sans relecture. C'est precisement ce qu'on veut lui montrer.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-free-corr-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402
from backend import auth, billing  # noqa: E402
from backend.models import User  # noqa: E402


def _compte_gratuit():
    m.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    with m.DB.session() as db:
        user = User(email="gratuit-%s@exemple.fr" % tag,
                    password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        return user


def test_le_plan_gratuit_n_est_plus_ferme_en_amont() -> None:
    """Le verrou n'etait pas le quota mais `max_files`, lu avant lui."""
    cfg = billing.correction_config_for_plan("free")
    assert cfg["max_files"] > 0, (
        "max_files a 0 fait refuser AVANT tout quota, avec « non incluses dans ton forfait »")
    assert cfg["model"], "sans modele declare, la correction ne peut pas aboutir"


def test_un_compte_gratuit_passe_la_porte_du_correcteur() -> None:
    """Le test qui compte : la porte reelle, pas la valeur de configuration."""
    user = _compte_gratuit()
    autorise, message, cap, modele = m._correction_gate(user)
    assert autorise, "un compte gratuit est encore refuse : %s" % message
    assert cap > 0 and modele


def test_le_gratuit_s_arrete_APRES_avoir_essaye_pas_avant() -> None:
    """Deux corrections, puis la porte se ferme — et le message parle d'upgrade, pas de forfait.

    C'est la difference entre « tu n'y as pas droit » et « tu as essaye, voila la suite ». La
    premiere formule ne vend rien.
    """
    user = _compte_gratuit()
    limite = billing.plan_catalog()["free"]["limits"]["ai_corrections_month"]
    with m.DB.session() as db:
        billing.usage_add(db, user_id=str(user.id), metric="ai_corrections_month", amount=limite)
    autorise, message, _cap, _mod = m._correction_gate(user)
    assert not autorise
    assert "Quota" in message and "upgrade" in message.lower(), message


def test_l_offre_gratuite_reste_TRES_loin_de_Solo() -> None:
    """Un avant-gout ne doit pas remplacer l'abonnement : Solo en offre cinquante fois plus."""
    cat = billing.plan_catalog()
    gratuit = cat["free"]["limits"]["ai_corrections_month"]
    solo = cat["solo"]["limits"]["ai_corrections_month"]
    assert 0 < gratuit <= 5, gratuit
    assert solo >= gratuit * 20, (gratuit, solo)


def test_la_carte_de_prix_ANNONCE_ce_qui_est_offert() -> None:
    """Une offre que la page de prix contredit ne sert a personne.

    Le libelle disait « Corrections en pull request : non incluses ». Le laisser aurait rendu la
    decision invisible pour le seul public qu'elle vise.
    """
    features = " ".join(billing.plan_catalog()["free"]["features"]).lower()
    assert "non incluses" not in features, features
    assert "pull request" in features, features


def test_une_correction_DETERMINISTE_ne_consomme_pas_le_quota() -> None:
    """La generosite reelle, et elle ne nous coute rien : aucun appel de modele, aucun debit.

    Si cette regle changeait, l'avant-gout se reduirait a deux fichiers et le compte gratuit ne
    verrait jamais les corrections mecaniques — celles qui se mergent sans relecture, donc celles
    qui montrent le mieux ce que le produit fait.
    """
    user = _compte_gratuit()
    avant = 0
    with m.DB.session() as db:
        avant = billing.usage_sum(db, user_id=str(user.id), metric="ai_corrections_month")
    m._correction_charge(user, 0)          # ce que le code fait pour un lot 100 % deterministe
    with m.DB.session() as db:
        apres = billing.usage_sum(db, user_id=str(user.id), metric="ai_corrections_month")
    assert apres == avant
