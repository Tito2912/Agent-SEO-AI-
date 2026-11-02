"""Les comptes jamais utilises — ce que laissent les inscriptions de robots — et leur suppression.

Releve du 02/11/2026 : 39 des 44 comptes etaient des inscriptions de robots, faites avec les
adresses de personnes qui n'avaient rien demande. Decision du proprietaire : « si tu es sur que
ce ne sont pas de vrais clients, on supprime ».

« SUR » SE PROUVE COMPTE PAR COMPTE, et la preuve est refaite au moment de supprimer, jamais
reprise d'un ecran affiche plus tot. Un compte n'est supprimable que si AUCUNE trace d'usage
n'existe : pas de site, pas d'analyse, pas de consommation, pas de client Stripe, pas d'equipe,
pas d'invitation, pas de connexion reussie, pas de compte Google ni de connexion a un service,
pas d'acces de demonstration, pas administrateur, et inscrit depuis plus de 7 jours — le temps de
confirmer son email et revenir. Il suffit d'UNE trace pour garder le compte, et la raison est dite.

La liste des candidats n'est pas la decision : l'ecran ne coche rien d'office, la personne choisit.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import or_, select, update

try:
    from .models import (AccountInvite, AccountMember, AuditLog, BillingCustomer, BillingSubscription,
                         EmailVerificationToken, JobRecord, OAuthIdentity, Project, ProjectRecipeAccess,
                         UsageEvent, User, UserConnection)
except ImportError:  # pragma: no cover - execution depuis backend/
    from models import (AccountInvite, AccountMember, AuditLog, BillingCustomer,  # type: ignore
                        BillingSubscription, EmailVerificationToken, JobRecord, OAuthIdentity, Project,
                        ProjectRecipeAccess, UsageEvent, User, UserConnection)

ANCIENNETE_MIN = timedelta(days=7)
# Les connexions reussies, telles que le journal d'audit les nomme.
_CONNEXIONS = (("auth.login", "ok"), ("auth.google.callback", "ok"), ("auth.verify_email", "ok"))


def _existe(db: Any, requete: Any) -> bool:
    return db.scalar(requete.limit(1)) is not None


def raisons_de_garder(db: Any, user: Any, *, maintenant: datetime | None = None) -> list[str]:
    """Toutes les traces d'usage d'un compte. Vide : rien ne montre qu'il ait jamais servi."""
    maintenant = maintenant or datetime.now(timezone.utc)
    uid, email = str(user.id), str(user.email or "").strip().lower()
    raisons: list[str] = []
    if bool(getattr(user, "is_admin", False)):
        raisons.append("administrateur")
    cree = getattr(user, "created_at", None)
    if cree is None:
        raisons.append("date d'inscription inconnue")
    else:
        if cree.tzinfo is None:
            cree = cree.replace(tzinfo=timezone.utc)
        if maintenant - cree < ANCIENNETE_MIN:
            raisons.append("inscrit il y a moins de 7 jours")
    controles = (
        ("possède un site", select(Project.id).where(Project.owner_user_id == uid)),
        ("a lancé une analyse", select(JobRecord.id).where(JobRecord.owner_user_id == uid)),
        ("consommation enregistrée", select(UsageEvent.id).where(UsageEvent.user_id == uid)),
        ("client Stripe", select(BillingCustomer.id).where(BillingCustomer.user_id == uid)),
        ("abonnement Stripe", select(BillingSubscription.id).where(BillingSubscription.user_id == uid)),
        ("équipe", select(AccountMember.id).where(or_(AccountMember.owner_user_id == uid,
                                                       AccountMember.member_user_id == uid))),
        ("invitation", select(AccountInvite.id).where(or_(AccountInvite.owner_user_id == uid,
                                                          AccountInvite.email == email))),
        ("compte Google associé", select(OAuthIdentity.id).where(OAuthIdentity.user_id == uid)),
        ("service connecté", select(UserConnection.id).where(UserConnection.user_id == uid)),
        ("accès de démonstration", select(ProjectRecipeAccess.user_id).where(ProjectRecipeAccess.user_id == uid)),
        # Un lien de confirmation clique : peut-etre un filtre de messagerie qui suit les liens,
        # peut-etre une personne. On ne peut pas prouver que personne n'est derriere : on garde.
        ("email confirmé", select(EmailVerificationToken.id).where(
            EmailVerificationToken.user_id == uid, EmailVerificationToken.used_at.is_not(None))),
        ("s'est déjà connecté", select(AuditLog.id).where(
            or_(*[(AuditLog.action == a) & (AuditLog.status == s) for a, s in _CONNEXIONS]),
            or_(AuditLog.actor_user_id == uid, AuditLog.actor_email == email))),
    )
    for raison, requete in controles:
        if _existe(db, requete):
            raisons.append(raison)
    return raisons


def candidats(db: Any, *, maintenant: datetime | None = None) -> list[Any]:
    """Les comptes sans aucune trace d'usage, les plus anciens d'abord."""
    return [u for u in db.scalars(select(User).order_by(User.created_at.asc()))
            if not raisons_de_garder(db, u, maintenant=maintenant)]


def supprimer(db: Any, ids: "list[str]", *, maintenant: datetime | None = None
              ) -> "tuple[list[str], list[tuple[str, list[str]]]]":
    """Supprime ceux des comptes `ids` qui n'ont TOUJOURS aucune trace. Rend (supprimes, gardes).

    La preuve est refaite ici : un compte coche sur un ecran ancien, qui s'est connecte depuis,
    est garde. Les adresses supprimees disparaissent aussi du journal d'audit — ce sont celles de
    personnes qui n'ont jamais demande de compte ; les evenements, eux, restent.
    """
    supprimes: list[str] = []
    gardes: list[tuple[str, list[str]]] = []
    for uid in dict.fromkeys(str(i) for i in ids if i):
        user = db.get(User, uid)
        if user is None:
            continue
        raisons = raisons_de_garder(db, user, maintenant=maintenant)
        if raisons:
            gardes.append((str(user.email), raisons))
            continue
        email = str(user.email or "").strip().lower()
        db.execute(update(AuditLog).where(AuditLog.actor_user_id == uid).values(actor_user_id=None, actor_email=None))
        if email:
            db.execute(update(AuditLog).where(AuditLog.actor_email == email).values(actor_email=None))
        db.delete(user)
        supprimes.append(email)
    db.commit()
    return supprimes, gardes
