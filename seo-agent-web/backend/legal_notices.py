"""Annoncer aux clients un changement des conditions ou du service (CGU, article 13).

Le propriétaire du site écrit ce qui change, en français et en anglais, et choisit une date
d'entrée en vigueur au moins 30 jours plus tard : c'est le préavis promis par les CGU. Chaque
compte reçoit un e-mail dans la langue qu'il a choisie, ou dans les deux langues s'il n'en a
choisi aucune. Chaque envoi est enregistré (`LegalNoticeDelivery`) : c'est la preuve que le
client a été prévenu. Ce n'est pas un message promotionnel : il part à tous les comptes.
"""
from __future__ import annotations

import html
import threading
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

from sqlalchemy import func, select

try:
    from . import i18n, legal
    from .models import LegalNotice, LegalNoticeDelivery, User
except ImportError:
    import i18n
    import legal
    from models import LegalNotice, LegalNoticeDelivery, User

NOTICE_DAYS = 30
SUMMARY_MAX = 2000

# Ce que l'annonce peut concerner, et où le client lit la version en vigueur.
DOCUMENTS: dict[str, dict[str, str]] = {
    "terms": {
        "label": "Conditions générales",
        "subject_fr": "modification de nos conditions générales",
        "subject_en": "changes to our Terms and Conditions",
        "intro_fr": "Nous modifions nos conditions générales.",
        "intro_en": "We are changing our Terms and Conditions.",
        "path": "/terms",
    },
    "privacy": {
        "label": "Politique de confidentialité, et liste des prestataires",
        "subject_fr": "modification de notre politique de confidentialité",
        "subject_en": "changes to our Privacy Policy",
        "intro_fr": "Nous modifions notre politique de confidentialité, qui comprend la liste de nos prestataires.",
        "intro_en": "We are changing our Privacy Policy, which includes the list of our providers.",
        "path": "/privacy",
    },
    "service": {
        "label": "Le service : prix ou fonctions",
        "subject_fr": "changement du service",
        "subject_en": "changes to the service",
        "intro_fr": "Nous modifions le service {app}, ses prix ou ses fonctions.",
        "intro_en": "We are changing the {app} service, its prices or its features.",
        "path": "/pricing",
    },
}

# Lance l'envoi sans faire attendre la page. Les tests le remplacent par un appel direct.
def dispatch(function: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    threading.Thread(target=function, args=args, kwargs=kwargs, daemon=True).start()


def earliest(today: date | None = None) -> date:
    """La première date d'entrée en vigueur possible : 30 jours après aujourd'hui."""
    return (today or date.today()) + timedelta(days=NOTICE_DAYS)


def validate(document: str, effective_on: str, summary_fr: str, summary_en: str, new_version_url: str = "",
             *, today: date | None = None) -> dict[str, Any]:
    """Vérifie la demande et la renvoie nettoyée ; une erreur est un message pour le propriétaire."""
    if document not in DOCUMENTS:
        raise ValueError("Choisissez ce qui change.")
    try:
        day = date.fromisoformat(str(effective_on or "").strip())
    except ValueError:
        raise ValueError("Indiquez la date d’entrée en vigueur.") from None
    first = earliest(today)
    if day < first:
        raise ValueError(
            f"La date d’entrée en vigueur doit tomber au moins {NOTICE_DAYS} jours après l’annonce, "
            f"soit le {legal.format_date(first, 'fr')} au plus tôt (CGU, article 13).")
    texts = {"fr": str(summary_fr or "").strip(), "en": str(summary_en or "").strip()}
    for lang, text in texts.items():
        name = "français" if lang == "fr" else "anglais"
        if not text:
            raise ValueError(f"Écrivez ce qui change, en {name}.")
        if len(text) > SUMMARY_MAX:
            raise ValueError(f"Le texte en {name} dépasse {SUMMARY_MAX} caractères.")
    url = str(new_version_url or "").strip()
    if url and not ((url.startswith("/") and not url.startswith("//")) or url.startswith("https://")):
        raise ValueError("L’adresse de la nouvelle version doit commencer par « / » ou par « https:// ».")
    return {"document": document, "effective_on": day, "summary_fr": texts["fr"], "summary_en": texts["en"],
            "new_version_url": url[:512] or None}


def _lang(user: User) -> str:
    chosen = str(getattr(user, "language", "") or "").strip().lower()
    return chosen if chosen in i18n.LANGUAGES else "fr+en"


def recipient_count(db: Any) -> int:
    return int(db.scalar(select(func.count()).select_from(User)) or 0)


def create(database: Any, *, author: User | None, **request: Any) -> tuple[str, int]:
    """Enregistre l'annonce et un envoi « en attente » par compte. Renvoie l'annonce et leur nombre."""
    fields = validate(**request)
    with database.session() as db:
        notice = LegalNotice(created_by_user_id=str(author.id) if author else None, **fields)
        db.add(notice)
        db.flush()
        users = list(db.scalars(select(User).order_by(User.created_at)))
        for user in users:
            db.add(LegalNoticeDelivery(notice_id=notice.id, user_id=str(user.id), email=str(user.email),
                                       lang=_lang(user), status="pending"))
        db.commit()
        return str(notice.id), len(users)


def _absolute(base_url: str, path: str) -> str:
    return path if path.startswith("https://") else base_url.rstrip("/") + path


def _part(notice: LegalNotice, lang: str, *, base_url: str, app_name: str, support_email: str) -> dict[str, Any]:
    """Les phrases de l'e-mail dans une langue."""
    spec = DOCUMENTS[notice.document]
    when = legal.format_date(notice.effective_on, lang)
    document_url = _absolute(base_url, i18n.localized_path(spec["path"], lang))
    billing_url = _absolute(base_url, "/billing")
    summary = notice.summary_fr if lang == "fr" else notice.summary_en
    if lang == "fr":
        version = (f"La nouvelle version est consultable dès maintenant : {_absolute(base_url, notice.new_version_url)}"
                   if notice.new_version_url else
                   f"La nouvelle version sera publiée le {when} à cette adresse : {document_url}. "
                   f"Vous pouvez la demander dès maintenant à {support_email}.")
        return {
            "subject": f"{app_name} : {spec['subject_fr']} au {when}",
            "paragraphs": [
                "Bonjour,",
                f"{spec['intro_fr'].format(app=app_name)} Ces changements s’appliqueront à partir du {when}.",
                "Ce qui change :",
                summary,
                version,
                "Si vous n’acceptez pas ces changements, vous pouvez résilier votre abonnement à tout moment "
                f"et sans frais, depuis la page Abonnement : {billing_url}",
                f"Ce message n’est pas publicitaire : nous l’envoyons à chaque compte {app_name}, "
                "comme le prévoient nos conditions générales (article 13).",
                f"L’équipe {app_name}",
            ],
        }
    version = (f"The new version can be read now: {_absolute(base_url, notice.new_version_url)}"
               if notice.new_version_url else
               f"The new version will be published on {when} at this address: {document_url}. "
               f"You can ask for it now at {support_email}.")
    return {
        "subject": f"{app_name}: {spec['subject_en']} on {when}",
        "paragraphs": [
            "Hello,",
            f"{spec['intro_en'].format(app=app_name)} These changes will apply from {when}.",
            "What changes:",
            summary,
            version,
            "If you do not accept these changes, you can cancel your subscription at any time and free of "
            f"charge, from the Subscription page: {billing_url}",
            f"This is not a promotional message: we send it to every {app_name} account, as our Terms and "
            "Conditions provide (section 13).",
            f"The {app_name} team",
        ],
    }


def compose(notice: LegalNotice, lang: str, *, base_url: str, app_name: str, support_email: str) -> tuple[str, str, str]:
    """Objet, texte et HTML de l'e-mail. « fr+en » : le français, puis l'anglais."""
    langs = ["fr", "en"] if lang == "fr+en" else [lang if lang in i18n.LANGUAGES else "fr"]
    parts = [_part(notice, code, base_url=base_url, app_name=app_name, support_email=support_email) for code in langs]
    subject = " / ".join(part["subject"] for part in parts) if len(parts) > 1 else parts[0]["subject"]
    text = "\n\n— English version below —\n\n".join("\n\n".join(part["paragraphs"]) for part in parts)

    def block(part: dict[str, Any]) -> str:
        return "".join(f'<p style="margin:0 0 16px;color:#333;">{html.escape(p).replace(chr(10), "<br>")}</p>'
                       for p in part["paragraphs"])

    separator = '<hr style="border:none;border-top:1px solid #ddd;margin:24px 0;">'
    body = separator.join(block(part) for part in parts)
    html_body = f"""<!DOCTYPE html>
<html lang="{langs[0]}"><head><meta charset="utf-8"><title>{html.escape(subject)}</title></head>
<body style="margin:0;padding:0;background:#f4f4f7;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f7;padding:32px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:8px;overflow:hidden;">
        <tr><td style="background:#111;padding:24px 32px;"><span style="color:#fff;font-size:18px;font-weight:700;">{html.escape(app_name)}</span></td></tr>
        <tr><td style="padding:32px;">{body}</td></tr>
      </table>
    </td></tr>
  </table>
</body></html>"""
    return subject, text, html_body


def send_pending(database: Any, notice_id: str, *, base_url: str, app_name: str, support_email: str,
                 send: Callable[..., None]) -> dict[str, int]:
    """Envoie les e-mails en attente ou en échec d'une annonce, un par un, en enregistrant chaque résultat."""
    with database.session() as db:
        notice = db.get(LegalNotice, notice_id)
        if notice is None:
            return {"sent": 0, "failed": 0}
        db.expunge(notice)
        todo = [row.id for row in db.scalars(select(LegalNoticeDelivery).where(
            LegalNoticeDelivery.notice_id == notice_id, LegalNoticeDelivery.status.in_(("pending", "failed"))))]
    counts = {"sent": 0, "failed": 0}
    for delivery_id in todo:
        with database.session() as db:
            row = db.get(LegalNoticeDelivery, delivery_id)
            if row is None or row.status == "sent":
                continue
            subject, text, html_body = compose(notice, row.lang, base_url=base_url, app_name=app_name,
                                               support_email=support_email)
            try:
                send(to_addr=row.email, subject=subject, body=text, html_body=html_body)
            except Exception as error:
                row.status, row.error = "failed", f"{type(error).__name__}: {error}"[:255]
                counts["failed"] += 1
            else:
                row.status, row.error, row.sent_at = "sent", None, datetime.now(timezone.utc)
                counts["sent"] += 1
            db.commit()
    return counts


def history(database: Any) -> list[dict[str, Any]]:
    """Les annonces, les plus récentes d'abord, avec le nombre d'envois par état."""
    with database.session() as db:
        notices = list(db.scalars(select(LegalNotice).order_by(LegalNotice.created_at.desc())))
        totals: dict[str, dict[str, int]] = {}
        for notice_id, status, count in db.execute(
                select(LegalNoticeDelivery.notice_id, LegalNoticeDelivery.status, func.count())
                .group_by(LegalNoticeDelivery.notice_id, LegalNoticeDelivery.status)):
            totals.setdefault(notice_id, {})[status] = int(count)
        return [{
            "id": notice.id,
            "document": DOCUMENTS.get(notice.document, {}).get("label", notice.document),
            "created_at": notice.created_at,
            "effective_on": legal.format_date(notice.effective_on, "fr"),
            "summary_fr": notice.summary_fr,
            "sent": totals.get(notice.id, {}).get("sent", 0),
            "failed": totals.get(notice.id, {}).get("failed", 0),
            "pending": totals.get(notice.id, {}).get("pending", 0),
        } for notice in notices]
