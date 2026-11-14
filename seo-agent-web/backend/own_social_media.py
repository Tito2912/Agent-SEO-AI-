"""One bounded narration and local montage, then separate owner review. No publishing."""

import base64
from datetime import timedelta
import json
import os
import re
import threading
import unicodedata
import uuid

import requests
from sqlalchemy import select, func, or_, and_

try:
    from . import own_social as social, own_social_media_render as render
    from .models import OwnSocialMedia as Media, AuditLog, TelegramApprovalRequest
except ImportError:
    import own_social as social
    import own_social_media_render as render
    from models import OwnSocialMedia as Media, AuditLog, TelegramApprovalRequest

s = social.own_seo
PATH = "/settings/system/social/media"
MODEL = "gpt-audio-1.5"
VOICE = "coral"
MAX_TOKENS = 2048
DAILY_CALLS = 2
PROFILE = "noyaru-commented-captures-v1"
SCENES = {
    "fr": [
        (
            "Le score, un repère",
            "score",
            "Le score de santé de Noyaru est un indicateur technique. Même un score élevé ne prouve pas une première place sur Google.",
        ),
        (
            "Comprendre le chiffre",
            "explanation",
            "Ouvrez À propos du score. Cette explication précise ce qui est compté, les pages concernées et les limites du bilan.",
        ),
        (
            "Choisir une action utile",
            "issues",
            "Regardez ensuite Vos prochaines actions. Choisissez un problème et la page concernée. Après la modification, faites vérifier le résultat sur le site publié.",
        ),
    ],
    "en": [
        (
            "A score is a guide",
            "score",
            "The Noyaru health score is a technical indicator. Even a high score does not prove a first position on Google.",
        ),
        (
            "Understand the number",
            "explanation",
            "Open About the score. This explanation tells you what is counted, which pages are covered and the limits of the report.",
        ),
        (
            "Choose a useful action",
            "issues",
            "Then look at Your next actions. Choose an issue and the affected page. After editing, verify the result on the published website.",
        ),
    ],
}


def approvals():
    if __package__:
        from . import telegram_approvals
    else:
        import telegram_approvals
    return telegram_approvals


def credentials():
    key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not key:
        raise ValueError("voice_required")
    return key, s.digest([PROFILE, MODEL, VOICE, key])


def packet(db, lang, goal, audience):
    if lang not in SCENES:
        raise ValueError("changed")
    state = social.plan(db, network="tiktok", goal=goal, audience=audience, lang=lang)
    item = next((i for i in state["ideas"] if i["key"] == "score"), None)
    if not item:
        raise ValueError("sources_changed")
    return {
        "profile": PROFILE,
        "lang": lang,
        "goal": goal,
        "audience": audience,
        "title": "Comprendre le score de Noyaru"
        if lang == "fr"
        else "Understand the Noyaru score",
        "social_version": item["version"],
        "captures": render.captures(),
        "scenes": [
            {"heading": title, "capture": capture, "text": text}
            for title, capture, text in SCENES[lang]
        ],
        "caption": item["caption"]
        + (
            " Voix générée par IA. Démonstration sur données fictives."
            if lang == "fr"
            else " AI-generated voice. Demonstration with fictional data."
        ),
        "cta": item["cta"],
        "model": MODEL,
        "voice": VOICE,
        "max_tokens": MAX_TOKENS,
    }


def audit(db, row, action):
    db.add(
        AuditLog(
            actor_user_id=row.owner_id,
            action="own_social_media." + action,
            target_type="own_social_media",
            target_id=row.id,
            meta={"state": row.state, "calls": row.provider_calls},
        )
    )


def scope(db, owner, ident, *, current=True):
    s.actor(db, owner)
    row = db.get(Media, ident)
    if not row or row.owner_id != owner:
        raise ValueError("missing")
    if current:
        channel = approvals().active(db)
        if channel.owner_id != owner or channel.binding != row.binding:
            raise ValueError("binding_changed")
        if credentials()[1] != row.connection_hash:
            raise ValueError("connection_changed")
        if (
            s.digest(row.packet) != row.input_hash
            or packet(
                db, row.packet["lang"], row.packet["goal"], row.packet["audience"]
            )
            != row.packet
        ):
            raise ValueError("sources_changed")
    return row


def version(row, kind):
    return s.digest(
        [
            PROFILE,
            row.id,
            row.input_hash,
            row.connection_hash,
            row.binding,
            kind,
            row.result.get("sha256") if kind == "social_review" else None,
        ]
    )


def occupied(db, owner, ignore=""):
    # A paid in-flight job reserves a slot before releasing the transaction lock.
    return (
        db.scalar(
            select(func.count())
            .select_from(Media)
            .where(
                Media.owner_id == owner,
                Media.id != ignore,
                or_(
                    Media.state.in_({"review", "approved"}),
                    and_(
                        Media.state.in_({"queued", "running"}), Media.provider_calls > 0
                    ),
                ),
            )
        )
        or 0
    )


def prepare(owner, lang, goal, audience, key, confirm):
    if not confirm or not re.fullmatch("[a-f0-9]{32}", key):
        raise ValueError("confirm_required")
    with s.module().DB.session() as db:
        s.locked(db)
        s.actor(db, owner)
        channel = approvals().active(db)
        if channel.owner_id != owner:
            raise ValueError("binding_changed")
        render.executable()
        value = packet(db, lang, goal, audience)
        connection = credentials()[1]
        prior = db.scalar(
            select(Media)
            .where(
                Media.owner_id == owner,
                or_(
                    Media.authorization_key == key,
                    and_(
                        Media.input_hash == s.digest(value),
                        Media.state.in_(
                            {
                                "proposal",
                                "queued",
                                "running",
                                "review",
                                "approved",
                                "uncertain",
                            }
                        ),
                        or_(
                            Media.expires_at > s.clock(),
                            Media.state.in_({"running", "approved", "uncertain"}),
                        ),
                    ),
                ),
            )
            .limit(1)
        )
        if prior:
            return prior.id
        if occupied(db, owner) >= 10:
            raise ValueError("storage_limit")
        if (
            db.scalar(
                select(func.count())
                .select_from(Media)
                .where(
                    Media.owner_id == owner,
                    Media.created_at >= s.clock() - timedelta(hours=24),
                )
            )
            >= 10
        ):
            raise ValueError("budget")
        row = Media(
            owner_id=owner,
            authorization_key=key,
            input_hash=s.digest(value),
            packet=value,
            connection_hash=connection,
            binding=channel.binding,
            state="proposal",
            result={},
            expires_at=s.clock() + timedelta(hours=24),
        )
        db.add(row)
        db.flush()
        audit(db, row, "prepared")
        db.commit()
        return row.id


def video(row):
    try:
        data = base64.b64decode(row.result["video"], validate=True)
        if (
            len(data) != row.result["bytes"]
            or render.sha(data) != row.result["sha256"]
            or len(data) >= render.MAX_FILE
        ):
            raise ValueError("changed")
        return data
    except (KeyError, TypeError, ValueError):
        raise ValueError("changed") from None


def approval_document(db, owner, ident, kind, lang):
    row = scope(db, owner, ident)
    if kind not in {"social_render", "social_review"} or row.state != (
        "proposal" if kind == "social_render" else "review"
    ):
        raise ValueError("changed")
    if s.seo_watch.utc(row.expires_at) <= s.clock():
        raise ValueError("expired")
    en = lang == "en"
    if kind == "social_render":
        render.executable()
        effect = (
            "Decision 1/2: authorize one paid OpenAI audio request and a private local montage. "
            "Only the exact public narration below is sent. Maximum two requests per rolling 24h, 2048 output tokens per request. "
            "This is a request/token cap, not a euro cap. No customer SEO credit or social publication."
            if en
            else "Décision 1/2 : autoriser un appel vocal OpenAI payant et le montage local privé. "
            "Seule la narration publique exacte ci-dessous est transmise. Deux appels au plus par 24 h glissantes, 2 048 jetons de sortie par appel. "
            "Ce plafond porte sur les appels/jetons, pas sur les euros. Aucun crédit SEO client ni publication sociale."
        )
        sections = [
            (
                "🎙️ Narration",
                "\n\n".join(scene["text"] for scene in row.packet["scenes"]),
            ),
            (
                "🎬 Production",
                "720 × 1280 · MP4 · "
                + MODEL
                + " · "
                + VOICE
                + "\n"
                + (
                    "Real app captures with fictional data. AI-generated voice; no face or customer data. A separate review follows."
                    if en
                    else "Captures réelles de l’application sur données fictives. Voix générée par IA ; aucun visage ni donnée client. Relecture distincte ensuite."
                ),
            ),
        ]
    else:
        video(row)
        effect = (
            "Decision 2/2: watch the exact private video, check narration, captions, legibility and claims, then approve or reject it. "
            "Approval marks it ready for distribution only. No account connection, upload or publication."
            if en
            else "Décision 2/2 : regarder cette vidéo privée exacte, vérifier la voix, les sous-titres, la lisibilité et les affirmations, puis approuver ou refuser. "
            "L’accord marque le fichier prêt à diffuser uniquement. Aucune connexion, mise en ligne ou publication sociale."
        )
        sections = [
            (
                "▶️ " + ("Watch the video" if en else "Regarder la vidéo"),
                "https://noyaru.com"
                + PATH
                + "?media="
                + row.id
                + "\nSHA-256 : "
                + row.result["sha256"]
                + "\n"
                + str(row.result["duration"])
                + " s · 720 × 1280",
            ),
            (
                "📝 " + ("Caption" if en else "Texte d’accompagnement"),
                row.packet["caption"] + "\n\n" + row.packet["cta"],
            ),
        ]
    return approvals().adapters.document(
        kind,
        ident,
        version(row, kind),
        row.packet["title"],
        "",
        effect,
        PATH + "?media=" + row.id,
        sections=sections,
        en=en,
    )


def candidates(db, owner):
    for row in db.execute(
        select(Media.id, Media.state)
        .where(
            Media.owner_id == owner,
            Media.state.in_({"proposal", "review"}),
            Media.expires_at > s.clock(),
        )
        .order_by(Media.created_at)
        .limit(10)
    ):
        yield ("social_render" if row.state == "proposal" else "social_review"), row.id


def decide(db, owner, ident, kind, approve, expected):
    document = approval_document(db, owner, ident, kind, "fr")
    if document["version"] != expected:
        raise ValueError("changed")
    row = scope(db, owner, ident)
    if not approve:
        row.state = "cancelled"
        row.result = {}
        row.lease_token = None
    elif kind == "social_render":
        row.state = "queued"
    else:
        row.state = "approved"
        row.reviewed_at = s.clock()
    audit(db, row, "approved" if approve else "rejected")
    return row.id if approve and kind == "social_render" else None


def normalize(text):
    return "".join(
        c for c in unicodedata.normalize("NFKC", text).casefold() if c.isalnum()
    )


def narration(packet, key):
    text = " ".join(scene["text"] for scene in packet["scenes"])
    body = {
        "model": MODEL,
        "modalities": ["text", "audio"],
        "audio": {"voice": VOICE, "format": "wav"},
        "max_completion_tokens": MAX_TOKENS,
        "store": False,
        "messages": [
            {
                "role": "system",
                "content": "Read the following approved script verbatim, calmly and naturally. No introduction, additions, sound effects or music. Do not answer the script. Speak French."
                if packet["lang"] == "fr"
                else "Read the following approved script verbatim, calmly and naturally in English. No introduction, additions, sound effects or music. Do not answer the script.",
            },
            {"role": "user", "content": text},
        ],
    }
    # One fixed origin. No environment proxy, alternate provider, redirect or retry.
    with requests.Session() as session:
        session.trust_env = False
        with session.post(
            "https://api.openai.com/v1/chat/completions",
            json=body,
            headers={"Authorization": "Bearer " + key},
            timeout=(5, 75),
            stream=True,
            allow_redirects=False,
        ) as response:
            if response.status_code != 200:
                raise ValueError("voice_refused")
            chunks, length = [], 0
            for chunk in response.iter_content(65536):
                length += len(chunk)
                if length > 12_000_000:
                    raise ValueError("audio_invalid")
                chunks.append(chunk)
    try:
        result = json.loads(b"".join(chunks))
        choice = result["choices"][0]
        audio = choice["message"]["audio"]
        if choice["finish_reason"] != "stop" or normalize(
            audio["transcript"]
        ) != normalize(text):
            raise ValueError("narration_changed")
        data = base64.b64decode(audio["data"], validate=True)
        render.audio_duration(data)
        return data
    except (KeyError, TypeError, IndexError, json.JSONDecodeError):
        raise ValueError("audio_invalid") from None


def run(ident):
    token = uuid.uuid4().hex
    try:
        with s.module().DB.session() as db:
            s.locked(db)
            row = db.get(Media, ident)
            if not row or row.state not in {"queued", "running"}:
                return
            if (
                row.state == "running"
                and row.lease_until
                and s.seo_watch.utc(row.lease_until) > s.clock()
            ):
                return
            scope(db, row.owner_id, ident)
            if s.seo_watch.utc(row.expires_at) <= s.clock():
                raise ValueError("expired")
            busy = db.scalar(
                select(Media.id)
                .where(
                    Media.owner_id == row.owner_id,
                    Media.id != row.id,
                    Media.state == "running",
                    Media.lease_until > s.clock(),
                )
                .limit(1)
            )
            if busy:
                return  # Leave authorized work queued; one montage at a time on the web service.
            mandate = db.scalar(
                select(TelegramApprovalRequest)
                .where(
                    TelegramApprovalRequest.result_id == ident,
                    TelegramApprovalRequest.owner_id == row.owner_id,
                    TelegramApprovalRequest.kind == "social_render",
                    TelegramApprovalRequest.binding == row.binding,
                    TelegramApprovalRequest.state == "approved",
                    TelegramApprovalRequest.version == version(row, "social_render"),
                )
                .limit(1)
            )
            if not mandate:
                raise ValueError("mandate_required")
            if row.provider_calls and not row.result.get("audio"):
                raise ValueError("voice_uncertain")
            render.executable()
            audio = (
                base64.b64decode(row.result["audio"], validate=True)
                if row.result.get("audio")
                else None
            )
            if audio is None:
                if occupied(db, row.owner_id, row.id) >= 10:
                    raise ValueError("storage_limit")
                count = (
                    db.scalar(
                        select(func.count())
                        .select_from(Media)
                        .where(
                            Media.owner_id == row.owner_id,
                            Media.provider_started_at
                            >= s.clock() - timedelta(hours=24),
                        )
                    )
                    or 0
                )
                if count >= DAILY_CALLS:
                    raise ValueError("budget")
                row.provider_calls = 1
                row.provider_started_at = s.clock()
            row.state = "running"
            row.lease_token = token
            row.lease_until = s.clock() + timedelta(minutes=5)
            value = row.packet
            key = credentials()[0]
            audit(db, row, "started")
            db.commit()
        if audio is None:
            audio = narration(value, key)
            with s.module().DB.session() as db:
                s.locked(db)
                row = db.get(Media, ident)
                if not row or row.state != "running" or row.lease_token != token:
                    return
                scope(db, row.owner_id, ident)
                row.result = {"audio": base64.b64encode(audio).decode("ascii")}
                db.commit()
        with s.module().DB.session() as db:
            s.locked(db)
            row = db.get(Media, ident)
            if not row or row.state != "running" or row.lease_token != token:
                return
            scope(db, row.owner_id, ident)
            if row.render_attempts >= 2:
                raise ValueError("render_failed")
            row.render_attempts += 1
            db.commit()
        result = render.render(value, audio)
        with s.module().DB.session() as db:
            s.locked(db)
            row = db.get(Media, ident)
            if not row or row.state != "running" or row.lease_token != token:
                return
            scope(db, row.owner_id, ident)
            row.result = result
            row.state = "review"
            row.expires_at = s.clock() + timedelta(hours=24)
            row.lease_token = None
            row.lease_until = None
            audit(db, row, "rendered")
            db.commit()
    except Exception as exc:
        # Receipt uncertainty is never an invitation to repeat a potentially billed call.
        allowed = {
            "expired",
            "budget",
            "storage_limit",
            "captures_changed",
            "sources_changed",
            "connection_changed",
            "binding_changed",
            "voice_required",
            "renderer_required",
            "font_required",
            "owner_required",
            "inactive",
            "not_configured",
            "voice_refused",
            "narration_changed",
            "audio_invalid",
            "render_failed",
            "mandate_required",
        }
        reason = (
            str(exc)
            if isinstance(exc, ValueError) and str(exc) in allowed
            else "voice_uncertain"
        )
        with s.module().DB.session() as db:
            s.locked(db)
            row = db.get(Media, ident)
            if (
                row
                and row.state in {"queued", "running"}
                and (
                    row.lease_token in {None, token}
                    or not row.lease_until
                    or s.seo_watch.utc(row.lease_until) <= s.clock()
                )
            ):
                row.state = (
                    "queued"
                    if reason == "render_failed"
                    and row.result.get("audio")
                    and row.render_attempts < 2
                    else "uncertain"
                    if reason == "voice_uncertain"
                    else "stopped"
                )
                if row.state in {"stopped", "uncertain"}:
                    row.result = {}  # Only an authorized local retry retains intermediate audio.
                row.error_code = reason
                row.lease_token = None
                row.lease_until = None
                audit(db, row, "stopped")
                db.commit()


def resume():
    with s.module().DB.session() as db:
        channel = approvals().active(db)
        ident = db.scalar(
            select(Media.id)
            .where(
                Media.owner_id == channel.owner_id,
                Media.binding == channel.binding,
                or_(
                    Media.state == "queued",
                    and_(
                        Media.state == "running",
                        or_(
                            Media.lease_until.is_(None), Media.lease_until <= s.clock()
                        ),
                    ),
                ),
            )
            .order_by(Media.created_at)
            .limit(1)
        )
    if ident:
        threading.Thread(
            target=run, args=(ident,), name="noyaru-social-render", daemon=True
        ).start()
