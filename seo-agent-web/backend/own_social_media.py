"""One bounded narration and local montage, then separate owner review. No publishing."""

import base64
import binascii
import difflib
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
    from .models import (
        OwnSocialMedia as Media,
        AuditLog,
        TelegramApprovalRequest,
        OwnSocialMediaPolicy,
    )
except ImportError:
    import own_social as social
    import own_social_media_render as render
    from models import (
        OwnSocialMedia as Media,
        AuditLog,
        TelegramApprovalRequest,
        OwnSocialMediaPolicy,
    )

s = social.own_seo
PATH = "/settings/system/social/media"
MODEL = "gpt-audio-1.5"
VOICE = "coral"
MAX_TOKENS = 4096
MAX_TRANSCRIPT = 4000
DEFAULT_DAILY_CALLS = 10
MAX_DAILY_CALLS = 100
LEGACY_DAILY_CALLS = 2
PROFILE = "noyaru-commented-captures-v2"
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
        current_packet = packet(
            db, row.packet["lang"], row.packet["goal"], row.packet["audience"]
        )
        if "daily_limit" in row.packet:
            current_packet["daily_limit"] = request_limit(row)
        if s.digest(row.packet) != row.input_hash or current_packet != row.packet:
            raise ValueError("sources_changed")
    return row


def version(row, kind):
    fields = [
        PROFILE,
        row.id,
        row.input_hash,
        row.connection_hash,
        row.binding,
        kind,
        row.result.get("sha256") if kind == "social_review" else None,
    ]
    if kind == "social_review" and row.result.get("narration_check") is not None:
        fields.append(row.result["narration_check"])
    return s.digest(fields)


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


def policy(db, owner):
    row = db.get(OwnSocialMediaPolicy, owner)
    limit = row.daily_calls if row else DEFAULT_DAILY_CALLS
    if type(limit) is not int or not 1 <= limit <= MAX_DAILY_CALLS:
        raise ValueError("limit_invalid")
    return {
        "limit": limit,
        "token": row.token if row else "",
        "max": MAX_DAILY_CALLS,
        "preparations": max(10, 2 * limit),
    }


def set_policy(owner, value, expected):
    """Explicit owner edit, serialized with provider reservations; no work is started."""
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,3}", value):
        raise ValueError("limit_invalid")
    limit = int(value)
    if not 1 <= limit <= MAX_DAILY_CALLS:
        raise ValueError("limit_invalid")
    with s.module().DB.session() as db:
        s.locked(db)
        s.actor(db, owner)
        prior = policy(db, owner)
        if prior["token"] != expected:
            raise ValueError("limit_changed")
        row = db.get(OwnSocialMediaPolicy, owner)
        if row and prior["limit"] == limit:
            return
        if not row:
            row = OwnSocialMediaPolicy(owner_id=owner)
            db.add(row)
        row.daily_calls, row.token = limit, uuid.uuid4().hex
        db.add(
            AuditLog(
                actor_user_id=owner,
                action="own_social_media.limit_changed",
                target_type="own_social_media_policy",
                target_id=owner,
                meta={"before": prior["limit"], "after": limit},
            )
        )
        db.commit()


def request_limit(row):
    # Legacy cards promised two requests. Raising today's setting never widens them.
    value = row.packet.get("daily_limit", LEGACY_DAILY_CALLS)
    if type(value) is not int or not 1 <= value <= MAX_DAILY_CALLS:
        raise ValueError("limit_invalid")
    return value


def audio_budget(db, owner, *, mandate_limit=None):
    """Read-only rolling availability; a slot is not a reservation or a mandate."""
    limit = policy(db, owner)["limit"]
    if mandate_limit is not None:
        limit = min(limit, mandate_limit)
    starts = list(
        db.scalars(
            select(Media.provider_started_at)
            .where(
                Media.owner_id == owner,
                Media.provider_started_at > s.clock() - timedelta(hours=24),
            )
            .order_by(Media.provider_started_at.desc())
            .limit(limit)
        )
    )
    return {
        "limit": limit,
        "remaining": limit - len(starts),
        # Wait until enough started calls have expired, including after a decrease.
        "retry_at": s.seo_watch.utc(starts[-1]) + timedelta(hours=24)
        if len(starts) >= limit
        else None,
    }


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
        value["daily_limit"] = policy(db, owner)["limit"]
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
        if not audio_budget(db, owner)["remaining"]:
            raise ValueError("budget")
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
            >= policy(db, owner)["preparations"]
        ):
            raise ValueError("preparation_limit")
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


def script(packet):
    return " ".join(scene["text"] for scene in packet["scenes"])


def progress_phase(row):
    """Read durable milestones only; neither a percentage nor a worker heartbeat."""
    if row.state in {"proposal", "queued", "running", "review"} and (
        s.seo_watch.utc(row.expires_at) <= s.clock()
    ):
        return "expired"
    if row.state == "running":
        if not row.lease_until or s.seo_watch.utc(row.lease_until) <= s.clock():
            return "unconfirmed"
        if row.render_attempts:
            return "montage"
        return "voice" if row.provider_started_at else "starting"
    if row.state == "queued" and row.render_attempts:
        return "montage_queued"
    return row.state


def valid_transcript(value):
    return (
        isinstance(value, str)
        and 1 <= len(value) <= MAX_TRANSCRIPT
        and any(char.isalnum() for char in value)
        and not any(
            unicodedata.category(char).startswith("C") and char not in "\n\t\r"
            for char in value
        )
    )


class NarrationReview(ValueError):
    """A complete, valid WAV differs from the script; never an automatic acceptance."""

    def __init__(self, audio, transcript):
        super().__init__("narration_changed")
        self.audio = audio
        self.transcript = transcript


def narration_check(row):
    saved = row.result.get("narration_check")
    if saved is None:
        return None
    expected = script(row.packet)
    if (
        not isinstance(saved, dict)
        or not valid_transcript(saved.get("transcript"))
        or saved.get("sha256") != s.digest([expected, saved["transcript"]])
        or normalize(saved["transcript"]) == normalize(expected)
    ):
        raise ValueError("changed")
    received = saved["transcript"]
    before = re.findall(r"\w+|\s+|[^\w\s]", expected)
    after = re.findall(r"\w+|\s+|[^\w\s]", received)
    parts = []
    for kind, a, b, c, d in difflib.SequenceMatcher(
        None, before, after, autojunk=False
    ).get_opcodes():
        if kind in {"equal", "delete", "replace"}:
            parts.append(
                {
                    "kind": "same" if kind == "equal" else "removed",
                    "text": "".join(before[a:b]),
                }
            )
        if kind in {"insert", "replace"}:
            parts.append({"kind": "added", "text": "".join(after[c:d])})
    return {"expected": expected, "received": received, "parts": parts}


def video(row):
    narration_check(row)
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
    check = None
    if kind == "social_render":
        render.executable()
        limit = request_limit(row)
        effect = (
            "Decision 1/2: authorize one paid OpenAI audio request and a private local montage. "
            f"Only the exact public narration below is sent. Maximum {limit} requests per rolling 24h, {MAX_TOKENS} output tokens per request. "
            "This is a request/token cap, not a euro cap. No customer SEO credit or social publication."
            if en
            else "Décision 1/2 : autoriser un appel vocal OpenAI payant et le montage local privé. "
            f"Seule la narration publique exacte ci-dessous est transmise. {limit} appels au plus par 24 h glissantes, {MAX_TOKENS} jetons de sortie par appel. "
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
        check = narration_check(row)
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
    if check:
        effect = (
            "Decision 2/2: the received transcript differs from the approved script. Listen to the entire video and compare both texts below. "
            "Approve only if you explicitly accept this received narration and its claims; otherwise reject it. Marks this exact file ready only; no publication or paid call."
            if en
            else "Décision 2/2 : la transcription reçue diffère du script approuvé. Écoutez toute la vidéo et comparez les deux textes ci-dessous. "
            "Approuvez uniquement si vous acceptez explicitement cette narration reçue et ses affirmations ; sinon refusez. Ce fichier exact sera marqué prêt uniquement ; aucune publication ni appel payant."
        )
        sections = [
            (
                "⚠️ " + ("Difference to review" if en else "Écart à relire"),
                "The provider transcript is a reading aid, not independent proof of what was spoken."
                if en
                else "La transcription du fournisseur aide la relecture ; elle ne prouve pas indépendamment les mots prononcés.",
            ),
            (
                "📋 " + ("Approved script" if en else "Script approuvé"),
                check["expected"],
            ),
            (
                "🎧 " + ("Received transcript" if en else "Transcription reçue"),
                check["received"],
            ),
        ] + sections
    return approvals().adapters.document(
        kind,
        ident,
        version(row, kind),
        row.packet["title"],
        "",
        effect,
        PATH + "?media=" + row.id,
        sections=sections,
        summary=(
            (
                "Accept the received narration difference: "
                if en
                else "Accepter l’écart de narration reçu : "
            )
            + row.packet["title"]
        )
        if check
        else "",
        en=en,
    )


def candidates(db, owner):
    # At most 200 free preparations per rolling day; inspect metadata only so
    # capped older proposals cannot hide a newer request with a higher limit.
    delivered = 0
    for row in db.execute(
        select(Media.id, Media.state, Media.packet)
        .where(
            Media.owner_id == owner,
            Media.state.in_({"proposal", "review"}),
            Media.expires_at > s.clock(),
        )
        .order_by(Media.created_at)
        .limit(2 * MAX_DAILY_CALLS)
    ):
        if (
            row.state == "proposal"
            and not audio_budget(db, owner, mandate_limit=request_limit(row))[
                "remaining"
            ]
        ):
            continue
        yield ("social_render" if row.state == "proposal" else "social_review"), row.id
        delivered += 1
        if delivered >= 10:
            break


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
        row.error_code = None
        row.reviewed_at = s.clock()
    audit(db, row, "approved" if approve else "rejected")
    return row.id if approve and kind == "social_render" else None


def normalize(text):
    return "".join(
        c for c in unicodedata.normalize("NFKC", text).casefold() if c.isalnum()
    )


def narration(packet, key):
    text = script(packet)
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
        if choice["finish_reason"] == "length":
            raise ValueError("narration_truncated")
        if choice["finish_reason"] != "stop":
            raise ValueError("narration_incomplete")
        audio = choice["message"].get("audio")
        if not isinstance(audio, dict):
            raise ValueError("audio_missing")
        if not valid_transcript(audio.get("transcript")):
            raise ValueError("audio_invalid")
        try:
            data = base64.b64decode(audio["data"], validate=True)
        except (binascii.Error, ValueError, TypeError):
            raise ValueError("audio_encoding") from None
        data = render.read_audio(data)[0]
        if normalize(audio["transcript"]) != normalize(text):
            raise NarrationReview(data, audio["transcript"])
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
                if not audio_budget(db, row.owner_id, mandate_limit=request_limit(row))[
                    "remaining"
                ]:
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
            check = None
            try:
                audio = narration(value, key)
            except NarrationReview as received:
                audio = render.read_audio(received.audio)[0]
                if not valid_transcript(received.transcript):
                    raise ValueError("audio_invalid") from None
                check = {
                    "transcript": received.transcript,
                    "sha256": s.digest([script(value), received.transcript]),
                }
            with s.module().DB.session() as db:
                s.locked(db)
                row = db.get(Media, ident)
                if not row or row.state != "running" or row.lease_token != token:
                    return
                scope(db, row.owner_id, ident)
                row.result = {"audio": base64.b64encode(audio).decode("ascii")}
                if check:
                    row.result = {**row.result, "narration_check": check}
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
            narration_check(row)
            check = row.result.get("narration_check")
            db.commit()
        result = render.render(value, audio)
        if check:
            result["narration_check"] = check
        with s.module().DB.session() as db:
            s.locked(db)
            row = db.get(Media, ident)
            if not row or row.state != "running" or row.lease_token != token:
                return
            scope(db, row.owner_id, ident)
            row.result = result
            row.state = "review"
            row.error_code = "narration_review" if check else None
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
            "limit_invalid",
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
            "narration_truncated",
            "narration_incomplete",
            "audio_invalid",
            "audio_missing",
            "audio_encoding",
            "audio_format",
            "audio_incomplete",
            "audio_duration",
            "mandate_required",
        } | render.LOCAL_ERRORS
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
                    if reason in render.LOCAL_ERRORS
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
