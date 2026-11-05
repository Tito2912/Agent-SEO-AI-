"""Bounded MIME selection and reduced questions; emails never authorize tools."""

import base64
from email import policy
from email.parser import HeaderParser
import re

try:
    from . import own_support_common as c
except ImportError:
    import own_support_common as c

BLOCKED = {"SENT", "DRAFT", "SPAM", "TRASH", "CHAT"}
SENSITIVE = re.compile(
    r"rembours|refund|chargeback|pirat|hacked|mot de passe|password|supprim.{0,24}compte|delete.{0,24}account|donn[ée]es personnelles|personal data",
    re.I,
)


def addresses(value, *, single=False):
    if (
        not isinstance(value, str)
        or len(value) > 1200
        or any(x in value for x in "\r\n\x00")
    ):
        raise c.Stop("sender_unconfirmed")
    try:
        header = HeaderParser(policy=policy.default).parsestr(
            "From: " + value + "\n\n"
        )["From"]
        if not header or header.defects:
            raise ValueError()
        # Preserve the local part: some email providers treat it as case sensitive.
        result = [a.username + "@" + a.domain.casefold() for a in header.addresses]
        if not result or (single and len(result) != 1) or len(result) > 10:
            raise ValueError()
        for addr in result:
            if len(addr) > 254 or not re.fullmatch(
                r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9.-]{0,250}[A-Za-z0-9])?\.[A-Za-z]{2,63}",
                addr,
            ):
                raise ValueError()
        return result
    except Exception:
        raise c.Stop("sender_unconfirmed") from None


def headers(message):
    values = {}
    source = (message.get("payload") or {}).get("headers")
    if not isinstance(source, list) or len(source) > 100:
        raise c.Stop("message_changed")
    for item in source:
        name = str(item.get("name", "")).casefold()
        value = item.get("value")
        if not isinstance(value, str) or len(value) > 4000:
            raise c.Stop("message_changed")
        if name in values and name in {
            "from",
            "to",
            "message-id",
            "reply-to",
            "subject",
            "auto-submitted",
        }:
            raise c.Stop("sender_unconfirmed")
        values[name] = value
    return values


def reduce(text):
    """Strip quoted history and common secrets. Not a guarantee of anonymization."""
    lines = []
    for line in text.splitlines():
        if re.match(
            r"^(?:On .{1,180}wrote:|Le .{1,180}écrit\s*:|[-_]{5,}|--\s*$)", line, re.I
        ):
            break
        if not line.lstrip().startswith(">"):
            lines.append(line)
    text = "\n".join(lines)
    text = re.sub(r"https?://\S+|www\.\S+", "[lien retiré]", text, flags=re.I)
    text = re.sub(
        r"[A-Za-z0-9.!#$%&\x27*+/=?^_`{|}~-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
        "[adresse retirée]",
        text,
    )
    text = re.sub(
        r"\b(?:sk-(?:proj-|ant-)?[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9_]{8,}|github_pat_[A-Za-z0-9_]{8,}|eyJ[A-Za-z0-9_.-]{20,})",
        "[secret retiré]",
        text,
    )
    text = re.sub(
        r"(?im)\b(?:password|mot de passe|api.?key|token|secret|authorization)\s*[:=]\s*\S+",
        "[secret retiré]",
        text,
    )
    text = re.sub(r"(?i)\bBearer\s+\S+", "[secret retiré]", text)
    text = re.sub(r"(?<!\w)(?:\+?\d[\d ()+.-]{7,}\d)(?!\w)", "[numéro retiré]", text)
    return "".join(ch for ch in text if ord(ch) >= 32 or ch == "\n").strip()


def envelope(message, cfg):
    c.google_id(message.get("id"))
    c.google_id(message.get("threadId"))
    labels = set(message.get("labelIds") or [])
    if cfg.label_id not in labels or labels & BLOCKED:
        raise c.Stop("outside_label")
    h = headers(message)
    if c.CONTACT not in {addr.casefold() for addr in addresses(h.get("to", ""))}:
        raise c.Stop("outside_support")
    sender = addresses(h.get("from", ""), single=True)[0]
    if sender.casefold() in {c.CONTACT, cfg.mailbox.casefold()}:
        raise c.Stop("loop_blocked")
    if (
        h.get("auto-submitted", "no").casefold() != "no"
        or h.get("list-id")
        or h.get("precedence", "").casefold() in {"bulk", "junk", "list"}
    ):
        raise c.Stop("loop_blocked")
    if h.get("reply-to") and addresses(h["reply-to"], single=True) != [sender]:
        raise c.Stop("sender_unconfirmed")
    ident = h.get("message-id", "")
    if (
        not re.fullmatch(r"<[A-Za-z0-9.!#$%&\x27*+/=?^_`{|}~@-]{3,220}>", ident)
        or ident.count("@") != 1
    ):
        raise c.Stop("sender_unconfirmed")
    subject = str(
        HeaderParser(policy=policy.default).parsestr(
            "Subject: " + h.get("subject", "") + "\n\n"
        )["Subject"]
        or ""
    )
    if (
        not subject.strip()
        or len(subject) > 180
        or any(x in subject for x in "\r\n\x00")
    ):
        raise c.Stop("message_limit")
    return {
        "from": sender,
        "message_id": ident,
        "subject": reduce(subject),
        "text": "",
        "received_at": str(message.get("internalDate") or ""),
        "identity_verified": False,
    }


def admissible(part, depth=0):
    """Called on a data-free MIME tree before any body data is requested."""
    if (
        not isinstance(part, dict)
        or depth > 2
        or part.get("filename")
        or (part.get("body") or {}).get("attachmentId")
    ):
        raise c.Stop("attachments_require_human")
    kind = part.get("mimeType")
    if kind in {"text/plain", "text/html"}:
        if (part.get("body") or {}).get("size", 0) > 16000:
            raise c.Stop("message_limit")
        return kind == "text/plain"
    children = part.get("parts")
    if (
        kind != "multipart/alternative"
        or not isinstance(children, list)
        or not 1 <= len(children) <= 3
    ):
        raise c.Stop("attachments_require_human")
    found = False
    for child in children:
        found = admissible(child, depth + 1) or found
    return found


def plain_body(message):
    payload = message.get("payload") or {}
    if not admissible(payload):
        raise c.Stop("plain_text_required")
    parts = (
        [payload]
        if payload.get("mimeType") == "text/plain"
        else [p for p in payload.get("parts", []) if p.get("mimeType") == "text/plain"]
    )
    if len(parts) != 1:
        raise c.Stop("plain_text_required")
    data = (parts[0].get("body") or {}).get("data")
    if (
        not isinstance(data, str)
        or len(data) > 22000
        or not re.fullmatch("[A-Za-z0-9_-]*={0,2}", data)
    ):
        raise c.Stop("message_limit")
    try:
        raw = base64.b64decode(
            data + "=" * ((-len(data)) % 4), altchars=b"-_", validate=True
        ).decode("utf-8")
    except Exception:
        raise c.Stop("plain_text_required") from None
    text = reduce(raw)
    if not 10 <= len(text) <= 3000:
        raise c.Stop("message_limit")
    return text
