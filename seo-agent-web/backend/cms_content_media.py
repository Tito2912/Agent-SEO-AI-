"""Validated image bodies for explicitly consented WordPress/Ghost transfers.

No download, external upload, journal write or cleanup is performed here.
"""
from __future__ import annotations

import io
import re
import uuid
import warnings
from dataclasses import dataclass
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit

from PIL import Image, UnidentifiedImageError

from .cms_commun import CmsError

MAX_IMAGE_BYTES = 10 * 1024 * 1024
MAX_IMAGE_PIXELS = 16_000_000
MAX_IMAGE_DIMENSION = 8192
_FORMATS = {"image/png": ("PNG", {"png"}), "image/jpeg": ("JPEG", {"jpg", "jpeg"}),
            "image/webp": ("WEBP", {"webp"})}


@dataclass(frozen=True)
class ValidatedImage:
    data: bytes
    filename: str
    content_type: str
    width: int
    height: int


def _invalid():
    return CmsError("Choisissez une image PNG, JPEG ou WebP valide, de taille autorisée.", code="image_invalid")


def validate_image(*, data, filename, content_type):
    """Decode the real image before any network operation; reject deceptive MIME."""
    if (not isinstance(data, bytes) or not data or len(data) > MAX_IMAGE_BYTES
            or not isinstance(content_type, str) or content_type not in _FORMATS or not isinstance(filename, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,139}\.(png|jpg|jpeg|webp)", filename)
            or ".." in filename):
        raise _invalid()
    expected, extensions = _FORMATS[content_type]
    if filename.rsplit(".", 1)[1] not in extensions:
        raise _invalid()
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(data)) as image:
                width, height = image.size
                if (image.format != expected or min(width, height) <= 0
                        or max(width, height) > MAX_IMAGE_DIMENSION
                        or width * height > MAX_IMAGE_PIXELS
                        or getattr(image, "n_frames", 1) != 1):
                    raise _invalid()
                image.verify()
            # verify() does not decode every format's pixels. A truncated JPEG,
            # for example, must not pass because its header is well formed.
            with Image.open(io.BytesIO(data)) as image:
                image.load()
    except CmsError:
        raise
    except (OSError, ValueError, SyntaxError, UnidentifiedImageError,
            Image.DecompressionBombError, Image.DecompressionBombWarning):
        raise _invalid() from None
    return ValidatedImage(data, filename, content_type, width, height)


def ghost_multipart(image):
    """One image and explicit purpose=image; filename was validated, not escaped."""
    boundary = "noyaru-" + uuid.uuid4().hex
    # A randomized boundary is not an idempotency key and is never persisted.
    if boundary.encode("ascii") in image.data:
        raise _invalid()
    body = (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{image.filename}"\r\n'
            f'Content-Type: {image.content_type}\r\n\r\n').encode("ascii")
    body += image.data
    body += (f'\r\n--{boundary}\r\nContent-Disposition: form-data; name="purpose"\r\n\r\n'
             f'image\r\n--{boundary}--\r\n').encode("ascii")
    return body, "multipart/form-data; boundary=" + boundary


def public_image_url(value, *, project_url, resolve_public, relative=False):
    """Validate the CMS-returned URL without fetching it or forwarding credentials.

    Ghost relative paths resolve against the public project, never the admin host.
    Temporary signed/query URLs are refused rather than saved in the journal.
    """
    try:
        if (not isinstance(value, str) or not value or len(value) > 2048
                or value != value.strip() or value.startswith("//")
                or any(ord(c) <= 32 or ord(c) == 127 or c in '<>"\\' for c in value)):
            raise ValueError()
        p = urlsplit(value)
        if relative and not p.scheme and not p.netloc:
            value = urljoin(project_url.rstrip("/") + "/", value)
            p = urlsplit(value)
        if (p.scheme != "https" or not p.hostname or p.username or p.password
                or p.port not in {None, 443} or p.query or p.fragment):
            raise ValueError()
        decoded = p.path
        for _ in range(4):
            decoded = unquote(decoded)
        if (any(ord(c) <= 32 or ord(c) == 127 or c in '<>"\\' for c in decoded)
                or "%" in decoded or any(part in {".", ".."} for part in decoded.split("/"))):
            raise ValueError()
        host = p.hostname.encode("idna").decode("ascii").lower()
        resolve_public(host, "média CMS")
        return urlunsplit(("https", host, p.path or "/", "", ""))
    except (ValueError, UnicodeError, CmsError):
        raise CmsError("Le média a pu être enregistré, mais son adresse publique n’a pas pu être confirmée.",
                       code="image_url", uncertain=True) from None
