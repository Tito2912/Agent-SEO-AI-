"""Adaptateur Ghost Admin API (posts, source=html, formats=html).

Endpoints utilises (API officielle du 19/11/2026) :
- POST /ghost/api/admin/posts/?source=html&formats=html       cree un post (status='draft' par defaut)
- GET  /ghost/api/admin/posts/{id}/?formats=html              relit un post
- PUT  /ghost/api/admin/posts/{id}/?source=html&formats=html  met a jour (updated_at obligatoire)

Points durs :
- Ghost 6.73 peut conserver le meme `updated_at` apres une edition rapide (resolution
  seconde). La revision Noyaru renvoyee a l'appelant est donc un hash SHA-256 d'un
  snapshot stable du post (`id`, `title`, `slug`, `html`, `custom_excerpt`, `status`,
  `updated_at`) : deux snapshots egaux donnent le meme hash, deux snapshots qui
  different d'un seul espace dans le HTML produisent un hash different meme si
  `updated_at` n'a pas bouge. Aucun champ d'auteur ou de cle n'est invente.
- Le hash sert UNIQUEMENT comme `expected_revision` cote appelant. Pour toute ecriture
  API (PUT) on envoie TOUJOURS le vrai `updated_at` relu juste avant. Aucune date n'est
  fabriquee ni derivee du hash.
- Le header `Accept-Version` est systematique (>= v5.0).
- JWT HS256 court prepare par le transport injecte : rien n'est signe ici.
- On n'envoie PAS la newsletter : `send_email_when_published` reste a `false` ; aucune
  option `email_segment` n'est transmise.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Optional, Tuple
from urllib.parse import quote

from .base import (
    AdapterError,
    ArticleDraft,
    Capabilities,
    RemoteArticle,
    TransportFn,
    TransportResponse,
    check_revision,
    require_config,
    require_draft_fields,
    wrap_transport_errors,
)
from .html_safe import validate_html


ADMIN_API_VERSION = "v5.0"
REQUIRED_CONFIG = ()

# Champs qui entrent dans la revision Noyaru. Seuls des champs retournes par Ghost
# sous `formats=html` sont retenus : pas d'auteur ni de cle inventee.
_REVISION_FIELDS = ("id", "title", "slug", "html", "custom_excerpt", "status", "updated_at")


def _compute_revision(post: Mapping[str, Any]) -> str:
    """Hash SHA-256 d'un snapshot canonique du post Ghost.

    Les champs sont lus dans l'ordre fixe `_REVISION_FIELDS` et serialises en JSON
    canonique (cles triees, separateurs serres) : deux reponses Ghost qui ne different
    que par l'ordre des cles donnent le meme hash, mais un espace ajoute au HTML, un
    changement de `status` ou une seconde supplementaire sur `updated_at` produit un
    hash different.
    """
    snapshot = {name: post.get(name) for name in _REVISION_FIELDS}
    serialized = json.dumps(
        snapshot, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


class GhostAdapter:
    platform = "ghost"
    capabilities = Capabilities(
        create_draft=True, read=True, publish=True, update_draft=True,
        upload_images=False,
        requires=REQUIRED_CONFIG,
        notes=("Ghost Admin >= 5.0. Le transport injecte le JWT court et Accept-Version. "
               "La revision Noyaru est un hash SHA-256 du contenu (resiste aux editions "
               "a `updated_at` identique). Aucune newsletter n'est envoyee par cet adaptateur."),
    )

    def __init__(self, *, transport: TransportFn, config: Mapping[str, Any]):
        require_config(config, REQUIRED_CONFIG)
        self.transport = transport
        self.config = dict(config or {})

    # ---- Operations publiques --------------------------------------------
    def validate_draft(self, draft: ArticleDraft) -> None:
        """Validate the draft without network access or a media transfer."""
        require_draft_fields(draft)
        validate_html(draft.html)

    def create_draft(self, draft: ArticleDraft) -> RemoteArticle:
        self.validate_draft(draft)
        require_draft_fields(draft)
        validate_html(draft.html)
        body = {"posts": [self._payload(draft, updated_at=None, status="draft")]}
        response = wrap_transport_errors(
            lambda: self.transport(
                "POST", "/ghost/api/admin/posts/?source=html&formats=html",
                payload=body,
                headers={"Accept": "application/json", "Accept-Version": ADMIN_API_VERSION}),
            write=True, cms="Ghost")
        article, _ = self._first_post(self._expect_ok(response, write=True), write=True)
        return article

    def read(self, remote_id: str) -> RemoteArticle:
        article, _ = self._read_internal(remote_id)
        return article

    def publish(self, remote_id: str, expected_revision: Optional[str] = None) -> RemoteArticle:
        current, real_updated_at = self._read_internal(remote_id)
        check_revision(expected_revision, current.revision)
        body = {"posts": [{"status": "published", "updated_at": real_updated_at}]}
        response = wrap_transport_errors(
            lambda: self.transport(
                "PUT", self._write_path(remote_id), payload=body,
                headers={"Accept": "application/json", "Accept-Version": ADMIN_API_VERSION}),
            write=True, cms="Ghost")
        article, _ = self._first_post(self._expect_ok(response, write=True), write=True)
        return article

    def update_draft(self, remote_id: str, draft: ArticleDraft,
                     expected_revision: Optional[str] = None) -> RemoteArticle:
        require_draft_fields(draft)
        validate_html(draft.html)
        current, real_updated_at = self._read_internal(remote_id)
        check_revision(expected_revision, current.revision)
        body = {"posts": [self._payload(draft, updated_at=real_updated_at, status=None)]}
        response = wrap_transport_errors(
            lambda: self.transport(
                "PUT", self._write_path(remote_id), payload=body,
                headers={"Accept": "application/json", "Accept-Version": ADMIN_API_VERSION}),
            write=True, cms="Ghost")
        article, _ = self._first_post(self._expect_ok(response, write=True), write=True)
        return article

    # ---- Helpers ---------------------------------------------------------
    def _read_internal(self, remote_id: str) -> Tuple[RemoteArticle, str]:
        """Relit l'article pour (hash courant, `updated_at` reel a renvoyer au PUT)."""
        path = ("/ghost/api/admin/posts/"
                + quote(self._check_id(remote_id), safe="") + "/?formats=html")
        response = wrap_transport_errors(
            lambda: self.transport(
                "GET", path,
                headers={"Accept": "application/json", "Accept-Version": ADMIN_API_VERSION}),
            write=False, cms="Ghost")
        return self._first_post(self._expect_ok(response, write=False))

    def _write_path(self, remote_id: str) -> str:
        return ("/ghost/api/admin/posts/"
                + quote(self._check_id(remote_id), safe="")
                + "/?source=html&formats=html")

    @staticmethod
    def _payload(draft: ArticleDraft, *, updated_at: Optional[str], status: Optional[str]) -> dict:
        payload = {
            "title": draft.title,
            "slug": draft.slug,
            "html": draft.html,
            "custom_excerpt": (draft.excerpt or "")[:300] or None,
            "send_email_when_published": False,
        }
        if draft.author:
            # L'auteur Ghost est l'email ou l'id du membre du staff ; sans mapping cote
            # Codex, on refuse d'inventer un identifiant.
            if "@" in draft.author:
                payload["authors"] = [{"email": draft.author}]
            else:
                payload["authors"] = [{"id": draft.author}]
        if status is not None:
            payload["status"] = status
        if updated_at is not None:
            payload["updated_at"] = updated_at
        return payload

    @staticmethod
    def _check_id(remote_id: Any) -> str:
        value = str(remote_id or "").strip()
        if not value or len(value) > 64 or not all(ch.isalnum() or ch in {"-", "_"} for ch in value):
            raise AdapterError("Identifiant Ghost attendu : alphanumerique.", code="validation")
        return value

    @staticmethod
    def _expect_ok(response: TransportResponse, *, write: bool) -> dict:
        status = response.status
        if status in (401, 403):
            raise AdapterError("Ghost refuse l'acces. Verifiez la cle Admin et ses droits.",
                               code="auth")
        if status == 404:
            raise AdapterError("Ghost : article introuvable.", code="not_found")
        if status == 422:
            raise AdapterError(
                "Ghost a refuse l'operation (updated_at manquant ou obsolete).",
                code="conflict")
        if status == 429:
            raise AdapterError("Ghost : trop de requetes.", code="limit")
        if status == 400:
            raise AdapterError("Ghost a refuse la valeur ; rien n'a ete enregistre.",
                               code="refused")
        if 300 <= status < 400:
            raise AdapterError("Ghost redirige la requete ; verifiez l'URL de base.",
                               code="refused", uncertain=write)
        if status >= 500:
            raise AdapterError("Ghost : erreur serveur.", code="server", uncertain=write)
        if status < 200 or status >= 300:
            raise AdapterError("Reponse Ghost inattendue (%s)." % status,
                               code="unreadable", uncertain=write)
        try:
            data = response.json()
        except AdapterError as error:
            # Reponse 2xx mais corps illisible : apres une ecriture, le POST/PUT a pu
            # aboutir cote Ghost. On remonte uncertain=True pour que l'appelant verifie
            # avant tout nouvel envoi et ne declenche jamais une creation aveugle.
            if write:
                raise AdapterError(
                    "Reponse Ghost illisible apres une ecriture ; verifier l'etat du post.",
                    code="unreadable", uncertain=True) from error
            raise
        if not isinstance(data, dict):
            raise AdapterError("Reponse Ghost non lisible.", code="unreadable",
                               uncertain=write)
        return data

    @staticmethod
    def _first_post(data: dict, *, write: bool = False) -> Tuple[RemoteArticle, str]:
        posts = data.get("posts") if isinstance(data, dict) else None
        if not isinstance(posts, list) or len(posts) != 1 or not isinstance(posts[0], dict):
            raise AdapterError("Ghost n'a pas renvoye un unique article.", code="unreadable", uncertain=write)
        post = posts[0]
        remote_id = str(post.get("id") or "").strip()
        if not remote_id:
            raise AdapterError("Ghost n'a pas renvoye d'identifiant.", code="unreadable", uncertain=write)
        raw_status = str(post.get("status") or "unknown")
        if raw_status in {"published", "sent"}:
            status = "published"
        elif raw_status == "scheduled":
            status = "scheduled"
        elif raw_status == "draft":
            status = "draft"
        else:
            status = "unknown"
        url = str(post.get("url") or "")
        real_updated_at = str(post.get("updated_at") or "")
        if not real_updated_at:
            raise AdapterError("Ghost n'a pas renvoye updated_at.", code="unreadable", uncertain=write)
        revision = _compute_revision(post)
        return (RemoteArticle(id=remote_id, status=status, url=url, revision=revision),
                real_updated_at)
