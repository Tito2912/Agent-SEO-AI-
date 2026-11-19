"""Adaptateur WordPress REST natif (pas besoin de Yoast pour un article).

Endpoints utilises (API officielle du 17/11/2026) :
- POST /wp-json/wp/v2/posts                      cree un post (status='draft' par defaut)
- GET  /wp-json/wp/v2/posts/{id}?context=edit    relit un post (y compris les brouillons)
- POST /wp-json/wp/v2/posts/{id}                 met a jour un post

Revision : WordPress ne fournit pas d'ETag native sur /posts. On prend `modified_gmt` comme
jeton de version et on refuse l'ecriture si le jeton local ne correspond pas au relu avant
l'appel (optimistic concurrency legere). WordPress n'accepte pas `If-Match` sur /posts.

L'authentification (application password) et l'URL de base du site sont injectees par le
transport ; aucun secret n'est manipule ici. Le transport est responsable d'ajouter
`Authorization` et de refuser les redirections.
"""
from __future__ import annotations

from typing import Any, Mapping, Optional

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


REQUIRED_CONFIG = ()  # l'URL de base et les droits sont a la charge du transport


class WordPressAdapter:
    platform = "wordpress"
    capabilities = Capabilities(
        create_draft=True, read=True, publish=True, update_draft=True,
        upload_images=False,
        requires=REQUIRED_CONFIG,
        notes=("REST natif WordPress 6.x. Auth = mot de passe d'application sur HTTPS. "
               "L'upload de media (POST /media) n'est pas inclus dans ce lot : l'article "
               "reference des images deja hebergees."),
    )

    def __init__(self, *, transport: TransportFn, config: Mapping[str, Any]):
        require_config(config, REQUIRED_CONFIG)
        self.transport = transport
        self.config = dict(config or {})

    # ---- Operations publiques --------------------------------------------
    def create_draft(self, draft: ArticleDraft) -> RemoteArticle:
        require_draft_fields(draft)
        validate_html(draft.html)
        payload = self._payload(draft, status="draft")
        response = wrap_transport_errors(
            lambda: self.transport("POST", "/wp-json/wp/v2/posts", payload=payload,
                                   headers={"Accept": "application/json"}),
            write=True, cms="WordPress")
        data = self._expect_ok(response, write=True)
        return self._to_remote(data)

    def read(self, remote_id: str) -> RemoteArticle:
        path = "/wp-json/wp/v2/posts/" + self._quote_id(remote_id) + "?context=edit"
        response = wrap_transport_errors(
            lambda: self.transport("GET", path, headers={"Accept": "application/json"}),
            write=False, cms="WordPress")
        data = self._expect_ok(response, write=False)
        return self._to_remote(data)

    def publish(self, remote_id: str, expected_revision: Optional[str] = None) -> RemoteArticle:
        current = self.read(remote_id)
        check_revision(expected_revision, current.revision)
        payload = {"status": "publish"}
        path = "/wp-json/wp/v2/posts/" + self._quote_id(remote_id)
        response = wrap_transport_errors(
            lambda: self.transport("POST", path, payload=payload,
                                   headers={"Accept": "application/json"}),
            write=True, cms="WordPress")
        data = self._expect_ok(response, write=True)
        return self._to_remote(data)

    def update_draft(self, remote_id: str, draft: ArticleDraft,
                     expected_revision: Optional[str] = None) -> RemoteArticle:
        require_draft_fields(draft)
        validate_html(draft.html)
        current = self.read(remote_id)
        check_revision(expected_revision, current.revision)
        # Ne jamais passer en 'publish' sur un update_draft : on garde le statut de depart.
        payload = self._payload(draft, status=None)
        path = "/wp-json/wp/v2/posts/" + self._quote_id(remote_id)
        response = wrap_transport_errors(
            lambda: self.transport("POST", path, payload=payload,
                                   headers={"Accept": "application/json"}),
            write=True, cms="WordPress")
        data = self._expect_ok(response, write=True)
        return self._to_remote(data)

    # ---- Helpers ---------------------------------------------------------
    def _payload(self, draft: ArticleDraft, *, status: Optional[str]) -> dict:
        payload = {
            "title": draft.title,
            "content": draft.html,
            "slug": draft.slug,
            "excerpt": draft.excerpt or "",
        }
        if status is not None:
            payload["status"] = status
        if draft.author:
            # WordPress attend un identifiant numerique ; Codex peut passer un id dans le
            # draft si disponible. Si c'est un nom, on refuse : pas de mapping implicite.
            try:
                payload["author"] = int(str(draft.author))
            except (TypeError, ValueError):
                raise AdapterError(
                    "L'auteur WordPress doit etre l'identifiant numerique du compte.",
                    code="author")
        return payload

    @staticmethod
    def _quote_id(remote_id: Any) -> str:
        try:
            value = int(remote_id)
        except (TypeError, ValueError):
            raise AdapterError("Identifiant WordPress attendu : nombre entier positif.",
                               code="validation")
        if value <= 0:
            raise AdapterError("Identifiant WordPress attendu : nombre entier positif.",
                               code="validation")
        return str(value)

    @staticmethod
    def _expect_ok(response: TransportResponse, *, write: bool) -> dict:
        status = response.status
        if status in (401, 403):
            raise AdapterError("WordPress refuse l'acces. Verifiez les droits du compte.",
                               code="auth")
        if status == 404:
            raise AdapterError("WordPress : article introuvable.", code="not_found")
        if status == 429:
            raise AdapterError("WordPress : trop de requetes.", code="limit")
        if status in (400, 422):
            raise AdapterError("WordPress a refuse la valeur ; rien n'a ete enregistre.",
                               code="refused")
        if 300 <= status < 400:
            raise AdapterError("WordPress redirige la requete ; verifiez l'URL de base.",
                               code="refused", uncertain=write)
        if status >= 500:
            raise AdapterError("WordPress : erreur serveur.", code="server", uncertain=write)
        if status < 200 or status >= 300:
            raise AdapterError("Reponse WordPress inattendue (%s)." % status,
                               code="unreadable", uncertain=write)
        data = response.json()
        if not isinstance(data, dict):
            raise AdapterError("Reponse WordPress non lisible.", code="unreadable",
                               uncertain=write)
        return data

    @staticmethod
    def _to_remote(data: dict) -> RemoteArticle:
        try:
            remote_id = str(int(data.get("id")))
        except (TypeError, ValueError):
            raise AdapterError("WordPress n'a pas renvoye d'identifiant exploitable.",
                               code="unreadable")
        status = str(data.get("status") or "unknown")
        if status == "publish":
            status = "published"
        elif status == "future":
            status = "scheduled"
        elif status in {"draft", "pending", "auto-draft"}:
            status = "draft"
        else:
            status = "unknown"
        link = str(data.get("link") or "")
        revision = str(data.get("modified_gmt") or data.get("modified") or "")
        if not revision:
            # Pas de jeton de version lisible : on refuse pour forcer l'echec cote appelant.
            raise AdapterError("WordPress n'a pas renvoye de date de modification.",
                               code="unreadable")
        return RemoteArticle(id=remote_id, status=status, url=link, revision=revision)
