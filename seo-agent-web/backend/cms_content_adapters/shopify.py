"""Adaptateur Shopify Admin GraphQL (version 2026-10, API officielle du 17/11/2026).

Mutations utilisees :
- articleCreate(article: ArticleCreateInput!) : brouillon par defaut (isPublished=false).
- articleUpdate(id: ID!, article: ArticleUpdateInput!) : met a jour un article.
- Query article(id: ID!) : relit un article pour recuperer `updatedAt` (revision).

Points durs :
- `blogId` est OBLIGATOIRE a la creation ; il est fourni par la configuration (pas inferable).
- L'auteur est obligatoire (`author.name`) pour `articleCreate`.
- Les mutations GraphQL peuvent renvoyer HTTP 200 avec `userErrors` non vide : on les traite
  comme des erreurs, jamais comme un succes silencieux.
- Les parametres sont toujours passes en VARIABLES de la requete GraphQL (jamais interpolees
  dans la chaine de la requete), pour eviter toute injection.
- Droits attendus : `write_content` (blog articles).
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
    require_author,
    wrap_transport_errors,
)
from .html_safe import validate_html


REQUIRED_CONFIG = ("blog_id",)
GRAPHQL_PATH = "/admin/api/2026-10/graphql.json"

_CREATE_MUTATION = """
mutation noyaruArticleCreate($article: ArticleCreateInput!) {
  articleCreate(article: $article) {
    article { id title handle isPublished publishedAt updatedAt author { name } blog { id } }
    userErrors { field message code }
  }
}
""".strip()

_UPDATE_MUTATION = """
mutation noyaruArticleUpdate($id: ID!, $article: ArticleUpdateInput!) {
  articleUpdate(id: $id, article: $article) {
    article { id title handle isPublished publishedAt updatedAt author { name } blog { id } }
    userErrors { field message code }
  }
}
""".strip()

_READ_QUERY = """
query noyaruArticleRead($id: ID!) {
  article(id: $id) { id title handle isPublished publishedAt updatedAt author { name } blog { id } }
}
""".strip()


class ShopifyAdapter:
    platform = "shopify"
    capabilities = Capabilities(
        create_draft=True, read=True, publish=True, update_draft=True,
        upload_images=False,
        requires=REQUIRED_CONFIG,
        notes=("Shopify Admin GraphQL 2026-10. blog_id requis, auteur requis, body HTML, "
               "isPublished=false par defaut. Droits : write_content."),
    )

    def __init__(self, *, transport: TransportFn, config: Mapping[str, Any]):
        require_config(config, REQUIRED_CONFIG)
        self.transport = transport
        self.config = dict(config or {})
        self.blog_id = str(self.config["blog_id"]).strip()
        if not self.blog_id.startswith("gid://shopify/Blog/"):
            raise AdapterError(
                "blog_id Shopify attendu au format gid://shopify/Blog/<id>.",
                code="mapping")

    # ---- Operations publiques --------------------------------------------
    def validate_draft(self, draft: ArticleDraft) -> None:
        """Validate the draft without network access or a media transfer."""
        require_draft_fields(draft)
        validate_html(draft.html)
        require_author(draft, "Shopify")

    def create_draft(self, draft: ArticleDraft) -> RemoteArticle:
        self.validate_draft(draft)
        require_draft_fields(draft)
        require_author(draft, "Shopify")
        validate_html(draft.html)
        variables = {"article": {
            "blogId": self.blog_id,
            "title": draft.title,
            "handle": draft.slug,
            "body": draft.html,
            "summary": draft.excerpt or "",
            "author": {"name": draft.author},
            "isPublished": False,
        }}
        data = self._graphql(_CREATE_MUTATION, variables, field="articleCreate", write=True)
        return self._to_remote(data.get("article"))

    def read(self, remote_id: str) -> RemoteArticle:
        gid = self._gid(remote_id)
        response = self._run(_READ_QUERY, {"id": gid}, write=False)
        data = self._expect_ok(response, write=False)
        article = ((data.get("data") or {}).get("article")) if isinstance(data, dict) else None
        if not isinstance(article, dict):
            raise AdapterError("Shopify : article introuvable.", code="not_found")
        return self._to_remote(article)

    def publish(self, remote_id: str, expected_revision: Optional[str] = None) -> RemoteArticle:
        current = self.read(remote_id)
        check_revision(expected_revision, current.revision)
        variables = {"id": current.id, "article": {"isPublished": True}}
        data = self._graphql(_UPDATE_MUTATION, variables, field="articleUpdate", write=True)
        return self._to_remote(data.get("article"))

    def update_draft(self, remote_id: str, draft: ArticleDraft,
                     expected_revision: Optional[str] = None) -> RemoteArticle:
        require_draft_fields(draft)
        require_author(draft, "Shopify")
        validate_html(draft.html)
        current = self.read(remote_id)
        check_revision(expected_revision, current.revision)
        variables = {"id": current.id, "article": {
            "title": draft.title,
            "handle": draft.slug,
            "body": draft.html,
            "summary": draft.excerpt or "",
            "author": {"name": draft.author},
        }}
        data = self._graphql(_UPDATE_MUTATION, variables, field="articleUpdate", write=True)
        return self._to_remote(data.get("article"))

    # ---- Helpers ---------------------------------------------------------
    def _graphql(self, query: str, variables: dict, *, field: str, write: bool) -> dict:
        response = self._run(query, variables, write=write)
        data = self._expect_ok(response, write=write)
        payload = ((data.get("data") or {}).get(field)) if isinstance(data, dict) else None
        if not isinstance(payload, dict):
            raise AdapterError("Reponse GraphQL Shopify incomplete (%s)." % field,
                               code="unreadable", uncertain=write)
        user_errors = payload.get("userErrors") or []
        if user_errors:
            # userErrors peut arriver avec HTTP 200 : c'est une ERREUR, pas un succes.
            message = "; ".join(
                str((entry or {}).get("message") or "") for entry in user_errors if isinstance(entry, dict))
            raise AdapterError(
                "Shopify a refuse l'operation : %s" % (message or "erreur utilisateur"),
                code="user_errors", details={"userErrors": user_errors})
        return payload

    def _run(self, query: str, variables: dict, *, write: bool) -> TransportResponse:
        body = {"query": query, "variables": variables}
        return wrap_transport_errors(
            lambda: self.transport("POST", GRAPHQL_PATH, payload=body,
                                   headers={"Accept": "application/json"}),
            write=write, cms="Shopify")

    @staticmethod
    def _gid(remote_id: Any) -> str:
        value = str(remote_id or "").strip()
        if value.startswith("gid://shopify/Article/") and len(value) < 128:
            return value
        if value.isdigit() and 0 < len(value) <= 20:
            return "gid://shopify/Article/" + value
        raise AdapterError(
            "Identifiant Shopify attendu : gid://shopify/Article/<id> ou nombre.",
            code="validation")

    @staticmethod
    def _expect_ok(response: TransportResponse, *, write: bool) -> dict:
        status = response.status
        if status in (401, 403):
            raise AdapterError("Shopify refuse l'acces. Verifiez le jeton et les scopes.",
                               code="auth")
        if status == 404:
            raise AdapterError("Shopify : ressource introuvable.", code="not_found")
        if status == 429:
            raise AdapterError("Shopify : trop de requetes.", code="limit")
        if status >= 500:
            raise AdapterError("Shopify : erreur serveur.", code="server", uncertain=write)
        if status < 200 or status >= 300:
            raise AdapterError("Reponse Shopify inattendue (%s)." % status,
                               code="refused", uncertain=write)
        data = response.json()
        if not isinstance(data, dict):
            raise AdapterError("Reponse Shopify non lisible.", code="unreadable",
                               uncertain=write)
        if data.get("errors"):
            # Erreurs GraphQL top-level : syntaxe, scopes manquants, ressource invalide...
            messages = "; ".join(
                str((e or {}).get("message") or "") for e in data["errors"] if isinstance(e, dict))
            raise AdapterError(
                "Shopify GraphQL a renvoye une erreur : %s" % (messages or "inconnue"),
                code="refused", details={"errors": data["errors"]})
        return data

    @staticmethod
    def _to_remote(article: Optional[dict]) -> RemoteArticle:
        if not isinstance(article, dict) or not article.get("id"):
            raise AdapterError("Shopify n'a pas renvoye d'article exploitable.",
                               code="unreadable")
        remote_id = str(article["id"])
        is_published = bool(article.get("isPublished"))
        published_at = article.get("publishedAt")
        if is_published:
            status = "published"
        elif published_at:
            status = "scheduled"
        else:
            status = "draft"
        url = ""  # Shopify ne renvoie pas d'URL finale par defaut ; laisser le soin a Codex
        revision = str(article.get("updatedAt") or "")
        if not revision:
            raise AdapterError("Shopify n'a pas renvoye updatedAt.", code="unreadable")
        return RemoteArticle(id=remote_id, status=status, url=url, revision=revision)
