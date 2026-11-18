"""Contrat partage des adaptateurs editoriaux : types, erreurs, fabrique, aides.

Un adaptateur expose quatre operations : `create_draft`, `read`, `publish`, `update_draft`.
La publication est TOUJOURS declenchee par un appel explicite ; une creation ne publie jamais.
Un `expected_revision` est obligatoire pour `update_draft` et `publish` : l'adaptateur relit
la version distante juste avant d'ecrire et refuse quand elle ne correspond plus.

Le transport est injecte par l'appelant (Codex branche le vrai transport et les secrets).
Sa signature : `transport(method, path, payload=None, headers=None) -> TransportResponse`.
Le corps de la reponse reste brut (bytes) : l'adaptateur decide du contenu attendu, verifie
le code HTTP et detecte les erreurs applicatives (userErrors GraphQL, publication partielle
Webflow). Aucune cle d'idempotence n'est simulee ici : la journalisation centrale de Codex
tient ce role. Un POST interrompu N'est PAS retente par l'adaptateur.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Tuple


# Taille maximale acceptee pour une reponse HTTP d'un CMS (bornage defensif).
MAX_BODY = 4 * 1024 * 1024


class AdapterError(Exception):
    """Erreur structuree pour l'appelant : message humain + `code` stable + `uncertain`.

    `uncertain=True` signale une ecriture dont le resultat n'a pas pu etre confirme
    (timeout apres envoi, reponse illisible apres POST). L'appelant doit alors verifier
    cote CMS avant tout nouvel envoi.
    """

    def __init__(self, message: str, *, code: str = "", uncertain: bool = False,
                 details: Optional[dict] = None):
        super().__init__(message)
        self.code = code
        self.uncertain = uncertain
        self.details = dict(details) if details else {}


@dataclass(frozen=True)
class ArticleImage:
    url: str
    alt: str = ""
    caption: str = ""


@dataclass(frozen=True)
class ArticleDraft:
    """Un brouillon d'article prepare pour ecriture. Les images sont des URLs deja hebergees ;
    aucun adaptateur n'uploade de media dans ce lot."""
    title: str
    html: str
    slug: str
    excerpt: str = ""
    language: str = "fr"
    author: str = ""
    images: Tuple[ArticleImage, ...] = ()


@dataclass(frozen=True)
class RemoteArticle:
    """Resultat normalise d'une operation ecriture/lecture.

    `status` = l'un de : 'draft', 'published', 'scheduled', 'unknown'.
    `revision` = jeton opaque utilise pour `expected_revision` ; chaque adaptateur documente
    son contenu (date de modification, numero de revision, etag...).
    """
    id: str
    status: str
    url: str
    revision: str


@dataclass(frozen=True)
class Capabilities:
    """Ce que cet adaptateur sait reellement faire. Une capacite non implementee est False."""
    create_draft: bool
    read: bool
    publish: bool
    update_draft: bool
    upload_images: bool
    requires: Tuple[str, ...]      # cles de configuration exigees
    notes: str = ""                 # limites, prerequis, droits attendus


@dataclass(frozen=True)
class TransportResponse:
    """Reponse brute d'un appel : l'adaptateur interprete le code et parse le corps."""
    status: int
    headers: Mapping[str, str]
    body: bytes

    def json(self) -> Any:
        if len(self.body) > MAX_BODY:
            raise AdapterError("Reponse CMS trop volumineuse.", code="unreadable")
        try:
            return json.loads(self.body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as error:
            raise AdapterError("Reponse CMS non lisible en JSON.", code="unreadable") from error

    def text(self) -> str:
        if len(self.body) > MAX_BODY:
            raise AdapterError("Reponse CMS trop volumineuse.", code="unreadable")
        try:
            return self.body.decode("utf-8")
        except UnicodeDecodeError as error:
            raise AdapterError("Reponse CMS non lisible en UTF-8.", code="unreadable") from error


TransportFn = Callable[..., TransportResponse]


# ---- Fabrique et registre -------------------------------------------------

#: Noms canoniques des plateformes reconnues par ce paquet. L'ordre n'a pas de sens.
PLATFORMS: Tuple[str, ...] = ("wordpress", "ghost", "shopify", "webflow", "wix", "prestashop")


def get_adapter(platform: str, *, transport: TransportFn, config: Mapping[str, Any]):
    """Instancie l'adaptateur de `platform`. La configuration est validee a l'instanciation,
    sans aucun appel reseau. Toute cle requise manquante leve AdapterError(code='mapping')."""
    name = (platform or "").strip().lower()
    if name not in PLATFORMS:
        raise AdapterError("Plateforme inconnue : %s." % platform, code="unsupported")
    # Imports locaux : le chargement de ce module ne declenche pas les adaptateurs.
    if name == "wordpress":
        from .wordpress import WordPressAdapter
        return WordPressAdapter(transport=transport, config=config)
    if name == "ghost":
        from .ghost import GhostAdapter
        return GhostAdapter(transport=transport, config=config)
    if name == "shopify":
        from .shopify import ShopifyAdapter
        return ShopifyAdapter(transport=transport, config=config)
    if name == "webflow":
        from .webflow import WebflowAdapter
        return WebflowAdapter(transport=transport, config=config)
    if name == "wix":
        from .wix import WixAdapter
        return WixAdapter(transport=transport, config=config)
    if name == "prestashop":
        from .prestashop import PrestaShopAdapter
        return PrestaShopAdapter(transport=transport, config=config)
    raise AdapterError("Plateforme inconnue : %s." % platform, code="unsupported")


# ---- Aides communes ------------------------------------------------------

def require_config(config: Mapping[str, Any], required: Tuple[str, ...]) -> None:
    """Refuse tout mapping incomplet des que l'adaptateur est instancie."""
    missing = [name for name in required if not str((config or {}).get(name) or "").strip()]
    if missing:
        raise AdapterError(
            "Configuration incomplete : champ requis manquant (%s)." % ", ".join(missing),
            code="mapping", details={"missing": missing})


def require_author(draft: ArticleDraft, platform: str) -> None:
    """Refuse une ecriture sans auteur quand la plateforme l'exige explicitement."""
    if not (draft.author or "").strip():
        raise AdapterError(
            "L'auteur est obligatoire pour ecrire un article %s." % platform,
            code="author")


def require_draft_fields(draft: ArticleDraft) -> None:
    if not isinstance(draft, ArticleDraft):
        raise AdapterError("Un ArticleDraft est attendu.", code="validation")
    if not (draft.title or "").strip():
        raise AdapterError("Le titre de l'article est obligatoire.", code="validation")
    if not (draft.slug or "").strip():
        raise AdapterError("Le slug de l'article est obligatoire.", code="validation")
    if not isinstance(draft.html, str):
        raise AdapterError("Le contenu HTML doit etre une chaine.", code="validation")
    # Bornes defensives : eviter d'envoyer accidentellement un document gigantesque.
    if len(draft.title) > 400 or len(draft.slug) > 200 or len(draft.excerpt) > 1000:
        raise AdapterError("Un champ depasse les limites editoriales attendues.", code="validation")
    if len(draft.html) > 1_000_000:
        raise AdapterError("Le contenu HTML depasse la taille editoriale attendue.", code="validation")


def check_revision(expected: Optional[str], current: str) -> None:
    """Refuse d'ecrire si le jeton de version distant a change depuis l'aperçu."""
    if expected is None or expected == "":
        raise AdapterError(
            "Lire la version distante avant toute mise a jour ou publication.",
            code="revision_required")
    if str(expected) != str(current):
        raise AdapterError(
            "L'article a change dans le CMS depuis l'apercu. L'operation est arretee.",
            code="conflict", details={"expected": expected, "current": current})


def wrap_transport_errors(fn: Callable[[], Any], *, write: bool, cms: str) -> Any:
    """Invoque `fn` en traduisant toute exception reseau en AdapterError clair.

    Pour une ecriture, `uncertain=True` : le POST a pu aboutir avant la coupure.
    Un POST interrompu N'est PAS retente ; l'appelant doit verifier cote CMS.

    Si le transport leve une exception portant deja les attributs `uncertain` et `code`
    (typiquement `cms_commun.CmsError`), on preserve ces valeurs : l'incertitude du
    transport ne doit pas etre perdue en remontant dans l'adaptateur.
    """
    try:
        return fn()
    except AdapterError:
        raise
    except Exception as error:
        has_uncertain = hasattr(error, "uncertain")
        has_code = hasattr(error, "code")
        if has_uncertain or has_code:
            inner_uncertain = bool(getattr(error, "uncertain", False))
            inner_code = str(getattr(error, "code", "") or "")
            raise AdapterError(
                str(error) or ("La reponse %s n'a pas pu etre confirmee." % cms),
                code=inner_code or "network",
                uncertain=inner_uncertain if has_uncertain else write,
            ) from error
        raise AdapterError(
            "La reponse %s n'a pas pu etre confirmee." % cms,
            code="network", uncertain=write) from error
