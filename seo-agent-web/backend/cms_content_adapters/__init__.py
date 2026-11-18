"""Adaptateurs editoriaux par CMS : ecrire des articles ou des pages, pas que des metadonnees.

Ce paquet ajoute une couche distincte du socle `cms_commun.Connecteur`, qui reste limite aux
titres et meta-descriptions (17/11/2026). Les adaptateurs ici publies ecrivent des BROUILLONS
par defaut, lisent la version avant toute mise a jour ou publication et refusent les capacites
qu'ils n'implementent pas. L'integration reelle, les secrets et la persistance seront branches
par la session Codex ; aucun appel reseau n'est fait au chargement de ce module.
"""
from __future__ import annotations

from .base import (
    ArticleDraft,
    ArticleImage,
    AdapterError,
    Capabilities,
    RemoteArticle,
    TransportResponse,
    get_adapter,
    PLATFORMS,
)

__all__ = [
    "ArticleDraft",
    "ArticleImage",
    "AdapterError",
    "Capabilities",
    "RemoteArticle",
    "TransportResponse",
    "get_adapter",
    "PLATFORMS",
]
