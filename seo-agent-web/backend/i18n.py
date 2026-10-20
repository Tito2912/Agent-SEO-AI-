"""Langue de l'interface : le francais reste la langue source, l'anglais est une traduction.

LA CLE EST LE TEXTE FRANCAIS LUI-MEME. Un gabarit ecrit `{{ tr("Se connecter") }}` et se lit
toujours en francais ; `locales/en.json` associe ce texte a « Log in ». Une phrase absente du
catalogue s'affiche en francais : un ecran pas encore traduit reste utilisable, il n'affiche
jamais une cle technique.

Choix de la langue, du plus explicite au plus implicite :
  1. l'adresse d'une page publique (`/en/pricing`), ou `?lang=en` (le selecteur de langue) —
     memorisee ensuite dans un cookie ;
  2. le cookie `noyaru_lang` ;
  3. la langue d'affichage choisie dans « Mon compte » ;
  4. l'en-tete `Accept-Language` du navigateur ;
  5. le francais.
Un robot d'indexation n'envoie en general ni cookie ni `Accept-Language` : il voit le
francais a l'adresse francaise, l'anglais a l'adresse anglaise (`LOCALIZED_PAGES`).
"""
from __future__ import annotations

import json
import re
from contextvars import ContextVar
import unicodedata
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

from jinja2 import pass_context
from markupsafe import Markup, escape
from starlette.responses import JSONResponse as _JSONResponse, RedirectResponse

DEFAULT_LANG = "fr"
# L'ordre est celui du selecteur.
LANGUAGES: dict[str, str] = {"fr": "Français", "en": "English"}
COOKIE_NAME = "noyaru_lang"
_COOKIE_MAX_AGE = 365 * 24 * 3600

# Les pages publiques qui ont une adresse par langue : `/pricing` en francais, `/en/pricing` en
# anglais. Google indexe chaque version, et chacune annonce l'autre (balises hreflang). Les
# ecrans de l'application gardent une seule adresse et suivent la langue du client.
LOCALIZED_PAGES = frozenset({"/", "/pricing", "/support", "/status", "/docs"})
# Les pages de documentation (`/en/docs/<page>`) : leur route sert la traduction quand elle
# existe et renvoie vers la page francaise sinon. Chacune annonce ses versions elle-meme
# (`alternates` dans le contexte du gabarit).
LOCALIZED_PREFIXES = ("/docs/",)

# La langue de la requete en cours, pour les macros importees sans contexte (`{% from ... import
# ... %}`) : elles ne voient pas `request`, seulement les variables globales.
_current_lang_var: ContextVar[str] = ContextVar("noyaru_lang", default=DEFAULT_LANG)

_LOCALES_DIR = Path(__file__).resolve().parent.parent / "locales"
_catalogs: dict[str, dict[str, str]] = {}


def _catalog(lang: str) -> dict[str, str]:
    if lang == DEFAULT_LANG:
        return {}
    if lang not in _catalogs:
        path = _LOCALES_DIR / f"{lang}.json"
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            raw = {}
        _catalogs[lang] = {str(k): str(v) for k, v in raw.items() if isinstance(v, str) and v.strip()}
    return _catalogs[lang]


def normalize(value: Any) -> str:
    """`en-US` -> `en` ; une langue inconnue -> chaine vide."""
    code = str(value or "").strip().lower().replace("_", "-").split("-", 1)[0]
    return code if code in LANGUAGES else ""


def localized_path(path: str, lang: str) -> str:
    """L'adresse d'une page publique dans une langue : `/pricing` -> `/en/pricing`, `/` -> `/en/`.
    En francais, ou pour une page sans adresse par langue, l'adresse ne change pas."""
    if lang == DEFAULT_LANG or lang not in LANGUAGES or path not in LOCALIZED_PAGES:
        return path
    return f"/{lang}/" if path == "/" else f"/{lang}{path}"


def localized_links(builder: Any) -> Any:
    """Les liens des pages publiques (`campaign_link`) menent a la version de la langue de la
    page : `/pricing#comparatif` devient `/en/pricing#comparatif` sur une page en anglais. Une
    personne connectee garde des adresses simples : sa langue suit son compte."""

    def link(request: Any, path: str) -> str:
        parts = urlsplit(str(path))
        if not parts.scheme and not parts.netloc and getattr(request.state, "user", None) is None:
            local = localized_path(parts.path, request_lang(request))
            path = urlunsplit(("", "", local, parts.query, parts.fragment))
        return builder(request, path)

    return link


def _from_accept_language(header: str) -> str:
    """La langue preferee du navigateur parmi celles que Noyaru parle. Un navigateur regle dans
    une autre langue seulement (allemand, espagnol...) recoit l'anglais, plus utile que le
    francais a un visiteur etranger. Sans en-tete, rien : le francais s'applique."""
    best, best_q = "", 0.0
    foreign = False
    for part in str(header or "").split(","):
        piece = part.strip()
        if not piece:
            continue
        tag, _, params = piece.partition(";")
        q = 1.0
        params = params.strip()
        if params.startswith("q="):
            try:
                q = float(params[2:])
            except ValueError:
                q = 0.0
        code = normalize(tag)
        if code and q > best_q:
            best, best_q = code, q
        elif not code and q > 0 and tag.strip() != "*":
            foreign = True
    return best or ("en" if foreign else "")


def request_lang(request: Any) -> str:
    if request is None:
        return _current_lang_var.get()
    state = getattr(request, "state", None)
    cached = getattr(state, "lang", "") if state is not None else ""
    if cached:
        return cached
    try:
        lang = (
            # `/en/pricing` : l'adresse elle-meme dit la langue (voir LocalizedPaths).
            normalize(getattr(state, "url_lang", None))
            or normalize(request.query_params.get("lang"))
            or normalize(request.cookies.get(COOKIE_NAME))
            # La langue choisie dans « Mon compte » suit le client d'un appareil a l'autre.
            or normalize(getattr(getattr(state, "user", None), "language", None))
            or _from_accept_language(request.headers.get("accept-language", ""))
            or DEFAULT_LANG
        )
    except Exception:
        lang = DEFAULT_LANG
    if state is not None:
        state.lang = lang
    return lang


# Un nombre ecrit a la francaise : « 12 », « 20 000 » (espace fine ou insecable).
_NUMBER = re.compile(r"\d{1,3}(?:[\u202f\u00a0]\d{3})+|\d+")


def _localize_number(number: str, lang: str) -> str:
    if lang == "en":
        return number.replace("\u202f", ",").replace("\u00a0", ",")
    return number


def _numbered(source: str, lang: str, catalog: dict[str, str]) -> str | None:
    """Les phrases calculees par le code (« 12 sites suivis ») : le catalogue les connait sous
    la forme « {0} sites suivis ». Les nombres reprennent leur place, au format de la langue."""
    numbers = _NUMBER.findall(source)
    if not numbers:
        return None
    counter = iter(range(len(numbers)))
    key = _NUMBER.sub(lambda _m: "{%d}" % next(counter), source)
    target = catalog.get(key)
    if target is None:
        return None
    try:
        return target.format(*(_localize_number(n, lang) for n in numbers))
    except (IndexError, KeyError, ValueError):
        return None


# Phrases composees par le code avec une valeur du site (« … la marque (exemple.fr) … ») : le
# catalogue les connait avec un trou nomme, « … la marque ({domain}) … ». Seuls ces noms-la
# ouvrent un trou : un trou comme {n} ou {plan} vient d'un gabarit qui passe deja sa valeur.
# `{detail}` sert de second trou quand une phrase porte deux valeurs differentes.
_PATTERN_FIELDS = frozenset({"domain", "url", "src", "label", "candidates", "detail"})
_PLACEHOLDER = re.compile(r"\{([a-z_][a-z_0-9]*)\}")
_pattern_cache: dict[str, list[tuple[re.Pattern[str], str]]] = {}


def _patterns(lang: str, catalog: dict[str, str]) -> list[tuple[re.Pattern[str], str]]:
    if lang not in _pattern_cache:
        compiled: list[tuple[re.Pattern[str], str]] = []
        for key, target in catalog.items():
            names = _PLACEHOLDER.findall(key)
            if not names or not set(names) <= _PATTERN_FIELDS:
                continue
            parts = _PLACEHOLDER.split(key)  # texte, nom, texte, nom, ..., texte
            seen: set[str] = set()
            regex = ""
            for index, part in enumerate(parts):
                if index % 2 == 0:
                    regex += re.escape(part)
                elif part in seen:
                    regex += f"(?P={part})"
                else:
                    seen.add(part)
                    regex += f"(?P<{part}>.+?)"
            compiled.append((re.compile(regex, re.S), target))
        _pattern_cache[lang] = compiled
    return _pattern_cache[lang]


def _patterned(source: str, lang: str, catalog: dict[str, str]) -> str | None:
    for regex, target in _patterns(lang, catalog):
        match = regex.fullmatch(source)
        if match is None:
            continue
        # La valeur inseree se traduit elle aussi quand elle est connue (« aucune page identifiée »).
        values = {name: catalog.get(value, value) for name, value in match.groupdict().items()}
        try:
            return target.format(**values)
        except (IndexError, KeyError, ValueError):
            return None
    return None


def translate(text: str, lang: str = DEFAULT_LANG, **values: Any) -> str:
    source = str(text or "")
    catalog = _catalog(lang)
    result = catalog.get(source)
    if result is None and catalog:
        result = _numbered(source, lang, catalog) or (None if values else _patterned(source, lang, catalog))
    if result is None:
        result = source
    if values:
        values = {
            key: _localize_number(value, lang) if isinstance(value, str) and _NUMBER.fullmatch(value) else value
            for key, value in values.items()
        }
        try:
            result = result.format(**values)
        except (KeyError, IndexError, ValueError):
            try:
                result = source.format(**values)
            except (KeyError, IndexError, ValueError):
                result = source
    return result


class JSONResponse(_JSONResponse):
    """Une reponse JSON dont le message `error` suit la langue du client, comme les messages
    `msg` et `err` qu'affichent les gabarits : l'ecran l'affiche tel quel. En francais, ou
    hors d'une requete (une tache de fond), la reponse ne change pas."""

    def render(self, content: Any) -> bytes:
        lang = current_lang()
        if lang != DEFAULT_LANG and isinstance(content, dict) and isinstance(content.get("error"), str):
            content = {**content, "error": translate(content["error"], lang)}
        return super().render(content)


def current_lang() -> str:
    """La langue de la requete en cours, hors gabarit (un e-mail compose pendant la requete)."""
    return _current_lang_var.get()


def tr_current(text: str, **values: Any) -> str:
    return translate(text, current_lang(), **values)


@pass_context
def _tr(context: Any, text: str, **values: Any) -> str:
    return translate(text, request_lang(context.get("request")), **values)


@pass_context
def _tr_html(context: Any, text: str, **values: Any) -> Markup:
    """Pour une phrase qui contient elle-meme du HTML (un lien, un <strong>).

    Le texte du catalogue est de confiance ; les valeurs inserees sont echappees.
    """
    safe_values = {key: escape(value) for key, value in values.items()}
    # Le texte vient du gabarit ou de locales/*.json, deux fichiers du depot ; seules les valeurs
    # sont variables, et elles sont echappees juste au-dessus.
    return Markup(translate(text, request_lang(context.get("request")), **safe_values))  # nosec B704


@pass_context
def _current_lang(context: Any) -> str:
    return request_lang(context.get("request"))


# Les seuls parametres que le selecteur recopie. Les autres (identifiants de clic, adresse
# e-mail...) ne doivent jamais se retrouver dans un lien de la page.
_KEPT_PARAMS = ("next",)
_link_builder: Any = None


@pass_context
def _lang_url(context: Any, code: str) -> str:
    """L'adresse de la page courante dans une autre langue.

    Une page publique a sa propre adresse dans chaque langue (`/en/pricing`) ; son retour en
    francais garde `?lang=fr`, qui remplace un choix anglais deja memorise. Les autres pages,
    et les pages vues par une personne connectee, gardent leur adresse avec `?lang=`."""
    request = context.get("request")
    if request is None:
        return f"/?lang={code}"
    own_address = False
    try:
        params = [(k, v) for k, v in request.query_params.multi_items() if k in _KEPT_PARAMS]
        path = request.url.path
        alternates = _page_alternates(context)
        if code in alternates and getattr(request.state, "user", None) is None:
            own_address = code != DEFAULT_LANG
            path = alternates[code]
        path = f"{path}?{urlencode(params)}" if params else path
        if _link_builder is not None:
            path = _link_builder(request, path)
    except Exception:
        path = "/"  # Requete incomplete (rendu hors HTTP) : le lien reste valide.
        own_address = False
    if own_address:
        return path
    return f"{path}{'&' if '?' in path else '?'}{urlencode([('lang', code)])}"


@pass_context
def _page_alternates(context: Any) -> dict[str, str]:
    """Les adresses de la page courante dans chaque langue ({} si elle n'en a qu'une) : celles
    que la route a passees (`alternates`, une page de documentation traduite), sinon celles
    d'une page de LOCALIZED_PAGES."""
    given = context.get("alternates")
    if isinstance(given, dict):
        return given
    request = context.get("request")
    path = getattr(getattr(request, "url", None), "path", "")
    if path in LOCALIZED_PAGES:
        return {code: localized_path(path, code) for code in LANGUAGES}
    return {}


_country_cache: dict[str, list[tuple[str, str]]] = {}


@pass_context
def _country_options(context: Any) -> list[tuple[str, str]]:
    """Les pays du formulaire, nommes et tries dans la langue de la page. Les codes ne changent
    pas : `country_options` (le francais) reste la reference et le repli."""
    french = context.get("country_options") or []
    lang = request_lang(context.get("request"))
    if lang == DEFAULT_LANG:
        return list(french)
    if lang not in _country_cache:
        try:
            names = json.loads((_LOCALES_DIR / f"countries_{lang}.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            names = {}
        options = [(code, str(names.get(code) or label)) for code, label in french]
        _country_cache[lang] = sorted(options, key=lambda item: unicodedata.normalize("NFKD", item[1]).casefold())
    return _country_cache[lang]


# Les fichiers static/*.js appellent `noyaruT("texte francais")`. Seuls ces textes-la sont
# envoyes a la page (voir `_lang_script.html`), pas tout le catalogue.
_STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
_JS_CALL = re.compile(r"noyaruT\(\s*(\"(?:[^\"\\]|\\.)*\"|'(?:[^'\\]|\\.)*')")
_js_keys_cache: list[str] | None = None
_js_catalog_cache: dict[str, dict[str, str]] = {}


def _js_keys() -> list[str]:
    global _js_keys_cache
    if _js_keys_cache is None:
        keys: list[str] = []
        for path in sorted(_STATIC_DIR.glob("*.js")):
            try:
                source = path.read_text(encoding="utf-8")
            except OSError:
                continue
            for literal in _JS_CALL.findall(source):
                try:
                    key = json.loads(literal) if literal.startswith('"') else json.loads('"' + literal[1:-1].replace("\\'", "'").replace('"', '\\"') + '"')
                except ValueError:
                    continue
                if key not in keys:
                    keys.append(key)
        _js_keys_cache = keys
    return _js_keys_cache


@pass_context
def _js_catalog(context: Any) -> dict[str, str]:
    lang = request_lang(context.get("request"))
    if lang == DEFAULT_LANG:
        return {}
    if lang not in _js_catalog_cache:
        catalog = _catalog(lang)
        _js_catalog_cache[lang] = {key: catalog[key] for key in _js_keys() if key in catalog}
    return _js_catalog_cache[lang]


def remember(response: Any, lang: Any, *, secure: bool = False) -> None:
    """Retenir la langue choisie dans « Mon compte » pour ce navigateur, ou l'oublier si le
    client a choisi une langue que l'interface ne parle pas encore."""
    code = normalize(lang)
    if code:
        response.set_cookie(COOKIE_NAME, code, max_age=_COOKIE_MAX_AGE, httponly=True, samesite="lax", secure=secure)
    else:
        response.delete_cookie(COOKIE_NAME)


def add_template_globals(env: Any) -> None:
    """Les fonctions de traduction d'un environnement Jinja (aussi utile a un test qui rend un
    morceau de gabarit dans son propre environnement)."""
    env.globals["tr"] = _tr
    env.globals["tr_html"] = _tr_html
    env.globals["current_lang"] = _current_lang
    env.globals["lang_url"] = _lang_url
    env.globals["localized_path"] = localized_path
    env.globals["localized_pages"] = LOCALIZED_PAGES
    env.globals["page_alternates"] = _page_alternates
    env.globals["languages"] = LANGUAGES
    env.globals["localized_country_options"] = _country_options
    env.globals["js_catalog"] = _js_catalog


def install(app: Any, templates: Any, *, is_secure: Any = None, link_builder: Any = None) -> None:
    """`link_builder(request, path)` ajoute aux liens les parametres publics autorises (les
    etiquettes de campagne, voir signup_attribution.carry)."""
    global _link_builder
    _link_builder = link_builder
    add_template_globals(templates.env)

    @app.middleware("http")
    async def remember_lang(request, call_next):  # type: ignore[no-untyped-def]
        chosen = normalize(getattr(request.state, "url_lang", None)) or normalize(request.query_params.get("lang"))
        token = _current_lang_var.set(request_lang(request))
        try:
            response = await call_next(request)
        finally:
            _current_lang_var.reset(token)
        # La meme adresse sert plusieurs langues : un cache intermediaire doit le savoir.
        if str(response.headers.get("content-type", "")).startswith("text/html"):
            vary = [v.strip() for v in str(response.headers.get("vary", "")).split(",") if v.strip()]
            for name in ("Accept-Language", "Cookie"):
                if name.lower() not in {v.lower() for v in vary}:
                    vary.append(name)
            response.headers["Vary"] = ", ".join(vary)
        if chosen and chosen != normalize(request.cookies.get(COOKIE_NAME)):
            response.set_cookie(
                COOKIE_NAME,
                chosen,
                max_age=_COOKIE_MAX_AGE,
                httponly=True,
                samesite="lax",
                secure=bool(is_secure(request)) if is_secure else request.url.scheme == "https",
            )
        return response


class LocalizedPaths:
    """Sert `/en/pricing` par la route de `/pricing`, l'anglais fixe par l'adresse.

    A declarer en DERNIER dans app.py, donc a executer en PREMIER : les controles d'acces et
    les routes ne voient que l'adresse francaise, qu'ils connaissent deja. `/en` renvoie vers
    `/en/` (l'accueil). Une adresse `/en/...` qui n'est pas dans LOCALIZED_PAGES n'est pas
    touchee : elle n'existe pas.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            path = str(scope.get("path") or "")
            for lang in LANGUAGES:
                prefix = f"/{lang}"
                if lang == DEFAULT_LANG or not (path == prefix or path.startswith(prefix + "/")):
                    continue
                if path == prefix:
                    query = scope.get("query_string", b"").decode("latin-1")
                    response = RedirectResponse(prefix + "/" + (f"?{query}" if query else ""), status_code=308)
                    await response(scope, receive, send)
                    return
                page = path[len(prefix):]
                if page in LOCALIZED_PAGES or page.startswith(LOCALIZED_PREFIXES):
                    state = {**(scope.get("state") or {}), "url_lang": lang}
                    scope = {**scope, "path": page, "raw_path": page.encode(), "state": state}
                break
        await self.app(scope, receive, send)
