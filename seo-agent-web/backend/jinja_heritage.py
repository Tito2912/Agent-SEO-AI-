"""Le document qu'un gabarit Jinja rend, quand le rendre ne demande AUCUNE evaluation.

Une page Flask, FastAPI ou Django n'est pas un document : c'est `{% extends "base.html" %}` et
quelques blocs surcharges. Les plans proteges du correcteur (carte Twitter, viewport) prouvent
une source en la lisant comme le HTML servi, et refusent tout `{{` ou `{%` — a raison : une
valeur calculee ne prouve rien. Composer la chaine d'heritage leur rend ce document, tant que la
composition reste litterale LA OU ILS LISENT, c'est-a-dire dans l'en-tete :

- une expression n'y est toleree que DANS une valeur d'attribut entre guillemets
  (`href="{{ url_for(...) }}"`) : elle y devient une valeur neutre, et si elle portait une
  valeur qu'un plan compare au crawl, la comparaison echoue et le plan refuse de lui-meme ;
- partout ailleurs dans l'en-tete — texte, balise de controle, `super()`, controle des espaces —
  la composition refuse, et le plan refuse comme il l'a toujours fait ;
- dans le corps, que ces plans ne lisent pas, une expression devient du texte neutre.

L'ecriture revient a la PAGE, jamais a la mise en page commune : `add_to_head` pose les balises a
la fin du dernier bloc d'en-tete que la page definit, recompose, et exige que le document obtenu
soit l'ancien plus exactement ces balises.
"""

import re

_EXTENDS_RE = re.compile(r'\{%\s*extends\s+(["\'])([^"\'{}%\s]+)\1\s*%\}')
_BLOCK_RE = re.compile(r'\{%\s*block\s+(\w+)\s*%\}')
_ENDBLOCK_RE = re.compile(r'\{%\s*endblock(?:\s+(\w+))?\s*%\}')
_TAG_RE = re.compile(r'\{%.*?%\}', re.S)
_COMMENT_RE = re.compile(r'\{#.*?#\}', re.S)
# Hors des blocs d'une page qui etend, Jinja n'ecrit rien ; seules ces declarations y ont un sens.
_OUTSIDE_TAG_RE = re.compile(r'\{%\s*(?:import|from|set|load)\b[^%]*%\}')
_CONSTRUCT_RE = re.compile(r'\{\{.*?\}\}|\{%.*?%\}', re.S)
# Une construction qui ne produit qu'une VALEUR, admise dans un attribut entre guillemets.
_VALUE_CONSTRUCT_RE = re.compile(r'\{\{.*\}\}|\{%\s*(?:static|url)\b.*%\}', re.S)
_HEAD_OPEN_RE = re.compile(r'<head\b[^>]*>', re.I)
_HEAD_END_RE = re.compile(r'</head\s*>', re.I)
_HTML_OPEN_RE = re.compile(r'<html\b[^>]*>', re.I)
_LANG_VALUE_RE = re.compile(r'''(\blang\s*=\s*)(["'])([^"'{}%<>]*)\2''', re.I)
_LANG_CODE_RE = re.compile(r'[a-z]{2,3}(?:-[a-z0-9]{2,8})*', re.I)
_META_TAG_RE = re.compile(r'<meta\b[^<>]*>', re.I)
_NEUTRAL = 'jinja-expression'
_MAX_DEPTH = 4


def template_root(path):
    """`app/templates/` pour `app/templates/gauntlet/x.html` : la racine des noms d'`extends`."""
    i = path.rfind('templates/')
    if i == -1 or (i > 0 and path[i - 1] != '/'):
        return None
    return path[:i + len('templates/')]


def _masked(raw):
    """Le texte, commentaires `{# #}` remplaces par des espaces : les positions restent justes."""
    return _COMMENT_RE.sub(lambda m: ' ' * len(m.group(0)), raw)


def _parse(raw):
    """(gabarit etendu ou '', [(nom de bloc | None, texte, position)]), ou None.

    Les autres balises restent dans le texte, ou la composition les jugera selon l'endroit ou
    elles tombent. Un bloc imbrique, ou un bloc a controle d'espaces (`{%- block %}`), n'a pas
    besoin de refus a part : le `endblock` en trop, ou le bloc jamais ferme, le refuse."""
    if not isinstance(raw, str):
        return None
    masked = _masked(raw)
    segments, pos, opened, parent = [], 0, None, None
    for match in _TAG_RE.finditer(masked):
        tag = match.group(0)
        block, end, extends = _BLOCK_RE.fullmatch(tag), _ENDBLOCK_RE.fullmatch(tag), _EXTENDS_RE.fullmatch(tag)
        if block:
            segments.append((None, raw[pos:match.start()], pos))
            opened, pos = block.group(1), match.end()
        elif end:
            if opened is None or end.group(1) not in (None, opened):
                return None
            segments.append((opened, raw[pos:match.start()], pos))
            opened, pos = None, match.end()
        elif extends or re.match(r'\{%\s*(?:extends|endblock|block)\b', tag):
            if not extends or opened is not None or parent is not None:
                return None
            parent = extends.group(2)
            segments.append((None, raw[pos:match.start()], pos))
            pos = match.end()
    if opened is not None:
        return None
    segments.append((None, raw[pos:], pos))
    names = [name for name, _, _ in segments if name]
    if len(names) != len(set(names)):
        return None
    return parent or '', segments


def _chain(path, read):
    """[(chemin, segments)] de la page a sa mise en page racine, ou None."""
    root = template_root(path)
    chain, seen = [], set()
    while root and path not in seen and len(chain) <= _MAX_DEPTH:
        seen.add(path)
        parsed = _parse(read(path))
        if parsed is None:
            return None
        parent, segments = parsed
        if parent and any(name is None and _OUTSIDE_TAG_RE.sub('', _COMMENT_RE.sub('', text)).strip()
                          for name, text, _ in segments):
            # Du texte hors bloc dans une page qui etend n'est jamais rendu : le voir signale un
            # gabarit qu'on comprend mal.
            return None
        chain.append((path, segments))
        if not parent:
            return chain if len(chain) > 1 else None
        path = root + parent
    return None


def compose(path, read):
    """(document, blocs) ou None. `read(chemin)` rend un fichier du depot ou None.

    `blocs[i] = (chemin qui definit le bloc retenu, nom, debut, fin)` dans le document, dont les
    commentaires `{# #}` ont disparu comme au rendu."""
    chain = _chain(path, read)
    if not chain:
        return None
    chosen = {}
    for source, segments in chain:
        for name, text, _ in segments:
            if name is not None and name not in chosen:
                chosen[name] = (source, text)
    out, blocks, offset = [], [], 0
    for name, text, _ in chain[-1][1]:
        source = None
        if name is not None:
            source, text = chosen[name]
        cleaned = _COMMENT_RE.sub('', text)
        if name is not None:
            blocks.append((source, name, offset, offset + len(cleaned)))
        out.append(cleaned)
        offset += len(cleaned)
    return ''.join(out), blocks


def _in_quoted_value(before):
    opened, closed = before.rfind('<'), before.rfind('>')
    if opened <= closed:
        return False
    quote = None
    for char in before[opened:]:
        if quote:
            if char == quote:
                quote = None
        elif char in '"\'':
            quote = char
    return quote is not None


def literal_view(path, read):
    """Le document rendu de la page tel qu'un plan litteral le lit, ou None."""
    composed = compose(path, read)
    if not composed:
        return None
    document = composed[0]
    head_end = _HEAD_END_RE.search(document)
    if not head_end:
        return None
    out, pos = [], 0
    for match in _CONSTRUCT_RE.finditer(document):
        out.append(document[pos:match.start()])
        pos = match.end()
        if match.start() >= head_end.end():
            out.append(_NEUTRAL if match.group(0).startswith('{{') else '')
        elif _VALUE_CONSTRUCT_RE.fullmatch(match.group(0)) and _in_quoted_value(''.join(out)):
            out.append(_NEUTRAL)
        else:
            return None
    out.append(document[pos:])
    return ''.join(out)


def _own_head_block(path, read):
    """Le dernier bloc QUE LA PAGE DEFINIT et que l'en-tete rend, ou None."""
    composed = compose(path, read)
    if not composed:
        return None
    document, blocks = composed
    head_open, head_end = _HEAD_OPEN_RE.search(document), _HEAD_END_RE.search(document)
    if not head_open or not head_end:
        return None
    own = [name for source, name, start, end in blocks
           if source == path and start >= head_open.end() and end <= head_end.start()]
    return own[-1] if own else None


def _squash(text):
    return re.sub(r'\s+', '', text)


def add_to_head(raw, path, read, old_view, new_view):
    """La page `raw` portant les balises `<meta>` que `new_view` ajoute a `old_view`, ou None.

    `old_view` est le document rendu, `new_view` ce qu'un plan litteral en a fait. Le plan ne
    doit avoir fait qu'AJOUTER des balises : une modification ne se transpose pas. Elles vont a la
    fin du dernier bloc d'en-tete que la page definit, a son indentation ; la composition etant
    une concatenation, la page recomposee est alors l'ancien document plus exactement elles. A
    l'appelant de relire ce rendu avec son plan, qui ne doit plus rien vouloir ajouter."""
    added = _META_TAG_RE.findall(new_view)
    for tag in _META_TAG_RE.findall(old_view):
        if tag not in added:
            return None
        added.remove(tag)
    if not added or any(re.search(r'\{[{%#]|[}%#]\}', tag) for tag in added):
        return None
    stripped = new_view
    for tag in added:
        stripped = stripped.replace(tag, '', 1)
    if _squash(stripped) != _squash(old_view):
        return None
    name = _own_head_block(path, read)
    parsed = _parse(raw) if name else None
    block = next(((text, start) for found, text, start in parsed[1] if found == name), None) if parsed else None
    if not block:
        return None
    text, start = block
    body = text.rstrip()
    lines = [line for line in _COMMENT_RE.sub('', body).splitlines() if line.strip()]
    indent = re.match(r'[ \t]*', lines[-1]).group(0) if lines else '    '
    newline = '\r\n' if '\r\n' in raw else '\n'
    addition = ''.join(newline + indent + tag for tag in added)
    return raw[:start + len(body)] + addition + raw[start + len(body):]


def rendered_lang(path, read):
    """La valeur `lang` du `<html>` que la page rend, en minuscules, ou None si elle se calcule."""
    composed = compose(path, read)
    tag = _HTML_OPEN_RE.search(composed[0]) if composed else None
    if not tag or '{' in tag.group(0):
        return None
    found = _LANG_VALUE_RE.findall(tag.group(0))
    return found[0][2].strip().lower() if len(found) == 1 else None


def served_lang(raw, path, read, lang):
    """La page qui fait rendre `<html lang="lang">`, ou None.

    Seul cas sans evaluation : la mise en page racine ecrit la langue DANS un bloc —
    `lang="{% block lang %}fr{% endblock %}"` (le bloc est la valeur) ou
    `<html{% block html_attrs %} lang="fr"{% endblock %}>` (le bloc porte l'attribut). La page
    surcharge alors ce bloc, ou le declare juste apres son `extends`. Une langue fixe dans la mise
    en page commune n'a pas de prise par page : refus, plutot que la changer pour tout le site."""
    if not isinstance(lang, str) or not _LANG_CODE_RE.fullmatch(lang):
        return None
    reader = lambda p: raw if p == path else read(p)  # noqa: E731
    chain = _chain(path, reader)
    if not chain:
        return None
    layout = chain[-1][1]
    shape = ''.join(text if name is None else '\x00' + name + '\x01' for name, text, _ in layout)
    tag = _HTML_OPEN_RE.search(shape)
    names = re.findall(r'\x00(\w+)\x01', tag.group(0)) if tag else []
    if len(names) != 1:
        return None
    name = names[0]
    shape = tag.group(0).replace('\x00' + name + '\x01', '\x02')
    default = next(text for found, text, _ in layout if found == name)
    if re.search(r'''\blang\s*=\s*(["'])\x02\1''', shape, re.I):
        if not _LANG_CODE_RE.fullmatch(default.strip()):
            return None
        rewrite = lambda current: lang  # noqa: E731
        empty = lang
    elif re.fullmatch(r'<html\s*\x02\s*>', shape, re.I):
        if _LANG_VALUE_RE.sub('', default).strip():
            return None

        def rewrite(current):
            if _LANG_VALUE_RE.search(current):
                return _LANG_VALUE_RE.sub(lambda m: m.group(1) + m.group(2) + lang + m.group(2), current, count=1)
            # Le bloc ne porte pas de langue : on l'ajoute a ses attributs, s'il n'y a qu'eux.
            if re.search(r'[<>{}]|\blang\b', current, re.I):
                return None
            return current.rstrip() + ' lang="' + lang + '"'
        empty = ' lang="' + lang + '"'
    else:
        return None
    own = next(((text, start) for found, text, start in chain[0][1] if found == name), None)
    if own is not None:
        text, start = own
        new = rewrite(text)
        if not new or new == text:
            return None
        child = raw[:start] + new + raw[start + len(text):]
    else:
        extends = _EXTENDS_RE.search(_masked(raw))
        newline = '\r\n' if '\r\n' in raw else '\n'
        child = (raw[:extends.end()] + newline + '{% block ' + name + ' %}' + empty + '{% endblock %}'
                 + raw[extends.end():])
    if rendered_lang(path, lambda p: child if p == path else read(p)) != lang.lower():
        return None
    return child
