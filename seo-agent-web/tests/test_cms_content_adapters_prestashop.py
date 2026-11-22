"""Adaptateur PrestaShop Webservice (pages CMS) : empreinte deterministe, XML echappe,
multilingue preserve, relecture GET autoritative apres chaque ecriture.

PrestaShop n'expose pas date_upd sur la ressource CMS ; la revision est une empreinte
SHA-256 des champs reels retournes par le GET autoritatif. Sur boutique 8.2.8 fr/en,
la reponse POST 201 echoe `indexation` vide et les variantes langue 2 de
`meta_title`/`link_rewrite` vides alors que le GET suivant renseigne ces champs : le
GET est donc la seule source de verite retenue."""
from __future__ import annotations

import pytest
from defusedxml import ElementTree as ET

from backend.cms_content_adapters import AdapterError, ArticleDraft, TransportResponse, get_adapter
from backend.cms_content_adapters.prestashop import PrestaShopAdapter


# ---- Fabrique de fixtures XML --------------------------------------------

def _draft(**overrides):
    base = {"title": "Un titre & essai", "html": "<p>Corps simple.</p>",
            "slug": "un-titre-essai", "excerpt": "Resume & synthese.",
            "language": "fr", "author": ""}
    base.update(overrides)
    return ArticleDraft(**base)


def _xml(body: str, status: int = 200) -> TransportResponse:
    return TransportResponse(status=status, headers={"content-type": "application/xml"},
                             body=body.encode("utf-8"))


def _esc(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _content_xml(cms_id: str = "12", active: int = 0, id_cms_category: str = "2",
                 fr_title: str = "Titre FR", en_title: str = "Title EN",
                 fr_desc: str = "Desc FR", en_desc: str = "Desc EN",
                 fr_slug: str = "titre-fr", en_slug: str = "title-en",
                 fr_body: str = "<p>FR</p>", en_body: str = "<p>EN</p>",
                 indexation: str = "0", position: str = "0",
                 extras: str = "") -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<prestashop><content>'
        f'<id>{cms_id}</id>'
        f'<id_cms_category>{id_cms_category}</id_cms_category>'
        f'<position>{position}</position>'
        f'<active>{active}</active>'
        f'<indexation>{indexation}</indexation>'
        '<meta_title>'
        f'<language id="1">{_esc(fr_title)}</language>'
        f'<language id="2">{_esc(en_title)}</language>'
        '</meta_title>'
        '<meta_description>'
        f'<language id="1">{_esc(fr_desc)}</language>'
        f'<language id="2">{_esc(en_desc)}</language>'
        '</meta_description>'
        '<link_rewrite>'
        f'<language id="1">{_esc(fr_slug)}</language>'
        f'<language id="2">{_esc(en_slug)}</language>'
        '</link_rewrite>'
        '<content>'
        f'<language id="1">{_esc(fr_body)}</language>'
        f'<language id="2">{_esc(en_body)}</language>'
        '</content>'
        + extras +
        '</content></prestashop>'
    )


def _post_echo_partial(cms_id: str = "12", draft_title: str = "Un titre & essai",
                       draft_desc: str = "Resume & synthese.",
                       draft_slug: str = "un-titre-essai",
                       draft_body: str = "<p>Corps simple.</p>") -> str:
    """Corps renvoye par POST 201 d'une boutique 8.2.8 : `indexation` vide,
    langue 2 vide pour meta_title et link_rewrite. Verifie reellement par Codex."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<prestashop><content>'
        f'<id>{cms_id}</id>'
        '<id_cms_category>2</id_cms_category>'
        '<position></position>'
        '<active>0</active>'
        '<indexation></indexation>'
        '<meta_title>'
        f'<language id="1">{_esc(draft_title)}</language>'
        '<language id="2"></language>'
        '</meta_title>'
        '<meta_description>'
        f'<language id="1">{_esc(draft_desc)}</language>'
        f'<language id="2">{_esc(draft_desc)}</language>'
        '</meta_description>'
        '<link_rewrite>'
        f'<language id="1">{_esc(draft_slug)}</language>'
        '<language id="2"></language>'
        '</link_rewrite>'
        '<content>'
        f'<language id="1">{_esc(draft_body)}</language>'
        f'<language id="2">{_esc(draft_body)}</language>'
        '</content>'
        '</content></prestashop>'
    )


def _revision_of(xml_body: str) -> str:
    root = ET.fromstring(xml_body.encode("utf-8"))
    return PrestaShopAdapter._fingerprint(root.find("content"))


class _Transport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, path, payload=None, headers=None):
        self.calls.append({"method": method, "path": path, "payload": payload,
                           "headers": dict(headers or {})})
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


# ---- Creation : le GET autoritatif decide, pas la reponse POST ----------

def test_create_draft_uses_authoritative_get_not_post_echo():
    """La reponse POST echoe un ORM partiel (indexation vide, langue 2 vide pour title/slug).
    L'adaptateur doit relire via GET et renvoyer la revision du GET, pas celle du POST."""
    post_echo = _post_echo_partial()
    get_snapshot = _content_xml(fr_title="Un titre & essai", en_title="Un titre & essai",
                                fr_slug="un-titre-essai", en_slug="un-titre-essai",
                                fr_desc="Resume & synthese.", en_desc="Resume & synthese.",
                                fr_body="<p>Corps simple.</p>", en_body="<p>Corps simple.</p>",
                                indexation="0", position="0")
    transport = _Transport([_xml(post_echo, status=201), _xml(get_snapshot)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    article = adapter.create_draft(_draft())
    assert article.id == "12"
    assert article.status == "draft"
    assert article.revision == _revision_of(get_snapshot)
    assert article.revision != _revision_of(post_echo)
    # Deux appels : POST puis GET sur /12.
    assert transport.calls[0]["method"] == "POST"
    assert transport.calls[1]["method"] == "GET"
    assert "/api/content_management_system/12" in transport.calls[1]["path"]


def test_create_draft_sends_xml_with_escaped_values_and_active_zero():
    get_snapshot = _content_xml()
    transport = _Transport([_xml(_post_echo_partial(), status=201), _xml(get_snapshot)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    article = adapter.create_draft(_draft())
    assert article.revision == _revision_of(get_snapshot)
    sent = transport.calls[0]["payload"].decode("utf-8")
    assert "<active>0</active>" in sent
    assert "Un titre &amp; essai" in sent
    assert "&lt;p&gt;Corps simple.&lt;/p&gt;" in sent
    assert "<script" not in sent


def test_create_uses_post_and_active_zero():
    transport = _Transport([_xml(_post_echo_partial(), status=201), _xml(_content_xml())])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    adapter.create_draft(_draft())
    assert transport.calls[0]["method"] == "POST"
    assert "id_shop=1" in transport.calls[0]["path"]
    sent = transport.calls[0]["payload"].decode("utf-8")
    assert sent.startswith("<?xml")
    assert '<language id="1">' in sent
    assert "<id>" not in sent


def test_create_draft_post_read_auth_refusal_is_uncertain_with_remote_id():
    """Si la relecture apres POST se voit refuser (401), l'ecriture a pu aboutir :
    on remonte uncertain=True avec l'identifiant connu dans details."""
    transport = _Transport([
        _xml(_post_echo_partial(), status=201),
        _xml("<prestashop><errors/></prestashop>", status=401),
    ])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.uncertain is True
    assert excinfo.value.details.get("remote_id") == "12"


def test_create_draft_post_read_server_error_is_uncertain_with_remote_id():
    transport = _Transport([
        _xml(_post_echo_partial(), status=201),
        _xml("<prestashop></prestashop>", status=503),
    ])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.uncertain is True
    assert excinfo.value.details.get("remote_id") == "12"


def test_create_draft_without_id_in_post_response_is_uncertain():
    """Si le POST reussit mais echoe sans identifiant exploitable, l'ecriture est incertaine
    et aucune relecture ne peut etre tentee."""
    no_id = ('<?xml version="1.0"?>'
             '<prestashop><content><active>0</active></content></prestashop>')
    transport = _Transport([_xml(no_id, status=201)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.code == "unreadable"
    assert excinfo.value.uncertain is True


# ---- Validations locales -------------------------------------------------

def test_bad_remote_id_refused_before_network():
    transport = _Transport([])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    for bad in ("0", "-1", "abc", "1.2"):
        with pytest.raises(AdapterError):
            adapter.read(bad)
    assert transport.calls == []


def test_oversize_html_refused_before_network():
    transport = _Transport([])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    huge = ArticleDraft(title="t", html="x" * 1_100_000, slug="s")
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(huge)
    assert excinfo.value.code == "validation"
    assert transport.calls == []


# ---- Reponses illisibles / interdites -----------------------------------

def test_unparseable_response_is_unreadable():
    transport = _Transport([_xml("<not-xml", status=200)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("12")
    assert excinfo.value.code == "unreadable"


def test_malformed_xml_is_unreadable_not_crash():
    transport = _Transport([_xml("<prestashop><content><id>12", status=200)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("12")
    assert excinfo.value.code == "unreadable"


def test_doctype_response_is_refused_before_parsing():
    poisoned = (
        '<?xml version="1.0"?>'
        '<!DOCTYPE lolz [<!ENTITY lol "lol"><!ENTITY lol2 "&lol;&lol;">]>'
        '<prestashop><content><id>12</id><active>0</active>'
        '</content></prestashop>'
    )
    transport = _Transport([_xml(poisoned, status=200)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("12")
    assert excinfo.value.code == "unreadable"


def test_entity_declaration_without_doctype_is_refused():
    poisoned = (
        '<?xml version="1.0"?>'
        '<!ENTITY ext SYSTEM "file:///etc/passwd">'
        '<prestashop><content><id>12</id><active>0</active>'
        '</content></prestashop>'
    )
    transport = _Transport([_xml(poisoned, status=200)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("12")
    assert excinfo.value.code == "unreadable"


# ---- Capacites declarees -------------------------------------------------

def test_capabilities_declare_native_contract():
    adapter = get_adapter("prestashop", transport=lambda *a, **k: None,
                          config={"id_lang": "1", "id_shop": "1"})
    assert adapter.capabilities.upload_images is False
    notes = adapter.capabilities.notes
    assert "PAS de blog" in notes
    assert "date_upd" in notes
    assert "langue par defaut" in notes
    assert "non autoritative" in notes


# ---- Empreinte : absence de date_upd, stabilite, sensibilite -------------

def test_revision_is_derived_without_date_upd():
    body = _content_xml()
    assert "<date_upd>" not in body
    rev = _revision_of(body)
    assert isinstance(rev, str) and len(rev) == 64
    assert all(c in "0123456789abcdef" for c in rev)


def test_revision_ignores_language_and_attribute_order():
    body_order_a = (
        '<prestashop><content>'
        '<id>12</id><id_cms_category>2</id_cms_category><active>0</active>'
        '<meta_title><language id="1">A</language><language id="2">B</language></meta_title>'
        '<meta_description><language id="1">D1</language><language id="2">D2</language></meta_description>'
        '<link_rewrite><language id="1">s1</language><language id="2">s2</language></link_rewrite>'
        '<content><language id="1">c1</language><language id="2">c2</language></content>'
        '</content></prestashop>'
    )
    body_order_b = (
        '<prestashop><content>'
        '<id>12</id><id_cms_category>2</id_cms_category><active>0</active>'
        '<meta_title><language xml:lang="en" id="2">B</language>'
        '<language xml:lang="fr" id="1">A</language></meta_title>'
        '<meta_description><language id="2">D2</language><language id="1">D1</language></meta_description>'
        '<link_rewrite><language id="2">s2</language><language id="1">s1</language></link_rewrite>'
        '<content><language id="2">c2</language><language id="1">c1</language></content>'
        '</content></prestashop>'
    )
    assert _revision_of(body_order_a) == _revision_of(body_order_b)


def test_revision_preserves_whitespace_in_content():
    base = _revision_of(_content_xml(fr_body="Ligne 1\nLigne 2"))
    spaced = _revision_of(_content_xml(fr_body="Ligne 1 \n  Ligne 2"))
    assert base != spaced


def test_revision_changes_with_text_state_or_language_set():
    baseline = _revision_of(_content_xml())
    assert baseline != _revision_of(_content_xml(fr_title="Autre"))
    assert baseline != _revision_of(_content_xml(active=1))
    changed = _content_xml().replace(
        '<meta_title>'
        '<language id="1">Titre FR</language>'
        '<language id="2">Title EN</language>'
        '</meta_title>',
        '<meta_title>'
        '<language id="1">Titre FR</language>'
        '<language id="2">Title EN</language>'
        '<language id="3">Titulo ES</language>'
        '</meta_title>')
    assert baseline != _revision_of(changed)


def test_revision_distinguishes_present_empty_and_absent():
    present = _content_xml()
    empty = present.replace("<id_cms_category>2</id_cms_category>",
                            "<id_cms_category></id_cms_category>")
    missing = present.replace("<id_cms_category>2</id_cms_category>", "")
    assert _revision_of(present) != _revision_of(empty)
    assert _revision_of(empty) != _revision_of(missing)


# ---- Revision obligatoire et conflit -------------------------------------

def test_publish_without_revision_refused():
    transport = _Transport([_xml(_content_xml())])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("12")
    assert excinfo.value.code == "revision_required"


def test_conflict_stops_before_write():
    transport = _Transport([_xml(_content_xml())])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("12", _draft(title="Nouveau"),
                             expected_revision="empreinte-perimee")
    assert excinfo.value.code == "conflict"
    assert len(transport.calls) == 1
    assert transport.calls[0]["method"] == "GET"


# ---- update_draft : methode, preservation decidee sur GET ----------------

def test_update_draft_uses_patch_then_reads_authoritative_snapshot():
    pre = _content_xml(active=0)
    patch_echo = _content_xml(active=0, fr_title="Nouveau titre")
    post = _content_xml(active=0, fr_title="Nouveau titre")
    transport = _Transport([_xml(pre), _xml(patch_echo), _xml(post)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    article = adapter.update_draft("12", _draft(title="Nouveau titre"),
                                   expected_revision=_revision_of(pre))
    assert [c["method"] for c in transport.calls] == ["GET", "PATCH", "GET"]
    assert "/api/content_management_system/12" in transport.calls[2]["path"]
    patch_body = transport.calls[1]["payload"].decode("utf-8")
    assert "<active>0</active>" in patch_body
    assert article.status == "draft"
    assert article.revision == _revision_of(post)


def test_update_draft_resends_every_language_for_touched_fields():
    """Un PATCH mono-langue effacait les autres sur 8.2.8. L'adaptateur renvoie chaque
    langue connue dans chaque champ modifie."""
    pre = _content_xml(fr_title="Ancien FR", en_title="Previous EN",
                       fr_body="<p>FR ancien</p>", en_body="<p>EN previous</p>")
    patch_echo = _content_xml(fr_title="Nouveau FR", en_title="Previous EN",
                              fr_body="<p>FR nouveau</p>", en_body="<p>EN previous</p>")
    post = _content_xml(fr_title="Nouveau FR", en_title="Previous EN",
                        fr_body="<p>FR nouveau</p>", en_body="<p>EN previous</p>")
    transport = _Transport([_xml(pre), _xml(patch_echo), _xml(post)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    adapter.update_draft("12", _draft(title="Nouveau FR", html="<p>FR nouveau</p>"),
                         expected_revision=_revision_of(pre))
    patch_body = transport.calls[1]["payload"].decode("utf-8")
    assert '<language id="1">Nouveau FR</language>' in patch_body
    assert '<language id="2">Previous EN</language>' in patch_body


def test_update_draft_ignores_partial_patch_echo_and_trusts_get():
    """La reponse PATCH peut echoer un ORM partiel (champs vides). L'adaptateur doit
    decider sur la relecture GET autoritative et PAS declencher de fausse mutation."""
    pre = _content_xml(active=0, fr_title="Ancien FR", en_title="Previous EN")
    patch_echo = (
        '<?xml version="1.0"?>'
        '<prestashop><content>'
        '<id>12</id><id_cms_category>2</id_cms_category><active>0</active>'
        '<meta_title>'
        '<language id="1">Nouveau FR</language>'
        '<language id="2"></language>'
        '</meta_title>'
        '</content></prestashop>'
    )
    post = _content_xml(active=0, fr_title="Nouveau FR", en_title="Previous EN")
    transport = _Transport([_xml(pre), _xml(patch_echo), _xml(post)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    article = adapter.update_draft("12", _draft(title="Nouveau FR"),
                                   expected_revision=_revision_of(pre))
    assert article.revision == _revision_of(post)


def test_update_draft_detects_other_language_mutation_on_get_snapshot():
    """Si le GET autoritatif montre qu'une langue NON ciblee a change, l'ecriture est
    incertaine et details.remote_id porte l'identifiant connu."""
    pre = _content_xml(fr_title="Ancien FR", en_title="Previous EN")
    patch_echo = _content_xml(fr_title="Nouveau FR", en_title="Previous EN")
    post = _content_xml(fr_title="Nouveau FR", en_title="OOPS perdu")
    transport = _Transport([_xml(pre), _xml(patch_echo), _xml(post)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("12", _draft(title="Nouveau FR"),
                             expected_revision=_revision_of(pre))
    assert excinfo.value.code == "unreadable"
    assert excinfo.value.uncertain is True
    assert excinfo.value.details.get("remote_id") == "12"


def test_update_draft_refuses_if_get_shows_flipped_active():
    pre = _content_xml(active=0)
    patch_echo = _content_xml(active=0)
    flipped = _content_xml(active=1)
    transport = _Transport([_xml(pre), _xml(patch_echo), _xml(flipped)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("12", _draft(title="x"),
                             expected_revision=_revision_of(pre))
    assert excinfo.value.code == "unreadable"
    assert excinfo.value.uncertain is True
    assert excinfo.value.details.get("remote_id") == "12"


def test_update_draft_post_read_error_is_uncertain_with_remote_id():
    pre = _content_xml(active=0)
    patch_echo = _content_xml(active=0)
    transport = _Transport([_xml(pre), _xml(patch_echo),
                            _xml("<prestashop></prestashop>", status=503)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("12", _draft(title="x"),
                             expected_revision=_revision_of(pre))
    assert excinfo.value.uncertain is True
    assert excinfo.value.details.get("remote_id") == "12"


def test_update_draft_refuses_if_patch_response_has_wrong_id():
    pre = _content_xml(cms_id="12", active=0)
    wrong_id_echo = _content_xml(cms_id="99", active=0)
    transport = _Transport([_xml(pre), _xml(wrong_id_echo)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("12", _draft(title="x"),
                             expected_revision=_revision_of(pre))
    assert excinfo.value.uncertain is True
    assert excinfo.value.details.get("remote_id") == "12"
    assert excinfo.value.details.get("returned_id") == "99"


# ---- publish : PATCH minimal, verification sur GET autoritatif -----------

def test_publish_toggles_active_true_after_revision_check():
    pre = _content_xml(active=0)
    patch_echo = _content_xml(active=1)
    post = _content_xml(active=1)
    transport = _Transport([_xml(pre), _xml(patch_echo), _xml(post)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    article = adapter.publish("12", expected_revision=_revision_of(pre))
    assert article.status == "published"
    assert article.revision == _revision_of(post)
    sent = transport.calls[1]["payload"].decode("utf-8")
    assert "<active>1</active>" in sent
    assert "<id>12</id>" in sent


def test_publish_uses_patch_with_only_id_and_active():
    pre = _content_xml(active=0)
    patch_echo = _content_xml(active=1)
    post = _content_xml(active=1)
    transport = _Transport([_xml(pre), _xml(patch_echo), _xml(post)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    adapter.publish("12", expected_revision=_revision_of(pre))
    assert transport.calls[1]["method"] == "PATCH"
    patch_body = transport.calls[1]["payload"].decode("utf-8")
    assert "<id>12</id>" in patch_body
    assert "<active>1</active>" in patch_body
    assert "<meta_title>" not in patch_body
    assert "<prestashop><content>" in patch_body


def test_publish_ignores_partial_patch_echo_and_trusts_get():
    """La reponse PATCH peut etre un ORM partiel (champs texte absents). Si le GET
    autoritatif montre tout preserve et active=1, la publication reussit."""
    pre = _content_xml(active=0, fr_body="Texte reel\nligne 2")
    partial_patch = (
        '<?xml version="1.0"?>'
        '<prestashop><content>'
        '<id>12</id><id_cms_category>2</id_cms_category><active>1</active>'
        '</content></prestashop>'
    )
    post = _content_xml(active=1, fr_body="Texte reel\nligne 2")
    transport = _Transport([_xml(pre), _xml(partial_patch), _xml(post)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    article = adapter.publish("12", expected_revision=_revision_of(pre))
    assert article.status == "published"
    assert article.revision == _revision_of(post)


def test_publish_detects_text_rewrite_via_get_snapshot():
    """Si le GET autoritatif revele que le texte a ete reecrit par l'activation
    (conversion de "\\n" literaux signalee par l'ancien labo), on refuse."""
    pre = _content_xml(active=0, fr_body="Ligne 1\nLigne 2")
    patch_echo = _content_xml(active=1, fr_body="Ligne 1\nLigne 2")
    rewritten = _content_xml(active=1, fr_body="Ligne 1 Ligne 2")
    transport = _Transport([_xml(pre), _xml(patch_echo), _xml(rewritten)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("12", expected_revision=_revision_of(pre))
    assert excinfo.value.code == "unreadable"
    assert excinfo.value.uncertain is True
    assert excinfo.value.details.get("remote_id") == "12"


def test_publish_refuses_when_get_still_shows_inactive():
    pre = _content_xml(active=0)
    patch_echo = _content_xml(active=1)
    still_inactive = _content_xml(active=0)
    transport = _Transport([_xml(pre), _xml(patch_echo), _xml(still_inactive)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("12", expected_revision=_revision_of(pre))
    assert excinfo.value.code == "unreadable"
    assert excinfo.value.uncertain is True
    assert excinfo.value.details.get("remote_id") == "12"


def test_publish_post_read_error_is_uncertain_with_remote_id():
    pre = _content_xml(active=0)
    patch_echo = _content_xml(active=1)
    transport = _Transport([_xml(pre), _xml(patch_echo),
                            _xml("<prestashop><errors/></prestashop>", status=401)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("12", expected_revision=_revision_of(pre))
    assert excinfo.value.uncertain is True
    assert excinfo.value.details.get("remote_id") == "12"


def test_publish_server_error_on_patch_is_uncertain():
    pre = _content_xml(active=0)
    transport = _Transport([_xml(pre),
                            _xml("<prestashop></prestashop>", status=503)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("12", expected_revision=_revision_of(pre))
    assert excinfo.value.code == "server"
    assert excinfo.value.uncertain is True


# ---- Portee boutique et langue configuree --------------------------------

def test_id_shop_scopes_every_path_and_lang_targets_configured_id():
    pre = _content_xml(fr_title="Ancien FR", en_title="Previous EN")
    patch_echo = _content_xml(fr_title="Ancien FR", en_title="New EN")
    post = _content_xml(fr_title="Ancien FR", en_title="New EN")
    transport = _Transport([_xml(pre), _xml(patch_echo), _xml(post)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "2", "id_shop": "3"})
    adapter.update_draft("12", _draft(title="New EN"),
                         expected_revision=_revision_of(pre))
    for call in transport.calls:
        assert "id_shop=3" in call["path"]
    patch_body = transport.calls[1]["payload"].decode("utf-8")
    assert '<language id="2">New EN</language>' in patch_body
    # La langue 1 (non ciblee) doit etre re-envoyee avec sa valeur distante exacte.
    assert '<language id="1">Ancien FR</language>' in patch_body


# ---- Incertitude cote ecriture : erreur serveur sur PATCH ----------------

def test_server_error_on_patch_update_is_uncertain():
    pre = _content_xml()
    transport = _Transport([_xml(pre),
                            _xml("<prestashop></prestashop>", status=503)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("12", _draft(title="Autre"),
                             expected_revision=_revision_of(pre))
    assert excinfo.value.code == "server"
    assert excinfo.value.uncertain is True
