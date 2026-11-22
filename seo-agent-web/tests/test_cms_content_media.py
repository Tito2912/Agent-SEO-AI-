"""Real raster validation and injected HTTP uploads; no CMS account is contacted."""
from __future__ import annotations

import io

import pytest
from PIL import Image

from backend import cms_content_media as media, cms_content_transport as transport
from backend.cms_commun import CmsError
import test_cms_content_transport as _http

wire = _http.wire


def image_bytes(kind="PNG", size=(32, 24)):
    output = io.BytesIO()
    Image.new("RGB", size, (40, 130, 80)).save(output, format=kind)
    return output.getvalue()


@pytest.mark.parametrize("kind,extension,mime", [("PNG", "png", "image/png"), ("JPEG", "jpg", "image/jpeg"),
                                               ("WEBP", "webp", "image/webp")], ids=["png", "jpeg", "webp"])
def test_wordpress_upload_sends_validated_raw_bytes_with_safe_disposition(wire, kind, extension, mime):
    client = _http.wp()
    _http.verify_wp(client, wire)
    data = image_bytes(kind)
    url = "https://atelier.example/wp-content/uploads/photo." + extension
    wire["responses"].append(_http.Response({"id": 42, "source_url": url}, status=201))
    result = client.upload_image(data=data, filename="photo." + extension, content_type=mime)
    assert result == {"id": "42", "url": url}
    call = wire["calls"][-1]
    assert call["method"] == "POST" and call["path"] == "/wp-json/wp/v2/media"
    assert call["body"] == data
    assert call["headers"]["Content-Type"] == mime
    assert call["headers"]["Content-Disposition"] == 'attachment; filename="photo.' + extension + '"'
    assert call["headers"]["Authorization"].startswith("Basic ")
    assert len(wire["calls"]) == 2


def ghost(wire, *, project_url="https://atelier.example/atelier/"):
    client = transport.ContentTransport("ghost", {"base_url": "https://atelier.ghost.io", "key": _http.KEY}, project_url=project_url)
    wire["responses"] += [_http.Response({"site": {"url": project_url, "title": "Atelier"}}), _http.Response({"posts": []})]
    assert client.check()["verified"]
    return client


def test_ghost_upload_is_multipart_and_relative_url_uses_public_project(wire):
    client = ghost(wire)
    data = image_bytes("WEBP")
    wire["responses"].append(_http.Response({"images": [{"url": "content/images/photo.webp", "ref": "photo.webp"}]}, status=201))
    result = client.upload_image(data=data, filename="photo.webp", content_type="image/webp")
    assert result == {"id": "https://atelier.example/atelier/content/images/photo.webp", "url": "https://atelier.example/atelier/content/images/photo.webp"}
    call = wire["calls"][-1]
    assert call["host"] == "atelier.ghost.io" and call["path"] == "/ghost/api/admin/images/upload/"
    assert call["headers"]["Authorization"].startswith("Ghost ")
    assert call["headers"]["Accept-Version"] == "v5.0"
    assert "multipart/form-data; boundary=" in call["headers"]["Content-Type"]
    boundary = call["headers"]["Content-Type"].split("boundary=", 1)[1]
    assert call["body"].startswith(("--" + boundary + "\r\n").encode("ascii"))
    assert call["body"].endswith(("--" + boundary + "--\r\n").encode("ascii"))
    assert b'name="file"; filename="photo.webp"' in call["body"]
    assert b'Content-Type: image/webp\r\n\r\n' in call["body"] and data in call["body"]
    assert b'name="purpose"\r\n\r\nimage' in call["body"]
    assert len(wire["calls"]) == 3


@pytest.mark.parametrize("data,filename,mime", [
    (b"<svg xmlns='http://www.w3.org/2000/svg' />", "photo.svg", "image/svg+xml"),
    (b"<html><script>alert(1)</script></html>", "photo.png", "image/png"),
    (image_bytes("JPEG"), "photo.png", "image/png"),
    (image_bytes("PNG"), "photo.jpg", "image/png"),
    (image_bytes("PNG"), "photo.png\r\nX-Header:bad", "image/png"),
    (image_bytes("PNG"), "../photo.png", "image/png"),
    (image_bytes("PNG"), "photo\".png", "image/png"),
    (image_bytes("PNG"), "photo.png", "image/png\r\nBad:1"),
    (b"", "photo.png", "image/png"),
    (image_bytes("JPEG")[:40], "photo.jpg", "image/jpeg"),
], ids=["svg", "html", "mime-lie", "extension", "filename-control", "traversal", "quote", "mime-control", "empty", "truncated"])
def test_invalid_or_deceptive_images_are_refused_before_upload(wire, data, filename, mime):
    client = _http.wp()
    _http.verify_wp(client, wire)
    count = len(wire["calls"])
    with pytest.raises(CmsError) as caught:
        client.upload_image(data=data, filename=filename, content_type=mime)
    assert caught.value.code == "image_invalid" and not caught.value.uncertain
    assert len(wire["calls"]) == count


def test_pixel_and_byte_limits_are_checked_before_upload(wire, monkeypatch):
    client = _http.wp()
    _http.verify_wp(client, wire)
    data = image_bytes()
    monkeypatch.setattr(media, "MAX_IMAGE_BYTES", len(data) - 1)
    with pytest.raises(CmsError):
        client.upload_image(data=data, filename="photo.png", content_type="image/png")
    monkeypatch.setattr(media, "MAX_IMAGE_BYTES", len(data))
    monkeypatch.setattr(media, "MAX_IMAGE_PIXELS", 1)
    with pytest.raises(CmsError):
        client.upload_image(data=data, filename="photo.png", content_type="image/png")
    assert len(wire["calls"]) == 1


def test_animated_images_are_not_accepted_as_single_rasters(wire):
    client = _http.wp()
    _http.verify_wp(client, wire)
    output = io.BytesIO()
    frames = [Image.new("RGB", (16, 16), color) for color in ("red", "blue")]
    frames[0].save(output, format="WEBP", save_all=True, append_images=frames[1:], duration=50, loop=0)
    with pytest.raises(CmsError) as caught:
        client.upload_image(data=output.getvalue(), filename="photo.webp", content_type="image/webp")
    assert caught.value.code == "image_invalid" and len(wire["calls"]) == 1


def test_upload_requires_verified_site_and_is_not_universally_supported(wire):
    client = _http.wp()
    assert client.supports_image_upload
    with pytest.raises(CmsError) as caught:
        client.upload_image(data=image_bytes(), filename="photo.png", content_type="image/png")
    assert caught.value.code == "non_verifie"
    other = transport.ContentTransport("shopify", {"shop": "atelier.myshopify.com", "token": "fictitious"}, project_url=_http.PROJECT)
    assert not other.supports_image_upload
    with pytest.raises(CmsError) as caught:
        other.upload_image(data=image_bytes(), filename="photo.png", content_type="image/png")
    assert caught.value.code == "unsupported"
    assert not wire["calls"]


@pytest.mark.parametrize("url", ["http://atelier.example/photo.png", "https://user:pass@atelier.example/photo.png",
    "https://atelier.example/photo.png?token=temporary", "https://atelier.example/photo.png#fragment",
    "//attacker.example/photo.png", "javascript:alert(1)", "https://atelier.example/photo%0a.png"],
    ids=["http", "userinfo", "signed", "fragment", "authority", "javascript", "control"])
def test_invalid_public_url_after_upload_is_uncertain_and_never_retried(wire, url):
    client = _http.wp()
    _http.verify_wp(client, wire)
    wire["responses"].append(_http.Response({"id": 42, "source_url": url}, status=201))
    with pytest.raises(CmsError) as caught:
        client.upload_image(data=image_bytes(), filename="photo.png", content_type="image/png")
    assert caught.value.code == "image_url" and caught.value.uncertain
    assert len(wire["calls"]) == 2


def test_private_cdn_resolution_after_upload_is_uncertain(wire, monkeypatch):
    client = _http.wp()
    _http.verify_wp(client, wire)
    resolver = transport.adresse_publique

    def address(host, cms):
        if host == "private.example":
            raise CmsError("Adresse privée", code="adresse_privee")
        return resolver(host, cms)

    monkeypatch.setattr(transport, "adresse_publique", address)
    wire["responses"].append(_http.Response({"id": 42, "source_url": "https://private.example/photo.png"}, status=201))
    with pytest.raises(CmsError) as caught:
        client.upload_image(data=image_bytes(), filename="photo.png", content_type="image/png")
    assert caught.value.code == "image_url" and caught.value.uncertain
    assert len(wire["calls"]) == 2


@pytest.mark.parametrize("response,code,uncertain", [
    (_http.Response(status=302), "redirection", True),
    (_http.Response(status=403), "acces_refuse", False),
    (_http.Response(status=500), "reponse_illisible", True),
    (_http.Response({"id": True, "source_url": "https://atelier.example/a.png"}, status=201), "reponse_illisible", True),
    (_http.Response(raw=b"invalid", status=201), "reponse_illisible", True),
    (OSError("fictitious-password"), "injoignable", True),
], ids=["redirect", "permission", "server", "missing-id", "bad-json", "timeout"])
def test_upload_preserves_transport_error_certainty_and_never_retries(wire, response, code, uncertain):
    client = _http.wp()
    _http.verify_wp(client, wire)
    wire["responses"].append(response)
    with pytest.raises(CmsError) as caught:
        client.upload_image(data=image_bytes(), filename="photo.png", content_type="image/png")
    assert caught.value.code == code and caught.value.uncertain is uncertain
    assert "fictitious-password" not in str(caught.value)
    assert len(wire["calls"]) == 2


def test_public_cdn_returned_by_ghost_is_accepted_without_forwarding_credentials(wire):
    client = ghost(wire)
    wire["responses"].append(_http.Response({"images": [{"url": "https://cdn.example/photo.webp"}]}))
    result = client.upload_image(data=image_bytes("WEBP"), filename="photo.webp", content_type="image/webp")
    assert result["url"] == "https://cdn.example/photo.webp"
    assert all(call["host"] == "atelier.ghost.io" for call in wire["calls"])
    assert ("cdn.example", "média CMS") in wire["dns"]


def test_wordpress_upload_keeps_its_real_id_if_public_url_is_rejected(wire):
    client = _http.wp()
    _http.verify_wp(client, wire)
    wire["responses"].append(_http.Response({"id": 42, "source_url": "http://atelier.example/private.webp"}, status=201))
    with pytest.raises(CmsError) as error:
        client.upload_image(data=image_bytes("WEBP"), filename="photo.webp", content_type="image/webp")
    assert error.value.uncertain and error.value.remote_id == "42"
    assert len(wire["calls"]) == 2
