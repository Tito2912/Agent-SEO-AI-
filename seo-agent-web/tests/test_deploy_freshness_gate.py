# -*- coding: utf-8 -*-
"""Un site qui REPOND n'est pas un site A JOUR.

Le 11/09/2026, les neuf fixtures servaient l'etat du cycle precedent et la phase d'attente les
declarait pretes parce que leur index renvoyait 200. Un cycle entier a ete conduit dessus :
injection declaree conforme, 291 pages corrigees, neuf previews vertes — le tout mesure sur un
contenu qui n'etait pas celui qu'on venait de pousser. Pire, le verificateur d'injection comptait
`image_redirects` comme detectee alors que sa seule preuve etait la redirection http->https de
`og.png`, portee par une tout autre page.

Ce test tient la seule question qui tranche : le site sert-il le cycle qu'on vient de pousser ?
"""

from __future__ import annotations

import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops import stack_loop  # noqa: E402

CYCLE = "20260911T070037Z"


class _Args:
    """Ce que la ligne de commande passerait a `cmd_await`, en plus court."""

    def __init__(self, site: str, cycle: str) -> None:
        self.stack = "static-html"
        self.site = site
        self.cycle = cycle
        self.timeout = 0.5
        self.every = 0.1


@pytest.fixture()
def serveur():
    """Un hote local dont on choisit le marqueur servi — y compris aucun."""
    etat = {"corps": b""}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 — signature imposee par http.server
            corps = etat["corps"]
            if self.path == "/_cycle.txt" and corps:
                self.send_response(200)
                self.send_header("Content-Length", str(len(corps)))
                self.end_headers()
                self.wfile.write(corps)
            else:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()

        def log_message(self, *args):  # le journal de http.server pollue la sortie des tests
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        yield "http://127.0.0.1:%d" % httpd.server_address[1], etat
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_un_site_sans_marqueur_est_perime(serveur):
    """Le cas reel du 11/09 : le site repond, mais ne porte pas le cycle."""
    site, _etat = serveur
    assert stack_loop.cmd_await(_Args(site, CYCLE)) == 1


def test_un_marqueur_ancien_est_perime(serveur):
    site, etat = serveur
    etat["corps"] = b"20260910T235959Z\n"
    assert stack_loop.cmd_await(_Args(site, CYCLE)) == 1


def test_le_marqueur_du_cycle_ouvre_la_porte(serveur):
    site, etat = serveur
    etat["corps"] = CYCLE.encode() + b"\n"
    assert stack_loop.cmd_await(_Args(site, CYCLE)) == 0
