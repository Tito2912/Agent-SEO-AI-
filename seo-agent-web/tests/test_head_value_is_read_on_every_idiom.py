"""Un controle qui ne sait pas LIRE ne dit pas « je ne sais pas » : il dit « rien a signaler ».

`_find_head_text_value` rend la valeur ecrite dans le fichier. Tout le bloc sequentiel des
familles de doublons la lit a travers elle — pour relancer le modele, pour verifier le plancher
de longueur, et pour la garantie dure qui interdit de recopier une valeur deja posee.
`_keep_length_above_floor` s'en sert aussi.

Mesure du 16/09/2026, cycle des neuf idiomes, nuxt : elle rendait `None` sur
`useHead({ meta: [{ name: 'description', content: '…' }] })`. Sur cette stack, TOUS ces controles
devenaient donc inertes — silencieusement. Cinq pages sont sorties entre 76 et 92 caracteres pour
un minimum de 100 :

    double-slash 92 · duplicate-a 88 · duplicate-b 76 · hreflang-invalid 84 · noindex-no-desc 78

et le journal du passage n'a rien dit, puisque la valeur lue etait vide et qu'une valeur vide ne
declenche aucun refus. C'est la meme forme que le `except Exception: pass` du 15/09 qui masquait
une mesure absente : l'echec de lecture se presentait comme une absence de probleme.

Ces tests couvrent les idiomes des neuf stacks du banc, pour qu'un trou pareil se voie en local.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-head-read-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

ATTENDU = "La description de la page, assez longue pour depasser le plancher de cent caracteres sans peine."

NUXT = """<script setup>
useHead({
  title: 'Page du parcours',
  meta: [
    { name: 'description', content: '%s' },
    { property: 'og:description', content: 'Le texte Open Graph, qui est un AUTRE texte.' },
  ],
})
</script>
""" % ATTENDU

NUXT_SANS_DESCRIPTION = """<script setup>
useHead({ meta: [{ property: 'og:description', content: 'Open Graph seulement.' }] })
</script>
"""


def test_nuxt_la_description_vit_dans_un_objet_qui_la_nomme() -> None:
    lu = app_module._find_head_text_value(NUXT, "description")
    assert lu is not None, "la valeur nuxt n'est pas lue : tous les controles deviennent inertes"
    assert lu[1] == ATTENDU, lu


def test_nuxt_og_description_n_est_PAS_la_description_de_la_page() -> None:
    """Le piege jumeau : lire l'Open Graph a la place ferait passer un controle sur la mauvaise
    valeur, ce qui est pire que de ne rien lire. Mieux vaut rendre None et s'abstenir."""
    assert app_module._find_head_text_value(NUXT_SANS_DESCRIPTION, "description") is None


def test_le_litteral_rendu_permet_un_remplacement_borne() -> None:
    """Le premier element du couple est la sous-chaine EXACTE a remplacer.

    Les appelants font `contenu.replace(ancien_litteral, nouveau_litteral, 1)` : si le litteral
    ne se retrouve pas tel quel dans la source, la reecriture est un silencieux non-evenement.
    """
    litteral, valeur = app_module._find_head_text_value(NUXT, "description")
    assert litteral in NUXT, litteral
    assert valeur in litteral, (valeur, litteral)


def test_les_idiomes_deja_couverts_le_restent() -> None:
    """Sans cette moitie, on pourrait « reparer » nuxt en cassant les huit autres stacks."""
    cas = {
        "html": '<meta name="description" content="%s" />' % ATTENDU,
        "next_metadata": "export const metadata = {\n  description: '%s',\n};" % ATTENDU,
        "front_matter": 'description: "%s"\n' % ATTENDU,
    }
    for nom, source in cas.items():
        lu = app_module._find_head_text_value(source, "description")
        assert lu is not None and lu[1] == ATTENDU, (nom, lu)
