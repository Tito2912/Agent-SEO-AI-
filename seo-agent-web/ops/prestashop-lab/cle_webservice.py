"""Active le webservice du PrestaShop de laboratoire et crée la clé « Noyaru » (jamais affichée).

Droits : lecture (GET, HEAD) et modification partielle (PATCH) des produits, catégories et pages
CMS, plus la lecture des langues et de la configuration — rien d'autre (ni POST, ni DELETE)."""
import os
import secrets
import string
import subprocess
from pathlib import Path

ICI = Path(os.environ["PRESTASHOP_LAB"])
MARIADB = ICI / "mariadb-11.4.13-winx64" / "bin" / "mariadb.exe"
cle_fichier = ICI / "cle_ws.txt"
cle = cle_fichier.read_text().strip() if cle_fichier.exists() else "".join(
    secrets.choice(string.ascii_uppercase + string.digits) for _ in range(32))
cle_fichier.write_text(cle)

droits = {r: ("GET", "HEAD", "PATCH") for r in ("products", "categories", "content_management_system")}
droits.update({"languages": ("GET", "HEAD"), "configurations": ("GET", "HEAD")})
sql = [
    "DELETE FROM ps_configuration WHERE name = 'PS_WEBSERVICE'",
    "INSERT INTO ps_configuration (name, value, date_add, date_upd) VALUES ('PS_WEBSERVICE', '1', NOW(), NOW())",
    "DELETE p FROM ps_webservice_permission p JOIN ps_webservice_account a USING (id_webservice_account) WHERE a.description = 'Noyaru'",
    "DELETE s FROM ps_webservice_account_shop s JOIN ps_webservice_account a USING (id_webservice_account) WHERE a.description = 'Noyaru'",
    "DELETE FROM ps_webservice_account WHERE description = 'Noyaru'",
    f"INSERT INTO ps_webservice_account (`key`, description, class_name, is_module, active) VALUES ('{cle}', 'Noyaru', 'WebserviceRequest', 0, 1)",
    "SET @id = LAST_INSERT_ID()",
    "INSERT INTO ps_webservice_account_shop (id_webservice_account, id_shop) VALUES (@id, 1)",
] + [f"INSERT INTO ps_webservice_permission (resource, method, id_webservice_account) VALUES ('{r}', '{m}', @id)"
     for r, methodes in droits.items() for m in methodes]
subprocess.run([str(MARIADB), "-h127.0.0.1", "-P3307", "-uroot", "prestashop", "-e", ";\n".join(sql)], check=True)
print("webservice actif, clé Noyaru créée :", len(cle), "caractères,", sum(len(m) for m in droits.values()), "droits")
