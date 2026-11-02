<?php
// Installe l'anglais comme le fait l'écran Localisation de PrestaShop (pack officiel), après avoir
// retiré la langue « en » ajoutée à la main dans la base.
define('_PS_ADMIN_DIR_', getenv('PRESTASHOP_LAB') . '/shop/admin');
require getenv('PRESTASHOP_LAB') . '/shop/config/config.inc.php';
// Le noyau Symfony du back-office : l'installation d'une langue traduit les tables par lui.
global $kernel;
$kernel = new AppKernel('prod', false);
$kernel->boot();

$existante = (int) Language::getIdByIso('en');
if ($existante) {
    $langue = new Language($existante);
    var_dump(['suppression' => $langue->delete()]);
}
$erreurs = [];
$ok = Language::downloadAndInstallLanguagePack('en', _PS_VERSION_, $erreurs, true);
var_dump(['installee' => $ok, 'erreurs' => $erreurs, 'id' => Language::getIdByIso('en', true)]);
