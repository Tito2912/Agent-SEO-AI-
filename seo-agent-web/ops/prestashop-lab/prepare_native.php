<?php
// ONLY a disposable local shop, outside the repository. Never a merchant installation.
$root = realpath(getenv('PRESTASHOP_LAB') ?: '');
if (!$root || !is_file($root . '/shop/config/config.inc.php')) {
    throw new RuntimeException('Expected PRESTASHOP_LAB with an installed disposable shop.');
}
if (file_exists($root . '/native-key.txt') || file_exists($root . '/fixture.json')) {
    throw new RuntimeException('The native fixture is already prepared; refusing to replace credentials.');
}
$_SERVER['HTTP_HOST'] = '127.0.0.1';
$adminEntries = array_values(array_filter(glob($root . '/shop/admin*/index.php'), static function ($entry) {
    return is_dir(dirname($entry) . '/themes');
}));
if (count($adminEntries) !== 1) {
    throw new RuntimeException('Expected one admin directory in the disposable shop.');
}
define('_PS_ADMIN_DIR_', dirname($adminEntries[0]));
require $root . '/shop/config/config.inc.php';
$domain = Configuration::get('PS_SHOP_DOMAIN');
if (!preg_match('/^(127\.0\.0\.1|localhost):[0-9]+$/D', $domain) || !in_array(_PS_VERSION_, ['8.2.8', '9.2.1'], true)) {
    throw new RuntimeException('This fixture requires a loopback PrestaShop 8.2.8 or 9.2.1 shop.');
}
Configuration::updateValue('PS_WEBSERVICE', 1);
Configuration::updateValue('PS_SHOP_ENABLE', 1);
Configuration::updateValue('PS_MAIL_METHOD', 3);
$english = (int) Language::getIdByIso('en');
if (!$english) {
    $language = new Language();
    $language->name = 'English';
    $language->iso_code = 'en';
    $language->language_code = 'en-us';
    $language->locale = 'en-US';
    $language->active = 1;
    $language->is_rtl = 0;
    $language->date_format_lite = 'm/d/Y';
    $language->date_format_full = 'm/d/Y H:i:s';
    if (!$language->add()) {
        throw new RuntimeException('Cannot add the second native test language.');
    }
    $english = (int) $language->id;
}
$key = strtoupper(bin2hex(random_bytes(16)));
$account = new WebserviceKey();
$account->key = $key;
$account->description = 'Noyaru disposable native recipe';
$account->active = 1;
if (!$account->add() || !WebserviceKey::setPermissionForAccount($account->id, [
    'content_management_system' => ['GET' => true, 'POST' => true, 'PATCH' => true],
])) {
    throw new RuntimeException('Cannot prepare the disposable CMS-page permissions.');
}
$handle = fopen($root . '/native-key.txt', 'x');
if (!$handle) {
    throw new RuntimeException('Cannot create private fixture credentials.');
}
chmod($root . '/native-key.txt', 0600);
fwrite($handle, $key);
fclose($handle);
$defaultLanguage = (int) Configuration::get('PS_LANG_DEFAULT');
$metadata = ['version' => _PS_VERSION_, 'default_lang' => $defaultLanguage,
    'default_iso' => Language::getIsoById($defaultLanguage), 'english_lang' => $english,
    'friendly_urls' => (bool) Configuration::get('PS_REWRITING_SETTINGS'), 'server_url' => 'http://' . $domain];
file_put_contents($root . '/fixture.json', json_encode($metadata, JSON_PRETTY_PRINT));
echo json_encode($metadata) . PHP_EOL;
