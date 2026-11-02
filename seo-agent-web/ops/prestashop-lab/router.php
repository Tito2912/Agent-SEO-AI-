<?php
// Le .htaccess de PrestaShop pour `php -S` (laboratoire local, 02/11/2026) : /api/... vers le
// webservice, les fichiers existants tels quels, tout le reste vers index.php (adresses simplifiées).
$racine = getenv('PRESTASHOP_LAB') . '/shop';
$chemin = urldecode(parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH));
if (preg_match('#^/api(?:/(.*))?$#', $chemin, $m)) {
    $_GET['url'] = $m[1] ?? '';
    $_SERVER['SCRIPT_NAME'] = '/webservice/dispatcher.php';
    $_SERVER['SCRIPT_FILENAME'] = $racine . '/webservice/dispatcher.php';
    chdir($racine . '/webservice');
    require $racine . '/webservice/dispatcher.php';
    return true;
}
if ($chemin !== '/' && is_file($racine . $chemin) && !preg_match('#\.php$#', $chemin)) {
    $type = mime_content_type($racine . $chemin) ?: 'application/octet-stream';
    if (preg_match('#\.css$#', $chemin)) $type = 'text/css';
    if (preg_match('#\.js$#', $chemin)) $type = 'application/javascript';
    header('Content-Type: ' . $type);
    readfile($racine . $chemin);
    return true;
}
$script = preg_match('#^(/[^?]*\.php)#', $chemin, $p) && is_file($racine . $p[1]) ? $p[1] : '/index.php';
$_SERVER['SCRIPT_NAME'] = $script;
$_SERVER['SCRIPT_FILENAME'] = $racine . $script;
chdir(dirname($racine . $script));
require $racine . $script;
return true;
