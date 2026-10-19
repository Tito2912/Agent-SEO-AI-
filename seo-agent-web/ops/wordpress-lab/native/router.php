<?php
$path = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
$file = realpath('/wordpress' . $path);
if ($file && str_starts_with($file, '/wordpress/') && is_file($file) &&
    preg_match('/\.(css|js|png|jpg|jpeg|gif|webp|svg|ico|woff2?|ttf)$/', $file)) return false;
$_SERVER['SCRIPT_NAME'] = '/index.php';
$_SERVER['PHP_SELF'] = '/index.php';
require '/wordpress/index.php';
