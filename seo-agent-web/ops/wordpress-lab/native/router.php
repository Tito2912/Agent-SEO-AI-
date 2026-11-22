<?php
$path = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
$uploadsPrefix = '/wp-content/uploads/';
if (str_starts_with($path, $uploadsPrefix)) {
    $name = substr($path, strlen($uploadsPrefix));
    $base = realpath(getenv('NOYARU_WP_DEMO_STATE') . '/uploads');
    $image = $base ? realpath($base . '/' . $name) : false;
    if (!in_array($_SERVER['REQUEST_METHOD'], ['GET', 'HEAD'], true) ||
        !preg_match('#^(?:[0-9]{4}/[0-9]{2}/)?[A-Za-z0-9][A-Za-z0-9._-]{0,139}\.webp$#D', $name) ||
        str_contains($name, '..') || !$image || !str_starts_with($image, $base . '/') || !is_file($image)) {
        http_response_code(404);
        return true;
    }
    header('Content-Type: image/webp');
    header('Content-Length: ' . filesize($image));
    if ($_SERVER['REQUEST_METHOD'] === 'GET') readfile($image);
    return true;
}
$file = realpath('/wordpress' . $path);
if ($file && str_starts_with($file, '/wordpress/') && is_file($file) &&
    preg_match('/\.(css|js|png|jpg|jpeg|gif|webp|svg|ico|woff2?|ttf)$/', $file)) return false;
$_SERVER['SCRIPT_NAME'] = '/index.php';
$_SERVER['PHP_SELF'] = '/index.php';
require '/wordpress/index.php';
