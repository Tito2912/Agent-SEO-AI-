<?php
add_filter('pre_wp_mail', '__return_false');
add_filter('upload_dir', static function ($upload) {
    $upload['basedir'] = getenv('NOYARU_WP_DEMO_STATE') . '/uploads';
    $upload['path'] = $upload['basedir'] . $upload['subdir'];
    return $upload;
});
