<?php
$state = getenv('NOYARU_WP_DEMO_STATE');
if (!$state || !is_dir($state)) throw new RuntimeException('Missing private state');
$salts = "<?php\n";
foreach (['AUTH_KEY', 'SECURE_AUTH_KEY', 'LOGGED_IN_KEY', 'NONCE_KEY',
          'AUTH_SALT', 'SECURE_AUTH_SALT', 'LOGGED_IN_SALT', 'NONCE_SALT'] as $name) {
    $salts .= 'define(' . var_export($name, true) . ', ' . var_export(bin2hex(random_bytes(48)), true) . ");\n";
}
file_put_contents($state . '/salts.php', $salts);
chmod($state . '/salts.php', 0600);
define('WP_INSTALLING', true);
require '/wordpress/wp-load.php';
require_once ABSPATH . 'wp-admin/includes/upgrade.php';
wp_install('Noyaru — démonstration temporaire', 'noyaru_disabled_admin',
           'admin@example.invalid', true, '', bin2hex(random_bytes(48)));
update_option('active_plugins', ['wordpress-seo/wp-seo.php']);
update_option('blog_public', '1');
update_option('admin_email', 'admin@example.invalid');
