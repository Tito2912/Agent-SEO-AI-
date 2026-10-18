<?php
// Disposable fixture only. The Playground server is bound to 127.0.0.1.
require '/wordpress/wp-load.php';
$id = wp_insert_post([
    'post_title' => 'Réparation de vélos à Lyon',
    'post_name' => 'atelier-demo',
    'post_type' => 'page',
    'post_status' => 'publish',
    'post_content' => '<h1>Notre atelier de réparation</h1><p>Notre atelier entretient et répare les vélos à Lyon.</p>',
]);
if (is_wp_error($id) || !$id) {
    throw new RuntimeException('Cannot create the demonstration page.');
}
update_post_meta($id, '_yoast_wpseo_title', 'Démo');
$builder = YoastSEO()->classes->get(\Yoast\WP\SEO\Builders\Indexable_Builder::class);
$builder->build_for_id_and_type($id, 'post');

$accounts = [];
foreach (['editor' => 'noyaru_lab', 'subscriber' => 'noyaru_reader'] as $role => $login) {
    $user = wp_create_user($login, wp_generate_password(40), $login . '@example.invalid');
    if (is_wp_error($user)) {
        throw new RuntimeException('Cannot create the disposable account.');
    }
    $account = new WP_User($user);
    $account->set_role($role);
    if ($role === 'editor') {
        $account->add_cap('wpseo_edit_advanced_metadata');
    }
    $result = WP_Application_Passwords::create_new_application_password($user, ['name' => 'Noyaru local test']);
    if (is_wp_error($result)) {
        throw new RuntimeException('Cannot create the disposable application password.');
    }
    $accounts[$role] = ['username' => $login, 'password' => $result[0]];
}
file_put_contents('/noyaru-lab.json', wp_json_encode([
    'post_id' => $id,
    'url' => get_permalink($id),
    'site_url' => home_url(),
    'accounts' => $accounts,
    'wp' => get_bloginfo('version'),
    'yoast' => WPSEO_VERSION,
]));
