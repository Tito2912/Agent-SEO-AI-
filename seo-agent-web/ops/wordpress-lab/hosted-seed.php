<?php
// PUBLIC, disposable demonstration. No customer data or reusable login password.
require '/wordpress/wp-load.php';
foreach (get_users() as $user) {
    wp_set_password(wp_generate_password(64, true, true), $user->ID);
    WP_Application_Passwords::delete_all_application_passwords($user->ID);
    WP_Session_Tokens::get_instance($user->ID)->destroy_all();
    wp_update_user(['ID' => $user->ID, 'user_email' => 'disabled-' . $user->ID . '@example.invalid']);
}
foreach (get_posts(['post_type' => 'any', 'post_status' => 'any', 'numberposts' => -1]) as $post) {
    wp_delete_post($post->ID, true);
}
$GLOBALS['wp_rewrite']->set_permalink_structure('/%postname%/');
$id = wp_insert_post([
    'post_title' => 'Réparation de vélos à Lyon',
    'post_name' => 'atelier-demo', 'post_type' => 'page', 'post_status' => 'publish',
    'post_content' => '<h1>Atelier Noyaru Démo</h1><p>Site fictif réservé aux essais du connecteur SEO. Aucun service ni aucune vente.</p>',
]);
if (is_wp_error($id) || !$id) throw new RuntimeException('Cannot create demonstration page');
flush_rewrite_rules(false);
update_post_meta($id, '_yoast_wpseo_title', 'Démo');
YoastSEO()->classes->get(\Yoast\WP\SEO\Builders\Indexable_Builder::class)->build_for_id_and_type($id, 'post');
$accounts = [];
foreach (['editor' => 'noyaru_demo', 'subscriber' => 'noyaru_reader'] as $role => $login) {
    $user = wp_create_user($login, wp_generate_password(64, true, true), $login . '@example.invalid');
    if (is_wp_error($user)) throw new RuntimeException('Cannot create demonstration account');
    $account = new WP_User($user);
    $account->set_role($role);
    if ($role === 'editor') $account->add_cap('wpseo_edit_advanced_metadata');
    $result = WP_Application_Passwords::create_new_application_password($user, ['name' => 'Noyaru disposable HTTPS test']);
    if (is_wp_error($result)) throw new RuntimeException('Cannot create demonstration credential');
    $accounts[$role] = ['username' => $login, 'password' => $result[0]];
}
// Outside the web document root; exposed only by the gateway's bearer-authenticated endpoint.
file_put_contents('/noyaru-demo.json', wp_json_encode([
    'post_id' => $id, 'url' => get_permalink($id), 'site_url' => home_url(),
    'accounts' => $accounts, 'wp' => get_bloginfo('version'), 'yoast' => WPSEO_VERSION,
]));
