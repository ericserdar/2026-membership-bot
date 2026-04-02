<?php
/**
 * CougConnect Discord Verification Page
 *
 * INSTALLATION:
 * 1. Upload this file to your theme or create it as a must-use plugin in /wp-content/mu-plugins/
 * 2. In WordPress admin, create a new Page with the slug "discord-verify"
 * 3. In the page content, add the shortcode: [cougconnect_discord_verify]
 * 4. Set BOT_VERIFY_SECRET and BOT_PUBLIC_URL in your wp-config.php (see below)
 *
 * Add these constants to wp-config.php:
 *   define( 'COUGCONNECT_BOT_SECRET', 'your_secret_here' );
 *   define( 'COUGCONNECT_BOT_URL',    'https://your-app.railway.app' );
 */

if ( ! defined( 'ABSPATH' ) ) {
    exit;
}

add_shortcode( 'cougconnect_discord_verify', 'cougconnect_discord_verify_shortcode' );

function cougconnect_discord_verify_shortcode() {
    // Must be logged in
    if ( ! is_user_logged_in() ) {
        $redirect = esc_url( add_query_arg( $_GET, get_permalink() ) );
        wp_redirect( wp_login_url( $redirect ) );
        exit;
    }

    $token      = isset( $_GET['token'] )      ? sanitize_text_field( $_GET['token'] )      : '';
    $discord_id = isset( $_GET['discord_id'] ) ? sanitize_text_field( $_GET['discord_id'] ) : '';

    if ( ! $token || ! $discord_id ) {
        return cougconnect_verify_html( 'error', 'Invalid verification link. Please click the button in Discord again.' );
    }

    $user = wp_get_current_user();

    // Check MemberPress is active
    if ( ! function_exists( 'mepr_get_user' ) && ! class_exists( 'MeprUser' ) ) {
        return cougconnect_verify_html( 'error', 'MemberPress is not active on this site.' );
    }

    $mepr_user = new MeprUser( $user->ID );
    $active_memberships = $mepr_user->active_product_subscriptions( 'products' );

    // Determine tier from active memberships
    $tier = cougconnect_resolve_tier( $active_memberships );

    // Get MemberPress member ID
    $mp_member_id = $user->ID; // WP user ID == MemberPress member ID in most setups

    // POST to Railway bot
    $bot_url    = defined( 'COUGCONNECT_BOT_URL' )    ? COUGCONNECT_BOT_URL    : '';
    $bot_secret = defined( 'COUGCONNECT_BOT_SECRET' ) ? COUGCONNECT_BOT_SECRET : '';

    if ( ! $bot_url ) {
        return cougconnect_verify_html( 'error', 'Bot URL is not configured. Contact an admin.' );
    }

    $payload = wp_json_encode( [
        'token'        => $token,
        'discord_id'   => $discord_id,
        'tier'         => $tier,
        'mp_member_id' => $mp_member_id,
        'mp_email'     => $user->user_email,
        'secret'       => $bot_secret,
    ] );

    $response = wp_remote_post( $bot_url . '/verify', [
        'headers'     => [ 'Content-Type' => 'application/json' ],
        'body'        => $payload,
        'timeout'     => 15,
        'data_format' => 'body',
    ] );

    if ( is_wp_error( $response ) ) {
        return cougconnect_verify_html( 'error', 'Could not reach the Discord bot. Please try again or contact an admin.' );
    }

    $code = wp_remote_retrieve_response_code( $response );
    $body = json_decode( wp_remote_retrieve_body( $response ), true );

    if ( $code === 200 && isset( $body['status'] ) && $body['status'] === 'ok' ) {
        $tier_labels = [
            'gold'         => 'Gold',
            'silver'       => 'Silver',
            'insider'      => 'Insider',
            'unsubscribed' => 'Unsubscribed',
        ];
        $label = $tier_labels[ $tier ] ?? ucfirst( $tier );
        return cougconnect_verify_html( 'success', "You're verified! Your <strong>{$label}</strong> role has been assigned in Discord. You can close this window." );
    }

    $error = isset( $body['error'] ) ? esc_html( $body['error'] ) : 'Unknown error';

    if ( strpos( $error, 'expired' ) !== false || strpos( $error, 'Invalid' ) !== false ) {
        return cougconnect_verify_html( 'error', 'This verification link has expired or already been used. Please click the button in Discord again.' );
    }

    return cougconnect_verify_html( 'error', "Verification failed: {$error}" );
}


/**
 * Resolve the CougConnect tier from a list of active MemberPress product objects.
 * Checks membership IDs against the same groups defined in the bot's .env.
 *
 * To keep this in sync with the bot, define these in wp-config.php:
 *   define( 'COUGCONNECT_TIER_GOLD_IDS',    '101,205' );
 *   define( 'COUGCONNECT_TIER_SILVER_IDS',  '102,206' );
 *   define( 'COUGCONNECT_TIER_INSIDER_IDS', '103,207' );
 */
function cougconnect_resolve_tier( array $memberships ): string {
    $parse = function( string $const ) {
        if ( ! defined( $const ) ) return [];
        return array_map( 'intval', array_filter( explode( ',', constant( $const ) ), 'is_numeric' ) );
    };

    $gold_ids    = $parse( 'COUGCONNECT_TIER_GOLD_IDS' );
    $silver_ids  = $parse( 'COUGCONNECT_TIER_SILVER_IDS' );
    $insider_ids = $parse( 'COUGCONNECT_TIER_INSIDER_IDS' );

    $active_ids = array_map( fn( $m ) => (int) ( is_object( $m ) ? $m->ID : $m ), $memberships );

    if ( array_intersect( $active_ids, $gold_ids ) )   return 'gold';
    if ( array_intersect( $active_ids, $silver_ids ) )  return 'silver';
    if ( array_intersect( $active_ids, $insider_ids ) ) return 'insider';

    return 'unsubscribed';
}


function cougconnect_verify_html( string $type, string $message ): string {
    $icon  = $type === 'success' ? '✅' : '❌';
    $color = $type === 'success' ? '#1a56db' : '#c81e1e';
    return "
    <div style='max-width:480px;margin:40px auto;padding:32px;border-radius:12px;
                background:#f9fafb;border:1px solid #e5e7eb;text-align:center;font-family:sans-serif;'>
        <div style='font-size:48px;margin-bottom:16px;'>{$icon}</div>
        <h2 style='color:{$color};margin-bottom:12px;'>
            " . ( $type === 'success' ? 'Verified!' : 'Verification Failed' ) . "
        </h2>
        <p style='color:#374151;font-size:16px;line-height:1.6;'>{$message}</p>
        <p style='margin-top:24px;'>
            <a href='https://cougconnect.com' style='color:#1a56db;text-decoration:none;'>← Back to CougConnect</a>
        </p>
    </div>";
}
