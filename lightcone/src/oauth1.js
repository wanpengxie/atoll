/**
 * Minimal OAuth 1.0a implementation (no external dependencies).
 * Supports the three-legged flow used by X (Twitter).
 */
import { createHmac } from 'crypto';

function pct(s) {
  return encodeURIComponent(String(s)).replace(/[!'()*]/g, c => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

function buildBaseString(method, url, params) {
  const sorted = Object.keys(params).sort().map(k => `${pct(k)}=${pct(params[k])}`).join('&');
  return [method.toUpperCase(), pct(url), pct(sorted)].join('&');
}

function buildHeader(oauthParams) {
  return 'OAuth ' + Object.keys(oauthParams).sort()
    .map(k => `${pct(k)}="${pct(oauthParams[k])}"`)
    .join(', ');
}

function sign(baseStr, consumerSecret, tokenSecret = '') {
  const key = `${pct(consumerSecret)}&${pct(tokenSecret)}`;
  return createHmac('sha1', key).update(baseStr).digest('base64');
}

function baseOAuthParams(consumerKey) {
  return {
    oauth_consumer_key:     consumerKey,
    oauth_nonce:            Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2),
    oauth_signature_method: 'HMAC-SHA1',
    oauth_timestamp:        String(Math.floor(Date.now() / 1000)),
    oauth_version:          '1.0',
  };
}

/**
 * Step 1 — get a temporary request token from the provider.
 * Returns { oauth_token, oauth_token_secret }
 */
export async function getRequestToken(consumerKey, consumerSecret, callbackUrl, requestTokenUrl) {
  const params = {
    ...baseOAuthParams(consumerKey),
    oauth_callback: callbackUrl,
  };
  const base = buildBaseString('POST', requestTokenUrl, params);
  params.oauth_signature = sign(base, consumerSecret);

  const res = await fetch(requestTokenUrl, {
    method: 'POST',
    headers: { Authorization: buildHeader(params) },
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Request token failed (${res.status}): ${body}`);
  }
  const body = await res.text();
  return Object.fromEntries(new URLSearchParams(body));
}

/**
 * Step 3 — exchange the verifier for a permanent access token.
 * Returns { oauth_token, oauth_token_secret, user_id, screen_name, ... }
 */
export async function getAccessToken(consumerKey, consumerSecret, requestToken, requestTokenSecret, verifier, accessTokenUrl) {
  const params = {
    ...baseOAuthParams(consumerKey),
    oauth_token:    requestToken,
    oauth_verifier: verifier,
  };
  const base = buildBaseString('POST', accessTokenUrl, params);
  params.oauth_signature = sign(base, consumerSecret, requestTokenSecret);

  const res = await fetch(accessTokenUrl, {
    method: 'POST',
    headers: { Authorization: buildHeader(params) },
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Access token failed (${res.status}): ${body}`);
  }
  const body = await res.text();
  return Object.fromEntries(new URLSearchParams(body));
}
