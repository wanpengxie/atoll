// X (Twitter) API v2 adapter

const BASE = 'https://api.twitter.com/2';

function authHeader() {
  // OAuth 1.0a User Context (required for posting on behalf of a user)
  const { X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET } = process.env;
  if (!X_API_KEY || !X_API_SECRET || !X_ACCESS_TOKEN || !X_ACCESS_SECRET) {
    throw new Error('X credentials not configured. Required env vars: X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET');
  }
  // Build OAuth 1.0a header
  return buildOAuth1Header('POST', `${BASE}/tweets`, {}, {
    apiKey: X_API_KEY,
    apiSecret: X_API_SECRET,
    accessToken: X_ACCESS_TOKEN,
    accessSecret: X_ACCESS_SECRET,
  });
}

// Minimal OAuth 1.0a signer (no external deps)
function buildOAuth1Header(method, url, params, keys) {
  const { createHmac } = await import('crypto').catch(() => { throw new Error('crypto not available'); });
  const nonce = Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
  const ts = Math.floor(Date.now() / 1000).toString();

  const oauthParams = {
    oauth_consumer_key: keys.apiKey,
    oauth_nonce: nonce,
    oauth_signature_method: 'HMAC-SHA1',
    oauth_timestamp: ts,
    oauth_token: keys.accessToken,
    oauth_version: '1.0',
  };

  const allParams = { ...params, ...oauthParams };
  const sortedKeys = Object.keys(allParams).sort();
  const paramStr = sortedKeys.map(k => `${encodeURIComponent(k)}=${encodeURIComponent(allParams[k])}`).join('&');
  const baseStr = [method.toUpperCase(), encodeURIComponent(url), encodeURIComponent(paramStr)].join('&');
  const sigKey = `${encodeURIComponent(keys.apiSecret)}&${encodeURIComponent(keys.accessSecret)}`;
  const sig = createHmac('sha1', sigKey).update(baseStr).digest('base64');

  oauthParams.oauth_signature = sig;
  const headerVal = 'OAuth ' + Object.keys(oauthParams).sort().map(
    k => `${encodeURIComponent(k)}="${encodeURIComponent(oauthParams[k])}"`
  ).join(', ');
  return headerVal;
}

export async function post({ content, media_urls }) {
  const { X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET } = process.env;
  if (!X_API_KEY || !X_API_SECRET || !X_ACCESS_TOKEN || !X_ACCESS_SECRET) {
    throw new Error('X credentials not configured. Required: X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET');
  }

  const { createHmac } = await import('crypto');
  const nonce = Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);
  const ts = Math.floor(Date.now() / 1000).toString();
  const tweetUrl = `${BASE}/tweets`;

  const oauthParams = {
    oauth_consumer_key: X_API_KEY,
    oauth_nonce: nonce,
    oauth_signature_method: 'HMAC-SHA1',
    oauth_timestamp: ts,
    oauth_token: X_ACCESS_TOKEN,
    oauth_version: '1.0',
  };

  const sortedKeys = Object.keys(oauthParams).sort();
  const paramStr = sortedKeys.map(k => `${encodeURIComponent(k)}=${encodeURIComponent(oauthParams[k])}`).join('&');
  const baseStr = ['POST', encodeURIComponent(tweetUrl), encodeURIComponent(paramStr)].join('&');
  const sigKey = `${encodeURIComponent(X_API_SECRET)}&${encodeURIComponent(X_ACCESS_SECRET)}`;
  const sig = createHmac('sha1', sigKey).update(baseStr).digest('base64');
  oauthParams.oauth_signature = sig;

  const authorizationHeader = 'OAuth ' + Object.keys(oauthParams).sort().map(
    k => `${encodeURIComponent(k)}="${encodeURIComponent(oauthParams[k])}"`
  ).join(', ');

  const body = { text: content };
  const res = await fetch(tweetUrl, {
    method: 'POST',
    headers: {
      'Authorization': authorizationHeader,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });

  const json = await res.json();
  if (!res.ok) throw new Error(`X API error ${res.status}: ${JSON.stringify(json)}`);
  return { tweetId: json.data?.id, tweetUrl: `https://x.com/i/web/status/${json.data?.id}` };
}

export async function getAccount() {
  const { X_ACCESS_TOKEN } = process.env;
  if (!X_ACCESS_TOKEN) throw new Error('X_ACCESS_TOKEN not set');
  const res = await fetch(`${BASE}/users/me`, {
    headers: { 'Authorization': `Bearer ${X_ACCESS_TOKEN}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`X API error ${res.status}: ${JSON.stringify(json)}`);
  return { id: json.data?.id, username: json.data?.username, name: json.data?.name };
}
