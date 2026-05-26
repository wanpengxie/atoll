import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import {
  isExtensionAvailable,
  extensionUnavailableReason,
  getDeviceInfo,
  setDeviceToken,
  unbindDevice,
  defaultServerWsUrl,
  describeReason,
} from '../extension.js';

export default function DeviceBind({ channelID, me }) {
  const [bind, setBind] = useState(null); // { actor_id, channel_id, user_id }
  const [status, setStatus] = useState('');
  const [busy, setBusy] = useState(false);
  const [available, setAvailable] = useState(false);

  useEffect(() => {
    const avail = isExtensionAvailable();
    setAvailable(avail);
    setBind(null);
    setStatus('');
    if (!channelID || !avail) return;
    let alive = true;
    (async () => {
      try {
        const info = await getDeviceInfo();
        if (!alive) return;
        if (info.status === 'ok' && info.bound && info.bound.channel_id === channelID && info.bound.actor_id) {
          // Extension self-reports a bind, but cross-check server: the
          // actor token might have been revoked / wiped server-side while
          // the extension was offline. Only restore "bound" UI when
          // server confirms it's still ours.
          try {
            const server = await api.getDeviceActor?.(channelID, info.bound.actor_id);
            if (!alive) return;
            if (server && server.actor_id === info.bound.actor_id) {
              setBind({
                actor_id: info.bound.actor_id,
                channel_id: info.bound.channel_id,
                user_id: info.bound.user_id,
              });
              return;
            }
          } catch (_) {}
          // Server says no — silently tell extension to forget its
          // stale bind so the next bindFlow starts clean.
          try { await unbindDevice(); } catch (_) {}
        }
      } catch (_) {}
    })();
    return () => { alive = false; };
  }, [channelID]);

  async function bindFlow() {
    setBusy(true);
    setStatus('');
    try {
      // Force-clear any stale token / WS state inside the extension
      // background before issuing a fresh actor token — fixes the case where
      // a previously revoked actor_id is being retried in a loop.
      try { await unbindDevice(); } catch (_) {}
      const info = await getDeviceInfo();
      if (info.status !== 'ok') {
        setStatus(describeReason(info.reason) || '获取 device_id 失败');
        return;
      }
      const placement = await api.getPlacement(channelID);
      const daemonID = placement.daemon_id || placement.DaemonID;
      if (!daemonID || (placement.state && placement.state !== 'active')) {
        setStatus('channel 还没绑定到 daemon');
        return;
      }
      const registration = await api.registerDeviceActor(channelID, {
        device_id: info.device_id,
        daemon_id: daemonID,
        device_type: 'xhs.chrome_extension',
      });
      const resp = await setDeviceToken({
        token: registration.token,
        server_ws_url: defaultServerWsUrl(),
        actor_id: registration.actor_id,
        channel_id: channelID,
        user_id: me.id,
        device_id: info.device_id,
        expires_at: registration.expires_at,
      });
      if (resp.status === 'connected') {
        setBind({
          actor_id: registration.actor_id,
          channel_id: channelID,
          user_id: me.id,
        });
        setStatus('✅ 已绑定');
      } else {
        setStatus(describeReason(resp.reason) || `绑定失败：${resp.status}`);
      }
    } catch (err) {
      setStatus(err.message || String(err));
    } finally {
      setBusy(false);
    }
  }

  async function unbindFlow() {
    if (!bind) return;
    setBusy(true);
    try {
      // Server-side revoke is best-effort — actor token may already be
      // gone (manual cleanup, daemon restart, etc). Either way, we MUST
      // clear the extension-side state so the UI / WS reconnect loop
      // doesn't keep retrying a dead actor_id.
      try { await api.revokeDeviceActor(channelID, bind.actor_id); } catch (_) {}
      await unbindDevice();
      setBind(null);
      setStatus('已解绑');
    } catch (err) {
      setStatus(err.message || String(err));
    } finally {
      setBusy(false);
    }
  }

  if (!available) {
    return (
      <span className="device-bind muted" title={extensionUnavailableReason() || ''}>
        先安装 xhs extension，并启动本机 proxy daemon ·{' '}
        <a href="/downloads/coagent-extension.zip" download>
          下载
        </a>
      </span>
    );
  }

  return (
    <div className="device-bind">
      <span className="muted device-bind-guide">
        先启动 coagent-proxy，再在扩展 popup 连接本机 proxy daemon
      </span>
      {bind ? (
        <>
          <span className="muted">legacy direct 已绑定</span>
          <button type="button" className="ghost" onClick={unbindFlow} disabled={busy}>
            解绑
          </button>
        </>
      ) : (
        <button type="button" onClick={bindFlow} disabled={busy || !channelID}>
          {busy ? '…' : 'Legacy direct 绑定'}
        </button>
      )}
      {status && <span className="muted device-bind-status">{status}</span>}
      <a
        href="/downloads/coagent-extension.zip"
        download
        className="muted"
        style={{ marginLeft: 8, fontSize: 12 }}
      >
        下载插件
      </a>
    </div>
  );
}
