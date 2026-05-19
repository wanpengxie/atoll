import React, { useEffect, useState } from 'react';
import { api, APIError } from './api.js';
import Auth from './components/Auth.jsx';
import Sidebar from './components/Sidebar.jsx';
import Chat from './components/Chat.jsx';

export default function App() {
  const [me, setMe] = useState(null);
  const [booting, setBooting] = useState(true);
  const [workspaces, setWorkspaces] = useState([]);
  const [activeWorkspaceID, setActiveWorkspaceID] = useState(null);
  const [channels, setChannels] = useState([]);
  const [activeChannelID, setActiveChannelID] = useState(null);

  // Boot: try to restore session via cookie.
  useEffect(() => {
    (async () => {
      try {
        const u = await api.me();
        setMe(u.user || u);
      } catch (_) {
        // not logged in — fall through to auth view
      } finally {
        setBooting(false);
      }
    })();
  }, []);

  // After login, fetch workspaces + auto-select first.
  useEffect(() => {
    if (!me) {
      setWorkspaces([]);
      setActiveWorkspaceID(null);
      setChannels([]);
      setActiveChannelID(null);
      return;
    }
    (async () => {
      try {
        const res = await api.listWorkspaces();
        const list = res.workspaces || [];
        setWorkspaces(list);
        if (list.length > 0) setActiveWorkspaceID(list[0].id || list[0].ID);
      } catch (err) {
        console.warn('listWorkspaces failed', err);
      }
    })();
  }, [me]);

  // On workspace change → fetch channels.
  useEffect(() => {
    if (!activeWorkspaceID) {
      setChannels([]);
      setActiveChannelID(null);
      return;
    }
    (async () => {
      try {
        const res = await api.listChannels(activeWorkspaceID);
        const list = res.channels || [];
        setChannels(list);
        if (list.length > 0) setActiveChannelID(list[0].id || list[0].ID);
        else setActiveChannelID(null);
      } catch (err) {
        console.warn('listChannels failed', err);
      }
    })();
  }, [activeWorkspaceID]);

  async function refreshWorkspaces() {
    const res = await api.listWorkspaces();
    setWorkspaces(res.workspaces || []);
  }
  async function refreshChannels() {
    if (!activeWorkspaceID) return;
    const res = await api.listChannels(activeWorkspaceID);
    setChannels(res.channels || []);
  }

  async function handleLogout() {
    try {
      await api.logout();
    } catch (_) {}
    setMe(null);
  }

  if (booting) {
    return <div className="boot">载入中…</div>;
  }

  if (!me) {
    return <Auth onAuthed={setMe} />;
  }

  return (
    <>
      <div className="rainbow-bar"></div>
      <div id="app">
        <Sidebar
          me={me}
          workspaces={workspaces}
          activeWorkspaceID={activeWorkspaceID}
          channels={channels}
          activeChannelID={activeChannelID}
          onSelectWorkspace={setActiveWorkspaceID}
          onSelectChannel={setActiveChannelID}
          onCreateWorkspace={async (name) => {
            await api.createWorkspace(name);
            await refreshWorkspaces();
          }}
          onCreateChannel={async (name, type) => {
            const res = await api.createChannel(activeWorkspaceID, name, type);
            const ch = res.channel || res;
            // M1.6 — newly created channel has no placement yet; bind it
            // to the dev daemon so message writes can flow. Production
            // would let the user pick a daemon; in single-daemon dev mode
            // we just hardcode "daemon-dev" (matches cmd/daemon -daemon-id).
            try {
              await api.bindChannel(activeWorkspaceID, ch.id || ch.ID, 'daemon-dev');
            } catch (err) {
              console.warn('bindChannel failed', err);
            }
            await refreshChannels();
          }}
          onLogout={handleLogout}
        />
        <Chat
          channelID={activeChannelID}
          channel={channels.find((c) => (c.id || c.ID) === activeChannelID)}
          me={me}
        />
      </div>
    </>
  );
}
