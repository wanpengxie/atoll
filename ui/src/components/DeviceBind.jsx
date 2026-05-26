import React from 'react';

export default function DeviceBind() {
  return (
    <div className="device-bind">
      <span className="muted device-bind-guide">
        启动 coagent-proxy 后，在 xhs extension popup 连接本机 proxy daemon
      </span>
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
