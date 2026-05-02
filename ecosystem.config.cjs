const path = require('path');

const root = __dirname;

const common = {
  cwd: root,
  interpreter: 'node',
  exec_mode: 'fork',
  autorestart: true,
  max_restarts: 10,
};

module.exports = {
  apps: [
    {
      ...common,
      name: 'coagent-server',
      script: path.join(root, 'lightcone', 'src', 'index.js'),
    },
    {
      ...common,
      name: 'coagent-daemon',
      script: path.join(root, 'lightcone', 'daemon', 'src', 'index.js'),
    },
  ],
};
