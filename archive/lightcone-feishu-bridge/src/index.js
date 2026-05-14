import 'dotenv/config';
import express from 'express';
import { setupFeishuBridge } from './bridge.js';

const PORT            = process.env.PORT            ?? 8778;
const LIGHTCONE_URL   = process.env.LIGHTCONE_URL   ?? 'http://localhost:8779';
const LIGHTCONE_TOKEN = process.env.LIGHTCONE_TOKEN ?? 'demo-token';

const app = express();
app.use(express.json());

setupFeishuBridge(app, { lightconeUrl: LIGHTCONE_URL, lightconeToken: LIGHTCONE_TOKEN });

app.listen(PORT, async () => {
  console.log(`[Bridge] Feishu bridge running on port ${PORT}`);
  console.log(`[Bridge] lightcone: ${LIGHTCONE_URL}`);
  console.log(`[Bridge] Webhook pattern: http://<host>:${PORT}/webhook/<agentId>`);
});
