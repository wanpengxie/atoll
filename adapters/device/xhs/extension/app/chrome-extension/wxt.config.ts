import { defineConfig } from 'wxt';
import { viteStaticCopy } from 'vite-plugin-static-copy';
import { resolve } from 'path';
import { resolveExternallyConnectableMatches, resolveManifestKey } from './wxt-helpers';

//
// T148 (M1.6-T6) + M1.6-T7 phase-3: externally_connectable.matches
// allowlist + optional unpacked-mode `key` field.
//
// The resolver functions live in ./wxt-helpers.ts so vitest can test
// them in isolation; full behaviour + security rationale documented
// in that file's JSDoc.
//
// Quick reference for the four env knobs:
//
//   COAGENT_WEB_ORIGINS        — comma-separated full chrome match
//                                patterns (M1.6-T6 legacy form).
//   COAGENT_WEB_DOMAIN         — single canonical https host expanded
//                                to https://${domain}/* + dev defaults
//                                (M1.6-T7 acceptance C1).
//   COAGENT_EXTENSION_KEY      — base64 RSA pubkey for manifest.key,
//                                pins unpacked extension ID across reloads.
//   COAGENT_EXTENSION_KEY_FILE — path to a file containing the base64
//                                key string (local dev convenience).
//
// Two layers of defense for the allowlist:
//   1. Manifest field below. Chrome only routes messages from these
//      origins to our chrome.runtime.onMessageExternal listener.
//   2. entrypoints/background/external-bind.ts::isAllowedSenderOrigin
//      re-validates sender.origin against the same list (manifest is
//      the runtime source of truth — see background/index.ts).
//
const manifestKey = resolveManifestKey(process.env as NodeJS.ProcessEnv);
const externallyConnectableMatches = resolveExternallyConnectableMatches(
  process.env as NodeJS.ProcessEnv,
);

export default defineConfig({
  modules: ['@wxt-dev/module-vue'],

  outDir: 'dist',

  zip: {
    enabled: true,
    artifactTemplate: '{{name}}-{{version}}.zip',
  },

  runner: {
    disabled: true, // 禁用自动启动浏览器
  },

  manifest: {
    name: 'Coagent · 小红书 Device',
    description: 'Coagent daemon 的小红书 device 端：长连 daemon WS，承载 publish / search / get-note 等命令',
    version: '1.1.0',

    // M1.6-T7 phase-3: optional stable extension ID for unpacked installs.
    // Resolved from COAGENT_EXTENSION_KEY / COAGENT_EXTENSION_KEY_FILE.
    // Omitted entirely (the `as any` cast accommodates wxt's manifest
    // typing) when no key is configured, preserving Chrome Web Store
    // behaviour for the distributed build.
    ...(manifestKey ? ({ key: manifestKey } as any) : {}),

    // T148 (M1.6-T6): allow the coagent web UI to inject device actor
    // tokens via chrome.runtime.sendMessage. See the comment block above
    // for the security model.
    externally_connectable: {
      matches: externallyConnectableMatches,
    },

    icons: {
      16: 'icon-16.png',
      32: 'icon-32.png',
      48: 'icon-48.png',
      128: 'icon-128.png',
    },

    permissions: [
      'nativeMessaging',
      'tabs',
      'activeTab',
      'scripting',
      'storage',
      'webRequest',
      'webNavigation',
      'downloads',
      'cookies',
      'debugger',
    ],

    host_permissions: [
      // 小红书
      'https://*.xiaohongshu.com/*',
      'https://www.xiaohongshu.com/*',
      'https://creator.xiaohongshu.com/*',
      // 新红数据
      'https://xh.newrank.cn/*',
      // 抖音
      'https://*.douyin.com/*',
      'https://creator.douyin.com/*',
      // 本地开发
      'http://127.0.0.1/*',
      'http://localhost/*',
    ],

    web_accessible_resources: [
      {
        resources: ['/inject-scripts/*'],
        matches: ['<all_urls>'],
      },
    ],
  },

  vite: (env) => ({
    plugins: [
      viteStaticCopy({
        targets: [
          {
            src: 'inject-scripts/*.js',
            dest: 'inject-scripts',
          },
          {
            src: 'assets/icons/*',
            dest: 'assets/icons',
          },
        ],
      }) as any,
      // 生产环境使用 Terser 进行代码压缩和混淆（见下方 terserOptions 配置）
    ],

    build: {
      target: 'es2015',
      sourcemap: env.mode !== 'production', // 生产环境禁用 sourcemap
      minify: env.mode === 'production' ? 'terser' : false, // 生产环境启用 Terser 压缩
      reportCompressedSize: false,
      chunkSizeWarningLimit: 1500,
      // Terser 压缩选项（仅生产环境）
      ...(env.mode === 'production' && {
        terserOptions: {
          compress: {
            // M1.1 Fix-T4 §3: 保留 console.error / console.warn（故障期可观测）。
            // terser ≥ 5.7 支持 drop_console 数组形式，仅 drop 列出的 console 方法。
            drop_console: ['log', 'info', 'debug'],
            drop_debugger: true, // 移除 debugger
            // pure_funcs 与 drop_console 配合，进一步标记副作用安全；
            // 同样不再列入 console.warn / console.error，让它们留在生产包里。
            pure_funcs: ['console.log', 'console.info', 'console.debug'],
            // 启用更多压缩选项
            passes: 2, // 多次压缩以获得更好的效果
            unsafe: false, // 不使用不安全的优化
          },
          mangle: {
            safari10: true, // 兼容 Safari 10
            // 保留浏览器 API 和 DOM 相关的关键字
            reserved: [
              // Chrome Extension APIs
              'chrome',
              'browser',
              'tabs',
              'scripting',
              'webNavigation',
              'webRequest',
              'runtime',
              'storage',

              // DOM APIs
              'window',
              'document',
              'navigator',
              'location',

              // 文件和数据处理 APIs (关键!)
              'DataTransfer',
              'File',
              'Blob',
              'FileReader',
              'FormData',
              'Uint8Array',
              'ArrayBuffer',

              // DOM 操作 APIs
              'Element',
              'HTMLElement',
              'HTMLInputElement',
              'Event',
              'CustomEvent',
              'MutationObserver',

              // 常用 DOM 方法名（作为字符串参数）
              'querySelector',
              'querySelectorAll',
              'getElementById',
              'getElementsByClassName',
              'addEventListener',
              'removeEventListener',
              'dispatchEvent',
              'getAttribute',
              'setAttribute',
              'appendChild',
              'removeChild',

              // 小红书相关（页面状态）
              '__INITIAL_STATE__',
              'userInfo',
              'nickname',
              'userId',
            ],
          },
          format: {
            comments: false, // 移除所有注释
          },
        },
      }),
    },

    resolve: {
      alias: {
        '@': resolve(__dirname, './'),
        'coagent-xhs-shared': resolve(__dirname, '../../packages/shared/src'),
      },
    },
  }),
});
