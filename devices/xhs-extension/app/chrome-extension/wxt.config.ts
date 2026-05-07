import { defineConfig } from 'wxt';
import { viteStaticCopy } from 'vite-plugin-static-copy';
import { resolve } from 'path';

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
            drop_console: true, // 移除所有 console
            drop_debugger: true, // 移除 debugger
            pure_funcs: ['console.log', 'console.info', 'console.debug', 'console.warn'],
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
        'xiaohongshu-mcp-shared': resolve(__dirname, '../../packages/shared/src'),
      },
    },
  }),
});
