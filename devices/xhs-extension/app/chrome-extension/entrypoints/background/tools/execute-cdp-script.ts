import { ToolResult, ERROR_MESSAGES } from 'xiaohongshu-mcp-shared';
import { BaseTool } from './base-tool';

/**
 * CDP 脚本执行参数
 */
interface ExecuteCdpScriptArgs {
  /** 目标标签页 ID，不传则使用当前活跃小红书 Tab */
  tabId?: number;
  /** 要执行的 JS 脚本字符串 */
  script: string;
  /** 传入脚本的参数对象 */
  args?: Record<string, any>;
  /** 执行超时时间(毫秒)，默认 120000 */
  timeout?: number;
}

/** 默认超时 120 秒 */
const DEFAULT_CDP_TIMEOUT = 120_000;
const tabExecutionLocks = new Map<number, Promise<void>>();

async function withTabExecutionLock<T>(tabId: number, fn: () => Promise<T>): Promise<T> {
  const previous = tabExecutionLocks.get(tabId) ?? Promise.resolve();
  let release: (() => void) | undefined;
  const current = new Promise<void>((resolve) => {
    release = resolve;
  });

  tabExecutionLocks.set(
    tabId,
    previous.then(() => current)
  );
  await previous;

  try {
    return await fn();
  } finally {
    release?.();
    if (tabExecutionLocks.get(tabId) === current) {
      tabExecutionLocks.delete(tabId);
    }
  }
}

/**
 * 通过 CDP Runtime.evaluate 注入执行 JS 脚本的通用工具。
 *
 * 核心流程：chrome.debugger.attach → Runtime.evaluate（IIFE 包装） → detach（finally 保证）
 *
 * 与现有 inject-script 工具（chrome.scripting.executeScript）的区别：
 * - 原生支持 awaitPromise（适合长耗时 async 脚本）
 * - 无消息大小限制（适合下发大型脚本）
 * - 返回完整异常 + 堆栈
 * - 脚本在页面主上下文执行，不能调用 Chrome Extension API
 * - Chrome 会显示"正在被调试"横幅（自动化场景可接受）
 */
export class ExecuteCdpScriptTool extends BaseTool {
  name = 'execute_cdp_script';

  async execute(args: ExecuteCdpScriptArgs): Promise<ToolResult> {
    // ─── 参数校验 ───
    const { script, args: scriptArgs, timeout = DEFAULT_CDP_TIMEOUT } = args || {};
    const topLevelToolCallRequestId =
      args && typeof args === 'object' ? (args as any).__toolCallRequestId : undefined;

    if (!script || typeof script !== 'string') {
      return this.createErrorResult(
        `${ERROR_MESSAGES.INVALID_ARGUMENTS}: script is required and must be a string`
      );
    }

    // ─── 确定目标 Tab ───
    let tabId = args.tabId;
    if (typeof tabId !== 'number') {
      try {
        const tab = await this.createIsolatedXHSTab();
        if (!tab || typeof tab.id !== 'number') {
          return this.createErrorResult('No available tab found');
        }
        tabId = tab.id;
      } catch (err) {
        return this.createErrorResult(
          `Failed to find or create XHS tab: ${err instanceof Error ? err.message : String(err)}`
        );
      }
    }

    if (typeof tabId !== 'number') {
      return this.createErrorResult('No available tab found');
    }

    return withTabExecutionLock(tabId, async () => {
      // ─── 构建 IIFE 表达式 ───
      // 将脚本包装为异步 IIFE，注入 args 参数。
      // websocket-client 会把 __toolCallRequestId 注入在工具参数顶层，这里显式桥接到脚本 args，
      // 供页面脚本在手动点击发布后回传 publish_clicked 事件使用。
      const normalizedScriptArgs =
        scriptArgs && typeof scriptArgs === 'object' && !Array.isArray(scriptArgs)
          ? { ...(scriptArgs as Record<string, any>) }
          : {};
      if (
        typeof topLevelToolCallRequestId === 'string' &&
        topLevelToolCallRequestId.trim() !== '' &&
        typeof normalizedScriptArgs.__toolCallRequestId !== 'string'
      ) {
        normalizedScriptArgs.__toolCallRequestId = topLevelToolCallRequestId.trim();
      }

      const argsJson = JSON.stringify(normalizedScriptArgs);
      const expression = `(async function(__args) {\n${script}\n})(${argsJson})`;

      const debugTarget: chrome.debugger.Debuggee = { tabId };
      let attachedByThisCall = false;

      try {
        // ─── Attach debugger ───
        // 同一 Tab 可能已有其他调试会话，此时借用现有会话并避免 detach 破坏。
        try {
          await chrome.debugger.attach(debugTarget, '1.3');
          attachedByThisCall = true;
        } catch (attachErr) {
          const msg = attachErr instanceof Error ? attachErr.message : String(attachErr);
          if (!this.isAlreadyAttachedError(msg)) {
            return this.createErrorResult(`CDP attach failed: ${msg}`);
          }
          attachedByThisCall = false;
        }

        // ─── Runtime.evaluate with timeout ───
        const evalResult = await this.evaluateViaCdpTimeout(debugTarget, expression, timeout);

        // ─── 处理 evaluate 结果 ───
        if (evalResult.exceptionDetails) {
          const exc = evalResult.exceptionDetails;
          const errText =
            exc.exception?.description ||
            exc.text ||
            'Script execution threw an exception';
          return this.createErrorResult(`Script error: ${errText}`);
        }

        // 提取返回值
        const remoteObject = evalResult.result;
        let resultValue: any;

        if (remoteObject.type === 'undefined') {
          resultValue = undefined;
        } else if (remoteObject.value !== undefined) {
          resultValue = remoteObject.value;
        } else if (remoteObject.description) {
          // 非序列化对象（如 DOM 节点），返回描述
          resultValue = remoteObject.description;
        } else {
          resultValue = null;
        }

        const serialized = this.serializeResult(resultValue);

        return {
          content: [{ type: 'text', text: serialized }],
          isError: false,
        };
      } catch (err) {
        return this.createErrorResult(
          `CDP script execution failed: ${err instanceof Error ? err.message : String(err)}`
        );
      } finally {
        if (attachedByThisCall) {
          try {
            await chrome.debugger.detach(debugTarget);
          } catch {
            // Tab 可能已关闭，忽略 detach 错误
          }
        }
      }
    });
  }

  /**
   * 带超时的 Runtime.evaluate
   *
   * CDP Runtime.evaluate 返回结构：
   * - result: { type, value?, description?, objectId? }  — returnByValue=true 时 value 即为序列化后的值
   * - exceptionDetails?: { text, exception?: { description } }  — 脚本抛异常时存在
   */
  private async evaluateViaCdpTimeout(
    target: chrome.debugger.Debuggee,
    expression: string,
    timeoutMs: number
  ): Promise<{
    result: { type: string; value?: any; description?: string };
    exceptionDetails?: { text?: string; exception?: { description?: string } };
  }> {
    return new Promise((resolve, reject) => {
      chrome.debugger.sendCommand(
        target,
        'Runtime.evaluate',
        {
          expression,
          awaitPromise: true,
          returnByValue: true,
          timeout: timeoutMs,
        },
        (result: any) => {
          if (chrome.runtime.lastError) {
            reject(new Error(chrome.runtime.lastError.message));
            return;
          }
          resolve(result);
        }
      );
    });
  }

  private isAlreadyAttachedError(message: string): boolean {
    const normalized = message.toLowerCase();
    return normalized.includes('already attached') || normalized.includes('another debugger');
  }

  /**
   * 安全序列化返回值
   */
  private serializeResult(result: any): string {
    if (result === undefined) {
      return 'undefined';
    }
    try {
      return JSON.stringify(result);
    } catch {
      try {
        return JSON.stringify(String(result));
      } catch {
        return 'null';
      }
    }
  }
}
