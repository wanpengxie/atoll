import {
  ToolResult,
  ExecutionWorld,
  XIAOHONGSHU_TOOL_NAMES,
  ERROR_MESSAGES,
  TIMEOUTS,
} from 'xiaohongshu-mcp-shared';
import { BaseTool } from './base-tool';

type ExecutionCode = string | ((args: Record<string, any>) => any | Promise<any>);

interface InjectScriptArgs {
  code: ExecutionCode;
  world?: ExecutionWorld;
  args?: Record<string, any>;
  tabId?: number;
}

interface InjectedTab {
  tabId: number;
  world: ExecutionWorld;
  injectedAt: number;
}

// Track injected tabs to manage cleanup
const injectedTabs = new Map<number, InjectedTab>();

async function cleanupInjectedScripts(tabId: number): Promise<void> {
  if (!injectedTabs.has(tabId)) {
    return;
  }

  try {
    await chrome.tabs.sendMessage(tabId, { type: 'xiaohongshu-mcp:cleanup' });
  } catch (error) {
    console.warn('Cleanup error:', error);
  } finally {
    injectedTabs.delete(tabId);
  }
}

function serializeExecutionCode(code: ExecutionCode): string {
  if (typeof code === 'function') {
    const fnSource = code.toString();

    // Include necessary helper functions that TypeScript might generate
    return `
      // Helper function for async/await transformation
      const __async = (__this, __arguments, generator) => {
        return new Promise((resolve, reject) => {
          var fulfilled = (value) => {
            try {
              step(generator.next(value));
            } catch (e) {
              reject(e);
            }
          };
          var rejected = (value) => {
            try {
              step(generator.throw(value));
            } catch (e) {
              reject(e);
            }
          };
          var step = (x) => x.done ? resolve(x.value) : Promise.resolve(x.value).then(fulfilled, rejected);
          step((generator = generator.apply(__this, __arguments)).next());
        });
      };

      // Execute the function
      const __fn = ${fnSource};
      return __fn(args);
    `;
  }
  return code;
}

function normalizeScriptArgs(args?: Record<string, any>): Record<string, any> {
  if (args == null) {
    return {};
  }
  if (typeof args !== 'object' || Array.isArray(args)) {
    throw new Error('Script args must be a JSON-serializable object');
  }

  try {
    // chrome.scripting.executeScript requires structured-clone/serializable args.
    // Normalize by JSON round-trip so missing optional args never become unserializable `undefined`.
    const serialized = JSON.stringify(args);
    if (serialized == null) {
      return {};
    }
    const parsed = JSON.parse(serialized);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return {};
    }
    return parsed as Record<string, any>;
  } catch (error) {
    throw new Error(
      `Script args must be JSON-serializable: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

async function injectMainWorldScript(
  tabId: number,
  code: ExecutionCode,
  args?: Record<string, any>,
  timeoutMs?: number
): Promise<any> {
  const serializedCode = serializeExecutionCode(code);
  const normalizedArgs = normalizeScriptArgs(args);

  await chrome.scripting.executeScript({
    target: { tabId },
    files: ['inject-scripts/inject-bridge.js'],
    world: 'ISOLATED' as any,
  });

  await chrome.scripting.executeScript({
    target: { tabId },
    files: ['inject-scripts/main-world-executor.js'],
    world: 'MAIN' as any,
  });

  return await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('Script execution timeout'));
    }, timeoutMs || TIMEOUTS.SCRIPT_INJECTION);

    chrome.tabs.sendMessage(
      tabId,
      {
        type: 'EXECUTE_SCRIPT',
        targetWorld: 'MAIN',
        action: 'EXECUTE_CODE',
        code: serializedCode,
        args: normalizedArgs,
      },
      (response) => {
        clearTimeout(timeout);

        if (chrome.runtime.lastError) {
          reject(new Error(chrome.runtime.lastError.message));
          return;
        }

        if (!response) {
          resolve(undefined);
          return;
        }

        if (response.error) {
          reject(new Error(response.error.message || 'Execution failed'));
          return;
        }

        const payload = response.data;
        if (!payload) {
          resolve(undefined);
          return;
        }

        if (payload.success === false) {
          const errorInfo = payload.error || {};
          const error = new Error(errorInfo.message || 'Execution failed');
          if (errorInfo.name) {
            error.name = errorInfo.name;
          }
          if (errorInfo.stack) {
            error.stack = errorInfo.stack;
          }
          reject(error);
          return;
        }

        resolve(payload.result);
      }
    );
  });
}

async function injectIsolatedScript(
  tabId: number,
  code: ExecutionCode,
  args?: Record<string, any>
): Promise<any> {
  const serializedCode = serializeExecutionCode(code);
  const normalizedArgs = normalizeScriptArgs(args);

  const results = await chrome.scripting.executeScript({
    target: { tabId },
    func: (codeString: string, funcArgs: any) => {
      const fn = new Function('args', codeString);
      return fn(funcArgs);
    },
    args: [serializedCode, normalizedArgs],
    world: 'ISOLATED' as any,
  });

  return results[0]?.result;
}

export async function runInMainWorld<T = any>(
  tabId: number,
  code: ExecutionCode,
  args?: Record<string, any>,
  timeoutMs?: number
): Promise<T> {
  await cleanupInjectedScripts(tabId);
  const result = await injectMainWorldScript(tabId, code, args, timeoutMs);
  injectedTabs.set(tabId, {
    tabId,
    world: ExecutionWorld.MAIN,
    injectedAt: Date.now(),
  });
  return result as T;
}

async function runInIsolatedWorld<T = any>(
  tabId: number,
  code: ExecutionCode,
  args?: Record<string, any>
): Promise<T> {
  await cleanupInjectedScripts(tabId);
  const result = await injectIsolatedScript(tabId, code, args);
  injectedTabs.set(tabId, {
    tabId,
    world: ExecutionWorld.ISOLATED,
    injectedAt: Date.now(),
  });
  return result as T;
}

/**
 * Tool for injecting and executing JavaScript in web pages
 */
export class InjectScriptTool extends BaseTool {
  name = XIAOHONGSHU_TOOL_NAMES.INJECT_SCRIPT;

  async execute(args: InjectScriptArgs): Promise<ToolResult> {
    try {
      const { code, world = ExecutionWorld.MAIN, args: scriptArgs, tabId: specifiedTabId } = args;

      if (!code) {
        return this.createErrorResult(ERROR_MESSAGES.INVALID_ARGUMENTS + ': Code is required');
      }

      // Get active tab
      let tabId = specifiedTabId;
      if (typeof tabId !== 'number') {
        const tab = await this.getActiveTab();
        if (!tab || !tab.id) {
          return this.createErrorResult(ERROR_MESSAGES.PAGE_LOAD_TIMEOUT);
        }
        tabId = tab.id;
      }

      if (typeof tabId !== 'number') {
        return this.createErrorResult(ERROR_MESSAGES.PAGE_LOAD_TIMEOUT);
      }

      // Inject based on execution world
      let result;
      if (world === ExecutionWorld.MAIN) {
        result = await runInMainWorld(tabId, code, scriptArgs);
      } else {
        result = await runInIsolatedWorld(tabId, code, scriptArgs);
      }

      const serializedResult = this.serializeResult(result);

      return {
        content: [
          {
            type: 'text',
            text: serializedResult,
          },
        ],
        isError: false,
      };
    } catch (error) {
      console.error('Error in InjectScriptTool:', error);
      return this.createErrorResult(
        `${ERROR_MESSAGES.TOOL_EXECUTION_FAILED}: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  private serializeResult(result: any): string {
    try {
      return JSON.stringify(result ?? null);
    } catch (error) {
      try {
        return JSON.stringify(String(result));
      } catch {
        return 'null';
      }
    }
  }
}

/**
 * Tool for reading page data through window object
 */
export class ReadPageDataTool extends BaseTool {
  name = XIAOHONGSHU_TOOL_NAMES.READ_PAGE_DATA;
  private injectTool = new InjectScriptTool();

  async execute(args: { path: string }): Promise<ToolResult> {
    try {
      const { path } = args;

      if (!path) {
        return this.createErrorResult(ERROR_MESSAGES.INVALID_ARGUMENTS + ': Path is required');
      }

      // Use inject tool to read window property
      const result = await this.injectTool.execute({
        code: `
          let value = window;
          const keys = args.path.split('.');

          for (const key of keys) {
            if (key && value != null) {
              value = value[key];
            } else {
              return undefined;
            }
          }

          // Handle circular references and functions
          try {
            return JSON.parse(JSON.stringify(value));
          } catch (e) {
            return String(value);
          }
        `,
        world: ExecutionWorld.MAIN,
        args: { path },
        tabId: (await this.getActiveTab())?.id,
      });

      if (result.isError) {
        const errorMessage = result.content.find((item) => item.type === 'error')?.text;
        return this.createErrorResult(
          `${ERROR_MESSAGES.TOOL_EXECUTION_FAILED}: ${errorMessage || 'Failed to read page data'}`
        );
      }

      const firstItem = result.content[0];
      if (!firstItem || firstItem.type !== 'text') {
        return this.createErrorResult('Invalid response from page script');
      }

      let parsedData: unknown;
      try {
        parsedData = JSON.parse(firstItem.text ?? 'null');
      } catch (error) {
        return this.createErrorResult(
          `${ERROR_MESSAGES.TOOL_EXECUTION_FAILED}: ${(error as Error).message || 'Unable to parse result'}`
        );
      }

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(
              {
                path,
                value: parsedData,
                type: typeof parsedData,
              },
              null,
              2
            ),
          },
        ],
        isError: false,
      };
    } catch (error) {
      console.error('Error in ReadPageDataTool:', error);
      return this.createErrorResult(
        `${ERROR_MESSAGES.TOOL_EXECUTION_FAILED}: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }
}

// Clean up when tabs are closed
chrome.tabs.onRemoved.addListener((tabId) => {
  if (injectedTabs.has(tabId)) {
    injectedTabs.delete(tabId);
    console.log(`Cleaned up injection state for tab ${tabId}`);
  }
});
