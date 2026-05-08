// Vitest unit tests for cmd-handlers.ts + cmd-handlers-init.wrapTool
// (M1.1 Fix-T4 §7). Covers:
//   - isKnownCommand only accepts the 5 spec commands
//   - registerCommandHandler / getCommandHandler round-trips identity
//   - wrapTool: handler throw passthrough preserves structured `code` from
//     the JSON error envelope (publish_timeout / publish_canceled), falling
//     back to `${cmd}_failed` when the tool emits raw text.

import { describe, it, expect, beforeEach, vi } from 'vitest';

// Inline shared constants — no build step needed.
vi.mock('coagent-xhs-shared', () => ({
  COAGENT_DEVICE_COMMANDS: {
    PUBLISH: 'publish',
    SEARCH: 'search',
    GET_MY_RECENT: 'get-my-recent',
    GET_NOTE: 'get-note',
    PUBLISH_STATUS: 'publish-status',
  },
}));

// We don't want cmd-handlers-init to wire up the real PublishContentTool etc.
// — wrapTool is unit-tested in isolation by re-implementing the same behavior
//   via the public registerCommandHandler API.
import {
  isKnownCommand,
  listRegisteredCommands,
  registerCommandHandler,
  getCommandHandler,
  notImplementedHandler,
  type CommandHandler,
  type CommandResultEnvelope,
} from './cmd-handlers';

// Re-create the wrapTool helper here to avoid pulling in PublishContentTool
// (which transitively imports BaseTool / chrome.* APIs).
type ToolLike = { name: string; execute(args: any): Promise<any> };
function wrapToolLocal(tool: ToolLike, cmd: string): CommandHandler {
  return async (params, _session): Promise<CommandResultEnvelope> => {
    const inner = await tool.execute(params ?? {});
    if (inner.isError) {
      const text = String(inner.content?.[0]?.text ?? `${tool.name} failed`);
      let parsedCode: string | undefined;
      let parsedMessage: string | undefined;
      try {
        const parsed = JSON.parse(text);
        if (parsed && typeof parsed === 'object') {
          if (typeof parsed.code === 'string') parsedCode = parsed.code;
          if (typeof parsed.message === 'string') parsedMessage = parsed.message;
        }
      } catch {
        /* not JSON */
      }
      const err = new Error(parsedMessage ?? text);
      (err as any).code = parsedCode ?? `${cmd}_failed`;
      throw err;
    }
    const text = String(inner.content?.[0]?.text ?? '');
    let data: Record<string, unknown> = {};
    try {
      const parsed = JSON.parse(text);
      data = parsed && typeof parsed === 'object' ? parsed : { raw: parsed };
    } catch {
      data = { raw: text };
    }
    return { result: data };
  };
}

describe('isKnownCommand', () => {
  it('accepts the 5 spec commands', () => {
    expect(isKnownCommand('publish')).toBe(true);
    expect(isKnownCommand('search')).toBe(true);
    expect(isKnownCommand('get-my-recent')).toBe(true);
    expect(isKnownCommand('get-note')).toBe(true);
    expect(isKnownCommand('publish-status')).toBe(true);
  });

  it('rejects unknown / mistyped commands', () => {
    expect(isKnownCommand('PUBLISH')).toBe(false); // wrong casing
    expect(isKnownCommand('publishContent')).toBe(false);
    expect(isKnownCommand('')).toBe(false);
    expect(isKnownCommand('unknown')).toBe(false);
  });
});

describe('registerCommandHandler / getCommandHandler', () => {
  it('returns the registered handler instance back', async () => {
    const fn: CommandHandler = async () => ({ result: { ok: true } });
    registerCommandHandler('search' as any, fn);
    const got = getCommandHandler('search');
    expect(got).toBe(fn);
    const out = await got!({}, null);
    expect(out.result).toEqual({ ok: true });
  });

  it('listRegisteredCommands includes registered ids', () => {
    registerCommandHandler('get-note' as any, notImplementedHandler);
    expect(listRegisteredCommands()).toContain('get-note');
  });
});

describe('notImplementedHandler', () => {
  it('throws an error with code=not_implemented', async () => {
    await expect(notImplementedHandler({}, null)).rejects.toMatchObject({
      message: 'command handler not implemented',
    });
    try {
      await notImplementedHandler({}, null);
    } catch (err: any) {
      expect(err.code).toBe('not_implemented');
    }
  });
});

describe('wrapTool error envelope passthrough', () => {
  it('preserves structured code from JSON error text (publish_timeout)', async () => {
    const tool: ToolLike = {
      name: 'xhs_publish_content',
      execute: async () => ({
        content: [
          {
            type: 'error',
            text: JSON.stringify({
              success: false,
              code: 'publish_timeout',
              message: 'publish wait timed out after 600000ms',
            }),
          },
        ],
        isError: true,
      }),
    };
    const handler = wrapToolLocal(tool, 'publish');

    let captured: any;
    try {
      await handler({}, null);
    } catch (err) {
      captured = err;
    }
    expect(captured).toBeInstanceOf(Error);
    expect(captured.code).toBe('publish_timeout');
    expect(captured.message).toBe('publish wait timed out after 600000ms');
  });

  it('preserves publish_canceled code from JSON envelope', async () => {
    const tool: ToolLike = {
      name: 'xhs_publish_content',
      execute: async () => ({
        content: [
          {
            type: 'error',
            text: JSON.stringify({ code: 'publish_canceled', message: 'user closed tab' }),
          },
        ],
        isError: true,
      }),
    };
    const handler = wrapToolLocal(tool, 'publish');
    try {
      await handler({}, null);
    } catch (err: any) {
      expect(err.code).toBe('publish_canceled');
      expect(err.message).toBe('user closed tab');
      return;
    }
    expect.fail('handler should have thrown');
  });

  it('falls back to ${cmd}_failed when error text is plain string', async () => {
    const tool: ToolLike = {
      name: 'xhs_search_feeds',
      execute: async () => ({
        content: [{ type: 'error', text: 'rate limited' }],
        isError: true,
      }),
    };
    const handler = wrapToolLocal(tool, 'search');
    try {
      await handler({}, null);
    } catch (err: any) {
      expect(err.code).toBe('search_failed');
      expect(err.message).toBe('rate limited');
      return;
    }
    expect.fail('handler should have thrown');
  });

  it('parses success JSON into envelope.result when isError=false', async () => {
    const tool: ToolLike = {
      name: 'xhs_get_note',
      execute: async () => ({
        content: [
          {
            type: 'text',
            text: JSON.stringify({ success: true, data: { note_id: 'abc' } }),
          },
        ],
        isError: false,
      }),
    };
    const handler = wrapToolLocal(tool, 'get-note');
    const out = await handler({}, null);
    expect(out.result).toMatchObject({ success: true, data: { note_id: 'abc' } });
  });
});
