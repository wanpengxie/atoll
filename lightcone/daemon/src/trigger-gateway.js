const PASS_TYPES = new Set([
  'user.message.posted',
  'user.message.mention',
  'sub_agent.report',
  'worker.completed',
  'worker.failed',
  'channel.member.joined',
  'channel.config.updated',
  'cron.tick',
]);

const ALWAYS_BLOCK = new Set(['heartbeat']);
const BLOCK_PREFIXES = ['health.', 'metric.', 'sync.', 'audit.', 'system.'];

export class TriggerGateway {
  constructor({ onPass, onBlock }) {
    this.onPass = onPass;
    this.onBlock = onBlock;
  }

  evaluate(event) {
    const type = String(event?.type ?? '').trim();
    if (!type) {
      return { decision: 'block', reason: 'missing_event_type' };
    }

    if (PASS_TYPES.has(type)) {
      return { decision: 'pass', reason: 'default_ruleset_pass' };
    }

    if (ALWAYS_BLOCK.has(type) || BLOCK_PREFIXES.some((prefix) => type.startsWith(prefix))) {
      return { decision: 'block', reason: 'default_ruleset_noise' };
    }

    return { decision: 'block', reason: 'default_ruleset_unknown' };
  }

  async dispatch({ channel, event }) {
    const outcome = this.evaluate(event);

    if (outcome.decision === 'pass') {
      await this.onPass(channel, event, outcome);
      return outcome;
    }

    await this.onBlock(channel, event, outcome);
    return outcome;
  }
}
