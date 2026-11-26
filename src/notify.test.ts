import { makeTestLogger } from '@yieldkit/logger/testing';
import { Effect } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import { settleNotify, validateNotifyChannel, validateNotifyPayload } from './notify.ts';
import { MAX_NOTIFY_PAYLOAD_BYTES } from './policy.ts';
import { logMessages } from './testing/index.ts';

describe('validateNotifyChannel', () => {
  it('accepts a postgres identifier and rejects spaces and an empty channel', async () => {
    const channel = await Effect.runPromise(validateNotifyChannel('app_events'));
    expect(channel).toBe('app_events');

    const spaced = await Effect.runPromise(Effect.flip(validateNotifyChannel('app events')));
    expect(spaced._tag).toBe('PgChannelError');
    expect(spaced.channel).toBe('app events');

    const empty = await Effect.runPromise(Effect.flip(validateNotifyChannel('')));
    expect(empty.message).toContain('1 to 63');
  });
});

describe('validateNotifyPayload', () => {
  it('accepts an empty payload and rejects one byte over the notify limit', async () => {
    const payload = await Effect.runPromise(validateNotifyPayload('app_events', ''));
    expect(payload).toBe('');

    const oversized = 'x'.repeat(MAX_NOTIFY_PAYLOAD_BYTES + 1);
    const error = await Effect.runPromise(
      Effect.flip(validateNotifyPayload('app_events', oversized)),
    );
    expect(error._tag).toBe('PgChannelError');
    expect(error.message).toContain(String(MAX_NOTIFY_PAYLOAD_BYTES));
  });

  it('accepts 7999 bytes and rejects exactly 8000, which PostgreSQL refuses', async () => {
    const largest = 'x'.repeat(MAX_NOTIFY_PAYLOAD_BYTES - 1);
    expect(await Effect.runPromise(validateNotifyPayload('app_events', largest))).toBe(largest);

    const atLimit = 'x'.repeat(MAX_NOTIFY_PAYLOAD_BYTES);
    const error = await Effect.runPromise(
      Effect.flip(validateNotifyPayload('app_events', atLimit)),
    );
    expect(error._tag).toBe('PgChannelError');
  });

  it('rejects a payload containing a NUL character', async () => {
    const error = await Effect.runPromise(
      Effect.flip(validateNotifyPayload('app_events', 'before\u0000after')),
    );
    expect(error._tag).toBe('PgChannelError');
    expect(error.message).toContain('NUL');
  });
});

describe('settleNotify', () => {
  it('logs pg.notify.failed and succeeds when delivery fails', async () => {
    const harness = makeTestLogger();
    await Effect.runPromise(
      settleNotify(Effect.fail({ message: 'socket closed' }), 'app_events', 12).pipe(
        Effect.provide(harness.layer),
      ),
    );
    expect(logMessages(harness.records)).toContain('pg.notify.failed');
  });
});
