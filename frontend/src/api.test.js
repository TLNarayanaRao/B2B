import test from 'node:test';
import assert from 'node:assert/strict';
import {api} from './api.js';

test('proxy failures explain the API port instead of hiding the error', async t => {
  t.mock.method(globalThis, 'fetch', async () => new Response('', {status: 500}));
  await assert.rejects(api('connections', {}), /HTTP 500.*RELAY_API_URL.*8010/);
});

test('HTML errors are not displayed as server markup', async t => {
  t.mock.method(globalThis, 'fetch', async () => new Response('<html>failure</html>', {status: 502}));
  await assert.rejects(api('connections', {}), /HTTP 502.*Python API/);
});

test('connector failure detail and validation errors are preserved', async t => {
  t.mock.method(globalThis, 'fetch', async () => Response.json({detail: 'Event stream topic is unavailable'}, {status: 422}));
  await assert.rejects(api('connections', {}), /Event stream topic is unavailable/);
  globalThis.fetch.mock.mockImplementation(async () => Response.json({detail: [{loc: ['body', 'endpoint'], msg: 'Invalid URL', input: 'private-secret'}]}, {status: 422}));
  await assert.rejects(api('connections', {}), {message: 'endpoint: Invalid URL'});
});

test('network and invalid success responses produce actionable errors', async t => {
  t.mock.method(globalThis, 'fetch', async () => {throw new TypeError('fetch failed');});
  await assert.rejects(api('connections'), /Cannot reach the API/);
  globalThis.fetch.mock.mockImplementation(async () => new Response('<html>SPA fallback</html>'));
  await assert.rejects(api('connections'), /invalid JSON.*RELAY_API_URL/);
});

test('GET and configuration PUT keep their request semantics', async t => {
  const fetch = t.mock.method(globalThis, 'fetch', async () => Response.json({success: true}));
  assert.deepEqual(await api('connections'), {success: true});
  assert.equal(fetch.mock.calls[0].arguments[1], undefined);
  await api('connections/id', {config: {topic: 'orders'}}, 'PUT');
  assert.equal(fetch.mock.calls[1].arguments[1].method, 'PUT');
  assert.equal(fetch.mock.calls[1].arguments[1].body, JSON.stringify({config: {topic: 'orders'}}));
});
