'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { EventEmitter } = require('node:events')
const { mockHttp, captureErrors, settle, syncs, pings } = require('./helpers')
const { monitorBullMQ, VERSION } = require('../dist')

class FakeQueue {
  constructor(name, schedulers) {
    this.name = name
    this.schedulers = schedulers
  }
  async getJobSchedulers() {
    if (this.schedulers instanceof Error) throw this.schedulers
    return this.schedulers
  }
}

class FakeWorker extends EventEmitter {
  constructor(name) {
    super()
    this.name = name
  }
}

function job(id, repeatJobKey, fields = {}) {
  return { id, repeatJobKey, attemptsMade: 0, opts: { attempts: 1 }, ...fields }
}

test('reconciles every Job Scheduler of a queue under its own source', async (t) => {
  const requests = mockHttp(t)
  const errors = captureErrors(t)
  const queue = new FakeQueue('reports', [
    { key: 'nightly-report', name: 'report', pattern: '0 2 * * *' },
    { key: 'Every Five', name: 'tick', pattern: '0 */5 * * * *' },
    { key: 'too-fast', name: 'tick', pattern: '*/10 * * * * *' },
    { key: 'interval', name: 'tick', every: 60000 },
    { key: 'opted-out', name: 'tick', pattern: '0 3 * * *', template: { opts: { cronradar: false } } },
    { key: 'skipped', name: 'tick', pattern: '0 4 * * *' },
    { key: 'weekly', name: 'tick', pattern: '@weekly' },
  ])

  const monitoring = monitorBullMQ({ queues: [queue], skip: ['skipped'] })
  await monitoring.synced
  monitoring.stop()

  const [sync] = syncs(requests)
  assert.equal(sync.method, 'POST')
  assert.equal(sync.headers.Authorization, 'Basic ' + Buffer.from('ck_app_test_secret:').toString('base64'))
  assert.deepEqual(sync.body, {
    source: 'bullmq:reports',
    version: VERSION,
    mode: 'reconcile',
    monitors: [
      { key: 'reports-nightly-report', name: 'nightly-report (reports)', schedule: '0 2 * * *', gracePeriod: 60 },
      { key: 'reports-every-five', name: 'Every Five (reports)', schedule: '*/5 * * * *', gracePeriod: 60 },
      { key: 'reports-weekly', name: 'weekly (reports)', schedule: '@weekly', gracePeriod: 60 },
    ],
  })
  assert.ok(errors.some((line) => line.includes("'too-fast'") && line.includes('seconds field')))
  assert.ok(errors.some((line) => line.includes("'interval'") && line.includes('no cron form')))
})

test('an unreadable queue is not reconciled, so its monitors are not paused', async (t) => {
  const requests = mockHttp(t)
  const errors = captureErrors(t)
  const monitoring = monitorBullMQ({ queues: [new FakeQueue('broken', new Error('connect ECONNREFUSED'))] })
  await monitoring.synced
  monitoring.stop()

  assert.equal(syncs(requests).length, 0)
  assert.ok(errors.some((line) => line.includes("queue 'broken'") && line.includes('ECONNREFUSED')))
})

test('an empty queue is reconciled as empty, pausing its removed schedulers', async (t) => {
  const requests = mockHttp(t)
  captureErrors(t)
  const monitoring = monitorBullMQ({ queues: [new FakeQueue('emptied', [])] })
  await monitoring.synced
  monitoring.stop()

  assert.deepEqual(syncs(requests)[0].body.monitors, [])
  assert.equal(syncs(requests)[0].body.mode, 'reconcile')
})

test('reports start and completion of a scheduled run, in order', async (t) => {
  const requests = mockHttp(t)
  captureErrors(t)
  const queue = new FakeQueue('mail', [{ key: 'digest', name: 'send', pattern: '30 7 * * 1-5' }])
  const worker = new FakeWorker('mail')
  const monitoring = monitorBullMQ({ queues: [queue], workers: [worker] })
  await monitoring.synced

  const run = job('repeat:digest:1', 'digest')
  worker.emit('active', run, 'waiting')
  worker.emit('completed', run, 'done', 'active')
  worker.emit('active', job('42', undefined), 'waiting')
  await settle()
  monitoring.stop()

  assert.deepEqual(
    pings(requests).map(({ key, event }) => [key, event]),
    [
      ['mail-digest', 'start'],
      ['mail-digest', 'complete'],
    ],
  )
  assert.equal(pings(requests)[0].query.schedule, '30 7 * * 1-5')
})

test('reports a failure only once the job has no retries left', async (t) => {
  const requests = mockHttp(t)
  captureErrors(t)
  const queue = new FakeQueue('billing', [{ key: 'invoice', name: 'invoice', pattern: '0 0 1 * *' }])
  const worker = new FakeWorker('billing')
  const monitoring = monitorBullMQ({ queues: [queue], workers: [worker] })
  await monitoring.synced

  const attempt = job('repeat:invoice:1', 'invoice', { opts: { attempts: 2 } })
  worker.emit('active', attempt)
  worker.emit('failed', { ...attempt, attemptsMade: 1 }, new Error('card declined'))
  worker.emit('active', { ...attempt, attemptsMade: 1 })
  worker.emit('failed', { ...attempt, attemptsMade: 2, finishedOn: Date.now() }, new Error('card declined again'))
  await settle()
  monitoring.stop()

  const events = pings(requests)
  assert.deepEqual(
    events.map(({ event }) => event),
    ['start', 'fail'],
  )
  assert.equal(events[1].query.message, 'card declined again')
})

test('syncs a scheduler added after startup before reporting its first run', async (t) => {
  const requests = mockHttp(t)
  captureErrors(t)
  const queue = new FakeQueue('late', [])
  const worker = new FakeWorker('late')
  const monitoring = monitorBullMQ({ queues: [queue], workers: [worker] })
  await monitoring.synced
  queue.schedulers = [{ key: 'added', name: 'added', pattern: '15 * * * *' }]

  // The last refresh is recent, so an unknown scheduler waits for the stale window.
  t.mock.method(Date, 'now', () => Number.MAX_SAFE_INTEGER)
  worker.emit('active', job('repeat:added:1', 'added'))
  await settle()
  monitoring.stop()

  const order = requests.map((r) => (r.path === '/api/v1/sync' ? `sync:${r.body.monitors.length}` : pings([r])[0].event))
  assert.deepEqual(order, ['sync:0', 'sync:1', 'start'])
})

test('falls back to getRepeatableJobs on queues without Job Schedulers', async (t) => {
  const requests = mockHttp(t)
  captureErrors(t)
  const queue = {
    name: 'legacy',
    getRepeatableJobs: async () => [
      { key: '3b4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b', name: 'cleanup', id: null, pattern: '0 1 * * *', tz: null },
      { key: 'rotate:keys::Europe/Berlin:0 5 * * *', name: 'rotate', id: 'keys', pattern: '0 5 * * *', tz: 'Europe/Berlin' },
    ],
  }
  const monitoring = monitorBullMQ({ queues: [queue] })
  await monitoring.synced
  monitoring.stop()

  assert.deepEqual(
    syncs(requests)[0].body.monitors.map((m) => m.key),
    ['legacy-cleanup', 'legacy-rotate-keys'],
  )
})

test('warns when schedulers run in a time zone other than the application', async (t) => {
  mockHttp(t)
  const errors = captureErrors(t)
  const queue = new FakeQueue('zones', [
    { key: 'tokyo', name: 'tokyo', pattern: '0 9 * * *', tz: 'Asia/Tokyo' },
    { key: 'utc', name: 'utc', pattern: '0 9 * * *', tz: 'Etc/UTC' },
  ])
  const monitoring = monitorBullMQ({ queues: [queue], timeZone: 'UTC' })
  await monitoring.synced
  monitoring.stop()

  assert.ok(errors.some((line) => line.includes("'zones-tokyo' runs in Asia/Tokyo")))
  assert.ok(!errors.some((line) => line.includes("'zones-utc' runs in")))
})

test('never throws into the application when CronRadar is unreachable', async (t) => {
  mockHttp(t, () => new Error('getaddrinfo ENOTFOUND cron.life'))
  captureErrors(t)
  const queue = new FakeQueue('offline', [{ key: 'job', name: 'job', pattern: '* * * * *' }])
  const worker = new FakeWorker('offline')
  const monitoring = monitorBullMQ({ queues: [queue], workers: [worker] })
  await monitoring.synced

  assert.doesNotThrow(() => {
    worker.emit('active', job('1', 'job'))
    worker.emit('failed', job('1', 'job', { attemptsMade: 1, finishedOn: 1 }), new Error('boom'))
    worker.emit('failed', undefined, new Error('stalled'))
  })
  await settle()
  monitoring.stop()
})

test('setup with unusable arguments logs and returns a handle', async (t) => {
  captureErrors(t)
  const monitoring = monitorBullMQ(undefined)
  await monitoring.synced
  monitoring.stop()
})
