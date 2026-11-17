'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { mockHttp, captureErrors, settle, syncs, pings } = require('./helpers')
const { monitorNodeCron } = require('../dist')

const SYNC_WAIT_MS = 1100

/** Mimics node-cron 4: schedule starts the task, createTask does not, getTasks lists both. */
function fakeNodeCron({ version = 4 } = {}) {
  const tasks = new Map()
  const make = (expression, func, options, started) => {
    const task = {
      expression,
      func,
      options,
      started,
      start() {
        this.started = true
      },
      stop() {
        this.started = false
      },
      destroy() {
        this.started = false
        tasks.delete(this)
      },
    }
    tasks.set(task, task)
    return task
  }
  const cron = {
    validate: () => true,
    schedule: (expression, func, options) => make(expression, func, options, version === 4 || options?.scheduled !== false),
    getTasks: () => tasks,
  }
  if (version === 4) cron.createTask = (expression, func, options) => make(expression, func, options, false)
  return cron
}

test('returns a monitored copy and leaves node-cron itself unchanged', (t) => {
  mockHttp(t)
  captureErrors(t)
  const cron = fakeNodeCron()
  const originalSchedule = cron.schedule
  const monitored = monitorNodeCron(cron)

  assert.equal(cron.schedule, originalSchedule)
  assert.notEqual(monitored.schedule, originalSchedule)
  assert.equal(monitored.validate, cron.validate)
})

test('syncs every scheduled task once scheduling settles', async (t) => {
  const requests = mockHttp(t)
  const errors = captureErrors(t)
  const cron = fakeNodeCron()
  const monitored = monitorNodeCron(cron, { skip: ['internal-cleanup'] })

  monitored.schedule('0 2 * * *', function backupDatabase() {}, { name: 'Nightly Backup' })
  monitored.schedule('0 */5 * * * *', () => {})
  monitored.schedule('*/10 * * * * *', () => {}, { name: 'too-fast' })
  monitored.schedule('0 3 * * *', () => {}, { name: 'opted-out', cronradar: false })
  monitored.schedule('0 4 * * *', () => {}, { name: 'internal-cleanup' })
  monitored.schedule('0 9 * * 1-7', function weekdays() {})
  monitored.createTask('0 6 * * *', () => {}, { name: 'created-not-started' })
  await settle(SYNC_WAIT_MS)

  const sync = syncs(requests)
  assert.equal(sync.length, 1)
  assert.equal(sync[0].body.source, 'node-cron')
  assert.equal(sync[0].body.mode, 'reconcile')
  assert.deepEqual(
    sync[0].body.monitors.map(({ key, name, schedule }) => ({ key, name, schedule })),
    [
      { key: 'nightly-backup', name: 'Nightly Backup', schedule: '0 2 * * *' },
      { key: 'cron-xs5-x-x-x-x', name: 'Cron */5 * * * *', schedule: '*/5 * * * *' },
      { key: 'cron-0-9-x-x-1r6c0', name: 'weekdays', schedule: '0 9 * * 1-6,0' },
    ],
  )
  const optedOut = [...cron.getTasks().values()].find((task) => task.options?.name === 'opted-out')
  assert.equal('cronradar' in optedOut.options, false)
  assert.ok(errors.some((line) => line.includes("'too-fast'") && line.includes('seconds field')))
})

test('a task created with createTask joins the registry when it starts', async (t) => {
  const requests = mockHttp(t)
  captureErrors(t)
  const monitored = monitorNodeCron(fakeNodeCron())

  const task = monitored.createTask('0 6 * * *', () => {}, { name: 'morning' })
  await settle(SYNC_WAIT_MS)
  task.start()
  await settle(SYNC_WAIT_MS)

  const sync = syncs(requests)
  assert.deepEqual(sync.at(-1).body.monitors.map((m) => m.key), ['morning'])
})

test('node-cron 3 tasks scheduled with scheduled: false wait for start()', async (t) => {
  const requests = mockHttp(t)
  captureErrors(t)
  const monitored = monitorNodeCron(fakeNodeCron({ version: 3 }))

  monitored.schedule('0 6 * * *', () => {}, { name: 'paused-at-boot', scheduled: false })
  monitored.schedule('0 7 * * *', () => {}, { name: 'running' })
  await settle(SYNC_WAIT_MS)

  assert.deepEqual(syncs(requests)[0].body.monitors.map((m) => m.key), ['running'])
})

test('stopping a task does not sync at once, so a graceful shutdown pauses nothing', async (t) => {
  const requests = mockHttp(t)
  captureErrors(t)
  const monitored = monitorNodeCron(fakeNodeCron())
  const task = monitored.schedule('0 6 * * *', () => {}, { name: 'kept' })
  await settle(SYNC_WAIT_MS)
  task.stop()
  await settle(SYNC_WAIT_MS)

  assert.equal(syncs(requests).length, 1)
  assert.equal(task.started, false)
})

test('reports each run and returns what the task returns', async (t) => {
  const requests = mockHttp(t)
  captureErrors(t)
  const cron = fakeNodeCron()
  const monitored = monitorNodeCron(cron)
  const task = monitored.schedule('15 * * * *', async (context) => `ran at ${context.date}`, { name: 'hourly' })

  assert.equal(await task.func({ date: 'noon' }), 'ran at noon')
  await settle()

  assert.deepEqual(
    pings(requests).map(({ key, event }) => [key, event]),
    [
      ['hourly', 'start'],
      ['hourly', 'complete'],
    ],
  )
  assert.equal(pings(requests)[0].query.schedule, '15 * * * *')
})

test("re-throws the task's own error unchanged and reports the failure", async (t) => {
  const requests = mockHttp(t)
  captureErrors(t)
  const monitored = monitorNodeCron(fakeNodeCron())
  const asyncError = new Error('export failed')
  const syncError = new TypeError('bad input')
  const asyncTask = monitored.schedule('0 1 * * *', async () => Promise.reject(asyncError), { name: 'export' })
  const syncTask = monitored.schedule('0 2 * * *', () => {
    throw syncError
  }, { name: 'import' })

  await assert.rejects(asyncTask.func(), (error) => error === asyncError)
  assert.throws(() => syncTask.func(), (error) => error === syncError)
  await settle()

  const failures = pings(requests).filter((p) => p.event === 'fail')
  assert.deepEqual(
    failures.map(({ key, query }) => [key, query.message]),
    [
      ['export', 'export failed'],
      ['import', 'bad input'],
    ],
  )
})

test('the task runs normally when CronRadar is unreachable', async (t) => {
  mockHttp(t, () => new Error('connect ETIMEDOUT'))
  captureErrors(t)
  const monitored = monitorNodeCron(fakeNodeCron())
  const task = monitored.schedule('* * * * *', () => 7, { name: 'offline' })

  assert.equal(task.func(), 7)
  await settle()
})

test('unnamed tasks on one schedule are told apart by order, with a warning', async (t) => {
  const requests = mockHttp(t)
  const errors = captureErrors(t)
  const monitored = monitorNodeCron(fakeNodeCron())
  monitored.schedule('0 8 * * *', () => {})
  monitored.schedule('0 8 * * *', () => {})
  monitored.schedule('0 8 * * *', () => {}, { name: 'dup' })
  monitored.schedule('0 9 * * *', () => {}, { name: 'dup' })
  await settle(SYNC_WAIT_MS)

  assert.deepEqual(syncs(requests).at(-1).body.monitors.map((m) => m.key), ['cron-0-8-x-x-x', 'cron-0-8-x-x-x-2', 'dup'])
  assert.ok(errors.some((line) => line.includes("Two tasks are named 'dup'")))
})

test('warns about tasks scheduled without the monitored copy and about file tasks', async (t) => {
  mockHttp(t)
  const errors = captureErrors(t)
  const cron = fakeNodeCron()
  const monitored = monitorNodeCron(cron)
  cron.schedule('0 0 * * *', () => {})
  monitored.schedule('0 1 * * *', './tasks/report.js', { name: 'file-task' })
  monitored.schedule('0 2 * * *', () => {}, { name: 'watched' })
  await settle(SYNC_WAIT_MS)

  assert.ok(errors.some((line) => line.includes('1 node-cron task(s) were scheduled without monitorNodeCron')))
  assert.ok(errors.some((line) => line.includes("'file-task' runs a file")))
})

test('invalid expressions still reach node-cron, which decides', (t) => {
  mockHttp(t)
  captureErrors(t)
  const cron = fakeNodeCron()
  cron.schedule = () => {
    throw new Error('invalid cron expression')
  }
  const monitored = monitorNodeCron(cron)
  assert.throws(() => monitored.schedule('not a cron', () => {}), /invalid cron expression/)
})
