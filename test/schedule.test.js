'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { toCronRadarSchedule } = require('../dist/schedule')
const { normalizeKey, scheduleKey, bullmqSource } = require('../dist/keys')

test('passes five-field expressions and shortcuts through', () => {
  assert.deepEqual(toCronRadarSchedule('*/15 9-17 * * MON-FRI'), { schedule: '*/15 9-17 * * MON-FRI' })
  assert.deepEqual(toCronRadarSchedule('@Daily'), { schedule: '@daily' })
})

test('drops a seconds field that is always 0', () => {
  assert.deepEqual(toCronRadarSchedule('0 30 2 * * *'), { schedule: '30 2 * * *' })
  assert.deepEqual(toCronRadarSchedule('00 0 * * * *'), { schedule: '0 * * * *' })
  assert.ok('reason' in toCronRadarSchedule('30 * * * * *'))
  assert.ok('reason' in toCronRadarSchedule('*/5 * * * * *'))
})

test('spells ? as * and Sunday as 0', () => {
  assert.deepEqual(toCronRadarSchedule('0 9 ? * 7'), { schedule: '0 9 * * 0' })
  assert.deepEqual(toCronRadarSchedule('0 9 * * 0-7'), { schedule: '0 9 * * 0-6' })
  assert.deepEqual(toCronRadarSchedule('0 9 * * 1,7'), { schedule: '0 9 * * 1,0' })
  assert.ok('reason' in toCronRadarSchedule('0 9 * * */7'))
})

test('rejects what five fields cannot express', () => {
  for (const pattern of ['@reboot', '0 9 L * *', '0 9 * * 1#1', '0 9 15W * *', '0 0 2 * * * 2026', '* * *']) {
    assert.ok('reason' in toCronRadarSchedule(pattern), pattern)
  }
  assert.deepEqual(toCronRadarSchedule('0 9 * JUL WED'), { schedule: '0 9 * JUL WED' })
})

test('derives stable keys within the 100-character limit', () => {
  assert.equal(normalizeKey('Reports.Nightly_Export'), 'reports-nightly-export')
  const long = normalizeKey('q-' + 'a'.repeat(120))
  assert.equal(long.length, 100)
  assert.equal(long, normalizeKey('q-' + 'a'.repeat(120)))
  assert.match(long, /^q-a{89}-[0-9a-f]{8}$/)
  assert.equal(scheduleKey('*/5 9-17 * * 1,3'), 'cron-xs5-9r17-x-x-1c3')
  assert.equal(bullmqSource('q'), 'bullmq:q')
  assert.equal(bullmqSource('x'.repeat(60)).length, 50)
})
