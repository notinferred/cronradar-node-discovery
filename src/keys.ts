import { createHash } from 'node:crypto'

export const MAX_MONITOR_KEY_LENGTH = 100
const MAX_SOURCE_LENGTH = 50
const HASH_LENGTH = 8

/** Lowercase, with every run of other characters turned into one hyphen, as cronradar-celery does. */
export function normalizeKey(raw: string): string {
  const key = raw
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
  return shorten(key, MAX_MONITOR_KEY_LENGTH)
}

/** The sync source for one BullMQ queue; the server stores at most 50 characters. */
export function bullmqSource(queueName: string): string {
  return shorten(`bullmq:${queueName}`, MAX_SOURCE_LENGTH)
}

/**
 * A key over the limit keeps its start and ends with 8 hex characters of the SHA-256 of the
 * whole key, the rule every CronRadar SDK uses, so it stays stable and distinct.
 */
function shorten(key: string, limit: number): string {
  if (key.length <= limit) return key
  const digest = createHash('sha256').update(key, 'utf8').digest('hex').slice(0, HASH_LENGTH)
  return key.slice(0, limit - HASH_LENGTH - 1).replace(/-+$/, '') + '-' + digest
}

const SCHEDULE_SYMBOLS: Record<string, string> = { '*': 'x', '/': 's', ',': 'c', '-': 'r', ' ': '-' }

/** A readable key for a task that has no name: `0 9 * * 1-5` becomes `cron-0-9-x-x-1r5`. */
export function scheduleKey(schedule: string): string {
  const spelled = schedule
    .toLowerCase()
    .replace(/[*/,\- ]/g, (symbol) => SCHEDULE_SYMBOLS[symbol])
    .replace(/^@/, '')
  return normalizeKey(`cron-${spelled}`)
}
