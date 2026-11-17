import * as cronradar from 'cronradar'

export const VERSION = '0.1.0'

const MAX_FAILURE_MESSAGE_LENGTH = 1000
const warned = new Set<string>()

/** Writes a warning to stderr once per process, so periodic reconciliation does not repeat it. */
export function warnOnce(prefix: string, message: string): void {
  const line = `${prefix} ${message}`
  if (warned.has(line)) return
  warned.add(line)
  console.error(line)
}

export function failureMessage(error: unknown): string {
  let message = 'Job failed'
  try {
    if (typeof error === 'string' && error) message = error
    else if (error && typeof (error as Error).message === 'string' && (error as Error).message) {
      message = (error as Error).message
    }
  } catch {
    // A job may throw any value, including an object with a throwing getter.
  }
  return message.slice(0, MAX_FAILURE_MESSAGE_LENGTH)
}

export interface Run {
  complete(): Promise<void>
  fail(error: unknown): Promise<void>
}

/**
 * Reports a start without delaying the job; the terminal report waits for the start
 * request, so CronRadar receives them in order. Passing the schedule lets a monitor that
 * sync has not created yet create itself.
 */
export function beginRun(key: string, schedule: string): Run {
  const started = cronradar.startJob(key, { schedule }).catch(() => undefined)
  return {
    complete: () => started.then(() => cronradar.completeJob(key)).catch(() => undefined),
    fail: (error) => started.then(() => cronradar.failJob(key, failureMessage(error))).catch(() => undefined),
  }
}

export interface Declaration {
  key: string
  name: string
  schedule: string
  /** The IANA zone the scheduler evaluates the schedule in. */
  timeZone: string
}

export function syncRegistry(source: string, declarations: Declaration[]): Promise<void> {
  const monitors = declarations.map(({ key, name, schedule }) => ({ key, name, schedule }))
  return cronradar.syncMonitors(monitors, { source, version: VERSION }).catch(() => undefined)
}

export function processTimeZone(): string {
  return Intl.DateTimeFormat().resolvedOptions().timeZone
}

export function canonicalTimeZone(zone: string): string {
  try {
    return new Intl.DateTimeFormat('en-US', { timeZone: zone }).resolvedOptions().timeZone
  } catch {
    return zone
  }
}

/**
 * CronRadar evaluates schedules in the application's time zone. With `expected` (that zone),
 * each job in another zone is named; without it, a registry spanning zones is named, since
 * one application cannot match them all.
 */
export function warnTimeZones(prefix: string, declarations: Declaration[], expected?: string): void {
  if (expected) {
    const zone = canonicalTimeZone(expected)
    for (const declaration of declarations) {
      if (declaration.timeZone !== zone) {
        warnOnce(
          prefix,
          `'${declaration.key}' runs in ${declaration.timeZone}, but the CronRadar application evaluates schedules in ${zone}. Expected run times will be off until the zones match.`,
        )
      }
    }
    return
  }
  const zones = [...new Set(declarations.map((declaration) => declaration.timeZone))].sort()
  if (zones.length > 1) {
    warnOnce(
      prefix,
      `Jobs run in more than one time zone (${zones.join(', ')}). CronRadar evaluates schedules in the application's time zone; report jobs from each zone to an application set to that zone.`,
    )
  }
}
