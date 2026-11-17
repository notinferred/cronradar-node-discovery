import * as cronradar from 'cronradar'
import { bullmqSource, normalizeKey } from './keys'
import {
  beginRun,
  canonicalTimeZone,
  Declaration,
  failureMessage,
  processTimeZone,
  Run,
  syncRegistry,
  warnOnce,
  warnTimeZones,
} from './report'
import { toCronRadarSchedule } from './schedule'

const PREFIX = '[CronRadar.BullMQ]'
const RECONCILE_INTERVAL_MS = 15 * 60 * 1000
const UNKNOWN_SCHEDULER_REFRESH_MS = 60 * 1000
const MAX_TRACKED_RUNS = 1000

/** The fields CronRadar reads from a Job Scheduler or legacy repeatable job. */
export interface BullMQSchedulerEntry {
  key: string
  name: string
  id?: string | null
  pattern?: string | null
  every?: number | string | null
  tz?: string | null
  template?: { opts?: object }
}

/** A BullMQ `Queue`; only its name and scheduler listing are used. */
export interface BullMQQueueLike {
  readonly name: string
  getJobSchedulers?(start?: number, end?: number, asc?: boolean): Promise<BullMQSchedulerEntry[]>
  getRepeatableJobs?(start?: number, end?: number, asc?: boolean): Promise<BullMQSchedulerEntry[]>
}

/** The fields CronRadar reads from a BullMQ `Job`. */
export interface BullMQJobLike {
  id?: string
  repeatJobKey?: string
  attemptsMade: number
  finishedOn?: number
  opts: { attempts?: number }
}

/** A BullMQ `Worker`; CronRadar listens to its `active`, `completed` and `failed` events. */
export interface BullMQWorkerLike {
  readonly name: string
  on(event: 'active' | 'completed' | 'failed', listener: (...args: any[]) => void): unknown
  off(event: 'active' | 'completed' | 'failed', listener: (...args: any[]) => void): unknown
}

export interface MonitorBullMQOptions {
  /** Every queue whose Job Schedulers should be monitored. */
  queues: BullMQQueueLike[]
  /** Workers in this process that run those queues' jobs; their runs are reported. */
  workers?: BullMQWorkerLike[]
  /** Scheduler IDs (or legacy repeatable job names) to leave unmonitored. */
  skip?: string[]
  /** The CronRadar application's time zone, to warn about schedulers that run in another one. */
  timeZone?: string
}

export interface BullMQMonitoring {
  /** Settles when the first sync of every queue has finished. Never rejects. */
  readonly synced: Promise<void>
  /** Stops periodic reconciliation and removes the worker listeners. */
  stop(): void
}

interface QueueState {
  queue: BullMQQueueLike
  /** By repeat key; null marks a scheduler that is known but not monitored. */
  registry: Map<string, Declaration | null>
  lastRefreshAt: number
  refreshing?: Promise<void>
}

interface Settings {
  skip: Set<string>
  timeZone?: string
}

/**
 * Discovers the Job Schedulers of each queue, reconciles them to CronRadar now and every
 * 15 minutes, and reports every scheduled run the given workers process. Never throws.
 */
export function monitorBullMQ(options: MonitorBullMQOptions): BullMQMonitoring {
  const detach: Array<() => void> = []
  let timer: NodeJS.Timeout | undefined
  let synced: Promise<void> = Promise.resolve()

  try {
    if (!process.env.CRONRADAR_API_KEY) {
      warnOnce(PREFIX, 'CRONRADAR_API_KEY is not set; BullMQ jobs will not be monitored.')
    }
    const settings: Settings = { skip: new Set(options.skip ?? []), timeZone: options.timeZone }
    const states = new Map<string, QueueState>()
    for (const queue of options.queues ?? []) {
      states.set(queue.name, { queue, registry: new Map(), lastRefreshAt: 0 })
    }

    const refreshAll = async () => {
      for (const state of states.values()) await refresh(state, settings)
    }
    synced = refreshAll()
    timer = setInterval(() => void refreshAll(), RECONCILE_INTERVAL_MS)
    timer.unref()

    for (const worker of options.workers ?? []) {
      const state = states.get(worker.name)
      if (!state) {
        warnOnce(PREFIX, `The worker for queue '${worker.name}' has no matching queue in monitorBullMQ({ queues }), so its jobs are not reported.`)
        continue
      }
      detach.push(attachWorker(worker, state, settings))
    }
  } catch (error) {
    console.error(`${PREFIX} Setup failed: ${failureMessage(error)}`)
  }

  return {
    synced,
    stop() {
      if (timer) clearInterval(timer)
      for (const remove of detach.splice(0)) {
        try {
          remove()
        } catch {
          // A closed worker may refuse listener changes; there is nothing left to report.
        }
      }
    },
  }
}

/** Rediscovers and reconciles one queue, sharing a refresh already in flight. Never rejects. */
function refresh(state: QueueState, settings: Settings): Promise<void> {
  state.refreshing ??= reconcile(state, settings)
    .catch((error) => console.error(`${PREFIX} Reconciling queue '${state.queue.name}' failed: ${failureMessage(error)}`))
    .finally(() => (state.refreshing = undefined))
  return state.refreshing
}

async function reconcile(state: QueueState, settings: Settings): Promise<void> {
  const registry = await discover(state.queue, settings.skip)
  state.lastRefreshAt = Date.now()
  // An unreadable registry is never sent: an empty reconcile would pause every monitor.
  if (!registry) return
  state.registry = registry
  const declarations = [...registry.values()].filter((entry): entry is Declaration => entry !== null)
  warnTimeZones(PREFIX, declarations, settings.timeZone)
  await syncRegistry(bullmqSource(state.queue.name), declarations)
}

async function discover(queue: BullMQQueueLike, skip: Set<string>): Promise<QueueState['registry'] | undefined> {
  let entries: BullMQSchedulerEntry[]
  try {
    // From BullMQ 5.16, getJobSchedulers lists legacy repeatable jobs too; both read one set.
    if (typeof queue.getJobSchedulers === 'function') entries = await queue.getJobSchedulers()
    else if (typeof queue.getRepeatableJobs === 'function') entries = await queue.getRepeatableJobs()
    else {
      warnOnce(PREFIX, `Queue '${queue.name}' has neither getJobSchedulers nor getRepeatableJobs, so it is not monitored.`)
      return undefined
    }
  } catch (error) {
    console.error(`${PREFIX} Could not read the schedulers of queue '${queue.name}': ${failureMessage(error)}. Its monitors are left unchanged.`)
    return undefined
  }

  const registry: QueueState['registry'] = new Map()
  const owners = new Map<string, string>()
  const defaultZone = processTimeZone()
  for (const entry of entries ?? []) {
    if (!entry || typeof entry.key !== 'string') continue
    const legacy = legacyRepeatKey(entry.key)
    const label = legacy ? entry.name : entry.key
    const optedOut = (entry.template?.opts as { cronradar?: unknown } | undefined)?.cronradar === false
    if (skip.has(entry.key) || skip.has(label) || optedOut) {
      registry.set(entry.key, null)
      continue
    }
    if (!entry.pattern) {
      warnOnce(PREFIX, `'${label}' in queue '${queue.name}' repeats every ${entry.every} ms, which has no cron form, so it is not monitored. Give it a cron pattern, or report it with wrap() from the cronradar package.`)
      registry.set(entry.key, null)
      continue
    }
    const translation = toCronRadarSchedule(entry.pattern)
    if ('reason' in translation) {
      warnOnce(PREFIX, `'${label}' in queue '${queue.name}' is not monitored: ${translation.reason}.`)
      registry.set(entry.key, null)
      continue
    }
    const key = normalizeKey(`${queue.name}-${legacy && entry.id ? `${entry.name}-${entry.id}` : label}`)
    if (owners.has(key)) {
      warnOnce(PREFIX, `'${label}' in queue '${queue.name}' would share the monitor key '${key}' with '${owners.get(key)}', so it is not monitored. Give it a distinct scheduler ID.`)
      registry.set(entry.key, null)
      continue
    }
    owners.set(key, label)
    registry.set(entry.key, {
      key,
      name: `${label} (${queue.name})`,
      schedule: translation.schedule,
      timeZone: canonicalTimeZone(entry.tz || defaultZone),
    })
  }
  return registry
}

/** Legacy repeatable jobs are keyed by a 32-character hash or by name:id:endDate:tz:pattern. */
function legacyRepeatKey(key: string): boolean {
  return /^[0-9a-f]{32}$/.test(key) || key.split(':').length >= 5
}

function attachWorker(worker: BullMQWorkerLike, state: QueueState, settings: Settings): () => void {
  // Events for one job are handled in order, so a terminal report never overtakes its start.
  const chains = new Map<string, Promise<void>>()
  const runs = new Map<string, Run>()

  const resolve = async (repeatKey: string): Promise<Declaration | null> => {
    if (state.refreshing) await state.refreshing
    const stale = Date.now() - state.lastRefreshAt > UNKNOWN_SCHEDULER_REFRESH_MS
    // A scheduler added since the last reconcile is synced before its first run is reported.
    if (!state.registry.has(repeatKey) && stale) await refresh(state, settings)
    return state.registry.get(repeatKey) ?? null
  }

  const handle = (job: BullMQJobLike | undefined, step: (declaration: Declaration, jobId: string) => Promise<void>) => {
    try {
      const repeatKey = job?.repeatJobKey
      if (!job || !repeatKey) return
      const jobId = job.id ?? repeatKey
      const next = (chains.get(jobId) ?? Promise.resolve())
        .then(() => resolve(repeatKey))
        .then((declaration) => (declaration ? step(declaration, jobId) : undefined))
        .catch((error) => console.error(`${PREFIX} Reporting failed: ${failureMessage(error)}`))
      chains.set(jobId, next)
      void next.finally(() => {
        if (chains.get(jobId) === next) chains.delete(jobId)
      })
    } catch (error) {
      console.error(`${PREFIX} Reporting failed: ${failureMessage(error)}`)
    }
  }

  const takeRun = (jobId: string) => {
    const run = runs.get(jobId)
    runs.delete(jobId)
    return run
  }

  const onActive = (job: BullMQJobLike) =>
    handle(job, async (declaration, jobId) => {
      // A retry continues the run its first attempt started, possibly in another process.
      if (job.attemptsMade > 0) return
      if (runs.size >= MAX_TRACKED_RUNS) runs.delete(runs.keys().next().value as string)
      runs.set(jobId, beginRun(declaration.key, declaration.schedule))
    })

  const onCompleted = (job: BullMQJobLike) =>
    handle(job, async (declaration, jobId) => {
      const run = takeRun(jobId)
      await (run ? run.complete() : cronradar.completeJob(declaration.key))
    })

  const onFailed = (job: BullMQJobLike | undefined, error: unknown) =>
    handle(job, async (declaration, jobId) => {
      if (job && !finalFailure(job)) return
      const run = takeRun(jobId)
      await (run ? run.fail(error) : cronradar.failJob(declaration.key, failureMessage(error)))
    })

  worker.on('active', onActive)
  worker.on('completed', onCompleted)
  worker.on('failed', onFailed)
  return () => {
    worker.off('active', onActive)
    worker.off('completed', onCompleted)
    worker.off('failed', onFailed)
  }
}

/** BullMQ sets finishedOn only when a failed job will not be retried; attempts cover older versions. */
function finalFailure(job: BullMQJobLike): boolean {
  if (typeof job.finishedOn === 'number') return true
  return job.attemptsMade >= Math.max(1, job.opts?.attempts ?? 1)
}
