import { normalizeKey, scheduleKey } from './keys'
import {
  beginRun,
  canonicalTimeZone,
  Declaration,
  failureMessage,
  processTimeZone,
  syncRegistry,
  warnOnce,
  warnTimeZones,
} from './report'
import { toCronRadarSchedule } from './schedule'

const PREFIX = '[CronRadar.NodeCron]'
const SOURCE = 'node-cron'
const RECONCILE_INTERVAL_MS = 15 * 60 * 1000
const SYNC_DEBOUNCE_MS = 1000

/** node-cron 3 or 4: the module object, its default export, or its namespace. */
export interface NodeCronLike {
  schedule(expression: string, func: any, options?: any): any
  createTask?(expression: string, func: any, options?: any): any
  getTasks?(): Map<string, unknown> | Record<string, unknown>
}

export interface MonitorNodeCronOptions {
  /** Task names to leave unmonitored. */
  skip?: string[]
  /** The CronRadar application's time zone, to warn about tasks that run in another one. */
  timeZone?: string
}

interface Entry {
  declaration: Declaration
  active: boolean
}

/**
 * Returns a copy of `cron` whose `schedule` and `createTask` register every task with
 * CronRadar and report each run. `cron` itself is not changed.
 */
export function monitorNodeCron<T extends NodeCronLike>(cron: T, options: MonitorNodeCronOptions = {}): T {
  const skip = new Set(options.skip ?? [])
  const entries: Entry[] = []
  const viaFacade = new WeakSet<object>()
  let debounce: NodeJS.Timeout | undefined
  let interval: NodeJS.Timeout | undefined

  if (!process.env.CRONRADAR_API_KEY) {
    warnOnce(PREFIX, 'CRONRADAR_API_KEY is not set; node-cron tasks will not be monitored.')
  }

  const reconcile = () => {
    try {
      const declarations = entries.filter((entry) => entry.active).map((entry) => entry.declaration)
      warnTimeZones(PREFIX, declarations, options.timeZone)
      warnUnwrappedTasks(cron, viaFacade)
      void syncRegistry(SOURCE, declarations)
    } catch (error) {
      console.error(`${PREFIX} Reconciling failed: ${failureMessage(error)}`)
    }
  }

  // Tasks are scheduled one after another at startup; one sync covers them all.
  const scheduleSync = () => {
    if (debounce) clearTimeout(debounce)
    debounce = setTimeout(reconcile, SYNC_DEBOUNCE_MS)
    debounce.unref()
    if (!interval) {
      interval = setInterval(reconcile, RECONCILE_INTERVAL_MS)
      interval.unref()
    }
  }

  const prepare = (expression: unknown, func: unknown, taskOptions: any): { options: any; func: unknown; entry?: Entry } => {
    const options = withoutOptOut(taskOptions)
    try {
      const name: unknown = taskOptions?.name
      if (taskOptions?.cronradar === false || (typeof name === 'string' && skip.has(name))) {
        return { func, options }
      }
      if (typeof func !== 'function') {
        warnOnce(PREFIX, `The task '${name ?? expression}' runs a file, which CronRadar cannot observe, so it is not monitored. Report it with wrap() from the cronradar package inside that file.`)
        return { func, options }
      }
      const translation = toCronRadarSchedule(String(expression))
      if ('reason' in translation) {
        warnOnce(PREFIX, `The task '${name ?? expression}' is not monitored: ${translation.reason}.`)
        return { func, options }
      }
      const declaration = declare(name, func, translation.schedule, taskOptions?.timezone)
      if (!declaration) return { func, options }
      return { func: instrument(func, declaration), options, entry: { declaration, active: false } }
    } catch (error) {
      console.error(`${PREFIX} Could not monitor a task: ${failureMessage(error)}`)
      return { func, options }
    }
  }

  const declare = (name: unknown, func: Function, schedule: string, zone: unknown): Declaration | undefined => {
    const named = typeof name === 'string' && name.trim() !== ''
    const baseKey = named ? normalizeKey(name) : scheduleKey(schedule)
    if (!baseKey) {
      warnOnce(PREFIX, `The task '${name}' has no letters or digits in its name, so it is not monitored. Give it a name.`)
      return undefined
    }
    let key = baseKey
    if (entries.some((entry) => entry.declaration.key === key)) {
      if (named) {
        warnOnce(PREFIX, `Two tasks are named '${name}', so only the first is monitored. Give each task a distinct name.`)
        return undefined
      }
      // Tasks without names are told apart by the order they are scheduled in.
      let count = 2
      while (entries.some((entry) => entry.declaration.key === `${baseKey}-${count}`)) count++
      key = `${baseKey}-${count}`
      warnOnce(PREFIX, `Several unnamed tasks run on '${schedule}'; they are keyed by the order they are scheduled in. Give each a name so its monitor stays the same when that order changes.`)
    }
    return {
      key,
      name: named ? (name as string) : func.name || `Cron ${schedule}`,
      schedule,
      timeZone: canonicalTimeZone(typeof zone === 'string' && zone ? zone : processTimeZone()),
    }
  }

  const track = (task: any, entry: Entry, active: boolean) => {
    try {
      entries.push(entry)
      entry.active = active
      if (task && typeof task === 'object') {
        for (const method of ['start', 'stop', 'destroy'] as const) {
          const original = task[method]
          if (typeof original !== 'function') continue
          task[method] = function (this: unknown, ...args: unknown[]) {
            entry.active = method === 'start'
            if (method === 'destroy' && entries.includes(entry)) entries.splice(entries.indexOf(entry), 1)
            // A stop is left for the next periodic sync, so a graceful shutdown pauses nothing.
            if (method === 'start') scheduleSync()
            return original.apply(this, args)
          }
        }
      }
      scheduleSync()
    } catch (error) {
      console.error(`${PREFIX} Could not monitor a task: ${failureMessage(error)}`)
    }
  }

  const register = (task: unknown, entry: Entry | undefined, active: boolean) => {
    if (task && typeof task === 'object') viaFacade.add(task)
    if (entry) track(task, entry, active)
  }

  const facade: any = { ...cron }
  // node-cron 3 has no createTask, and its schedule waits for start() when scheduled is false.
  facade.schedule = function (expression: string, func: unknown, taskOptions?: any) {
    const prepared = prepare(expression, func, taskOptions)
    const task = cron.schedule(expression, prepared.func, prepared.options)
    register(task, prepared.entry, taskOptions?.scheduled !== false || typeof cron.createTask === 'function')
    return task
  }
  if (typeof cron.createTask === 'function') {
    const createTask = cron.createTask
    facade.createTask = function (expression: string, func: unknown, taskOptions?: any) {
      const prepared = prepare(expression, func, taskOptions)
      const task = createTask.call(cron, expression, prepared.func, prepared.options)
      register(task, prepared.entry, false)
      return task
    }
  }
  return facade as T
}

/** Reports start, completion and failure around a task function and returns what it returns. */
function instrument(func: Function, declaration: Declaration): Function {
  return function (this: unknown, ...args: unknown[]) {
    let run: ReturnType<typeof beginRun> | undefined
    try {
      run = beginRun(declaration.key, declaration.schedule)
    } catch {
      run = undefined
    }
    let result: unknown
    try {
      result = func.apply(this, args)
    } catch (error) {
      void run?.fail(error)
      throw error
    }
    if (result && typeof (result as PromiseLike<unknown>).then === 'function') {
      return (result as PromiseLike<unknown>).then(
        (value) => {
          void run?.complete()
          return value
        },
        (error) => {
          void run?.fail(error)
          throw error
        },
      )
    }
    void run?.complete()
    return result
  }
}

/** The `cronradar` option is CronRadar's; node-cron receives the options without it. */
function withoutOptOut(taskOptions: any): any {
  if (!taskOptions || typeof taskOptions !== 'object' || !('cronradar' in taskOptions)) return taskOptions
  const { cronradar: _optOut, ...forwarded } = taskOptions
  return forwarded
}

function warnUnwrappedTasks(cron: NodeCronLike, viaFacade: WeakSet<object>): void {
  if (typeof cron.getTasks !== 'function') return
  const registered = cron.getTasks()
  const tasks = registered instanceof Map ? [...registered.values()] : Object.values(registered ?? {})
  const missed = tasks.filter((task) => task && typeof task === 'object' && !viaFacade.has(task)).length
  if (missed > 0) {
    warnOnce(PREFIX, `${missed} node-cron task(s) were scheduled without monitorNodeCron and are not monitored. Schedule them with the object monitorNodeCron returns.`)
  }
}
