# CronRadar for BullMQ and node-cron

Add one call at startup and every BullMQ Job Scheduler and node-cron task is monitored by [CronRadar](https://cronradar.com): it is registered with its schedule, each run is reported, and you hear about the runs that fail, run late or never start, and the jobs that disappear.

**Status: not yet published.** Version 0.1.0 is not yet on npm, so the install command below does not work yet. This README describes that release.

Built on the [`cronradar`](https://www.npmjs.com/package/cronradar) base SDK, which sends every request.

## Install

```bash
npm install cronradar-node-discovery cronradar
```

Node.js 20 or later. BullMQ 5 or 6, node-cron 3 or 4; install whichever you use. The package has no runtime dependencies of its own: `cronradar`, `bullmq` and `node-cron` are peer dependencies, so it uses your copies.

## Setup

The API key is the only required configuration. The package reads it from `CRONRADAR_API_KEY`:

```bash
export CRONRADAR_API_KEY="$(cat /run/secrets/cronradar-api-key)"
```

Copy the key (it starts with `ck_app_`) from your application's settings at [app.cronradar.com](https://app.cronradar.com). Create a separate CronRadar application and key for production, staging and development, store the key in your platform's secret manager, and never commit or print it.

Your process needs outbound HTTPS to `https://cron.life`.

## Quickstart

BullMQ: pass the queues that hold Job Schedulers and the workers that run them.

```js
import { Queue, Worker } from 'bullmq'
import { monitorBullMQ } from 'cronradar-node-discovery'

const connection = { host: 'localhost', port: 6379 }
const queue = new Queue('reports', { connection })
const worker = new Worker('reports', async (job) => console.log('building', job.name), { connection })

await queue.upsertJobScheduler('nightly-report', { pattern: '0 2 * * *' }, { name: 'build' })
monitorBullMQ({ queues: [queue], workers: [worker] })
```

node-cron: schedule through the copy `monitorNodeCron` returns.

```js
const nodeCron = require('node-cron')
const { monitorNodeCron } = require('cronradar-node-discovery')

const cron = monitorNodeCron(nodeCron)
cron.schedule('0 2 * * *', () => console.log('backing up'), { name: 'nightly-backup' })
```

The package is CommonJS and loads with `import` or `require`; the BullMQ example is an ES module (`.mjs`), the node-cron one CommonJS.

## Annotations

There is nothing to annotate: every scheduled job is monitored unless you opt it out.

### What is discovered

| Framework | Discovered                                                                                                  | Monitor key                                    |
| --------- | ----------------------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| BullMQ    | Every Job Scheduler from `queue.getJobSchedulers()`, including legacy repeatable jobs                        | `<queue>-<scheduler id>`, e.g. `reports-nightly-report` |
| BullMQ 5 before 5.16 | Every repeatable job from `queue.getRepeatableJobs()`                                             | `<queue>-<job name>`, plus `-<jobId>` when set |
| node-cron | Every task created with the returned `schedule` or `createTask`                                             | The task's `name`, e.g. `nightly-backup`        |
| node-cron | A task without a `name`                                                                                     | Its schedule spelled out: `0 9 * * 1-5` is `cron-0-9-x-x-1r5` |

Keys are lowercase, with every run of other characters turned into one hyphen. A key over 100 characters keeps its first 91 characters and ends with a hyphen and 8 hex characters of its SHA-256, so it stays stable and distinct. Keys never contain anything random, so a restart or a second replica reports to the same monitors.

Give node-cron tasks a `name`. Unnamed tasks on the same schedule are told apart by the order they are scheduled in (`cron-0-8-x-x-x`, then `cron-0-8-x-x-x-2`), which changes if that order does. Two tasks with the same name: only the first is monitored, with a warning.

### Opting out

BullMQ, by scheduler ID (or legacy job name):

```js
monitorBullMQ({ queues: [queue], workers: [worker], skip: ['internal-cleanup'] })
```

Or in the scheduler's job template, from JavaScript:

```js
queue.upsertJobScheduler('internal-cleanup', { pattern: '*/10 * * * *' }, { name: 'cleanup', opts: { cronradar: false } })
```

BullMQ's TypeScript types do not know the `cronradar` option, so from TypeScript use `skip`.

node-cron, in the task's options or by name:

```js
const cron = monitorNodeCron(nodeCron, { skip: ['cache-warmup'] })
cron.schedule('*/10 * * * *', () => console.log('warming cache'), { name: 'cache-warmup' })
cron.schedule('0 4 * * *', () => console.log('rotating logs'), { name: 'log-rotation', cronradar: false })
```

node-cron never sees the `cronradar` option; it is removed before the task is created.

## Lifecycle

You don't report runs yourself.

| Framework event                                          | CronRadar  |
| -------------------------------------------------------- | ---------- |
| BullMQ worker `active`, first attempt                    | `start`    |
| BullMQ worker `completed`                                | `complete` |
| BullMQ worker `failed`, with no retries left             | `fail`     |
| node-cron task function called                           | `start`    |
| node-cron task function returns, or its promise resolves | `complete` |
| node-cron task function throws, or its promise rejects   | `fail`     |

A BullMQ job that is retried stays one run: the start is reported once, and a failure only after the last attempt (`attempts` in the job options). The failure message is the error's message, at most 1,000 characters; job data, results and stack traces are never sent.

The start request does not delay the job: it is sent while the job runs, and the terminal report waits for it so CronRadar receives them in order. Each start carries the schedule, so a run reported before its monitor is synced creates the monitor.

A node-cron task function's return value and errors reach node-cron unchanged; the very same error object is re-thrown.

## Reference

### `monitorBullMQ(options): BullMQMonitoring`

| Option     | Type       | Required | Meaning                                                                                 |
| ---------- | ---------- | -------- | --------------------------------------------------------------------------------------- |
| `queues`   | `Queue[]`  | yes      | Queues whose Job Schedulers are monitored.                                              |
| `workers`  | `Worker[]` | no       | Workers in this process whose runs are reported. Each needs its queue in `queues`.      |
| `skip`     | `string[]` | no       | Scheduler IDs or legacy repeatable job names to leave unmonitored.                      |
| `timeZone` | `string`   | no       | Your CronRadar application's time zone, such as `Europe/Berlin`, to warn about others.  |

It reconciles each queue at once and every 15 minutes, one request per queue with source `bullmq:<queue>`, and listens to the workers' `active`, `completed` and `failed` events. When a worker runs a job from a scheduler it has not seen, it rereads that queue (at most once a minute) and syncs before reporting the run. Never throws.

It returns `{ synced, stop() }`. `synced` is a promise that settles when the first sync of every queue has finished; it never rejects. `stop()` ends the periodic sync and removes the listeners:

```js
const monitoring = monitorBullMQ({ queues: [queue], workers: [worker] })
await monitoring.synced
process.on('SIGTERM', () => monitoring.stop())
```

Run `monitorBullMQ` in every process that runs a worker; processes that only add jobs need nothing. Every process reading a queue sees the same schedulers, so several replicas reconcile the same registry.

### `monitorNodeCron(cron, options?): typeof cron`

| Option     | Type       | Required | Meaning                                                    |
| ---------- | ---------- | -------- | ---------------------------------------------------------- |
| `skip`     | `string[]` | no       | Task names to leave unmonitored.                           |
| `timeZone` | `string`   | no       | Your CronRadar application's time zone, to warn about others. |

Returns a copy of the node-cron module whose `schedule` and `createTask` (node-cron 4) register and report each task. Everything else is node-cron's own. The module you pass in is not changed, so other code that imports node-cron is unaffected. Never throws; errors node-cron itself throws, such as for an invalid expression, still reach you.

Tasks are synced with source `node-cron` about a second after the last one is scheduled, and every 15 minutes. A task created with `createTask`, or with `scheduled: false` on node-cron 3, joins when it starts. A stopped or destroyed task leaves at the next 15-minute sync, so a graceful shutdown pauses nothing.

Why a returned copy and not a patch of `require('node-cron')`: a patch silently misses code that took `schedule` before it ran (`const { schedule } = require('node-cron')`, or any ES module named import), cannot change an ES module namespace at all, and changes the scheduler for every library in the process. The copy has none of these gaps and shows what is monitored at the call site. If a task is scheduled with node-cron directly, the next sync logs how many were missed.

### Schedules

| Schedule                                            | Sent to CronRadar                         |
| --------------------------------------------------- | ----------------------------------------- |
| Five fields, such as `30 2 * * 1-5`                  | As written                                |
| `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly` | As written |
| Six fields with seconds `0`, such as `0 30 2 * * *`  | The seconds dropped: `30 2 * * *`         |
| `?` in a day field                                   | `*`                                       |
| `7` for Sunday                                       | `0`                                       |
| Seconds other than `0`, such as `*/10 * * * * *`     | Not monitored, with a warning             |
| `L`, `W`, `#`, a year field, `@reboot`               | Not monitored, with a warning             |
| BullMQ `every` (an interval in milliseconds)         | Not monitored, with a warning             |

An `every` interval has no cron form. Give the scheduler a `pattern` instead, or report the job yourself with `wrap()` from the [`cronradar`](https://www.npmjs.com/package/cronradar) package and a key of your choosing.

### Time zones

CronRadar evaluates every schedule in the application's time zone, set in the application's settings (UTC when unset). BullMQ evaluates a scheduler in its `tz` option and node-cron a task in its `timezone` option; without one, both use the process's time zone, which is UTC in most containers. When the zones differ, CronRadar expects runs at the wrong time and alerts on them.

Set the application's time zone to the zone your jobs run in. Jobs in different zones belong in separate applications, with separate keys and processes.

The package warns once per job: with `timeZone` set, about each job that runs in another zone; without it, when one queue's or one process's jobs span more than one zone.

## Configuration

| Environment variable | Required | Meaning                                    |
| -------------------- | -------- | ------------------------------------------ |
| `CRONRADAR_API_KEY`  | yes      | The application's API key.                 |

Every request goes through the `cronradar` base SDK: to `https://cron.life`, with a 5-second timeout. New monitors start with a 60-second grace period; a grace period or schedule edited in the dashboard stays until the code declares a different schedule.

All processes that report node-cron tasks to one application must schedule the same tasks: each sync is the complete list for source `node-cron`, so a process with fewer tasks pauses the others' monitors. Give services with different node-cron tasks their own applications. BullMQ has no such limit, since each queue has its own source.

## Error handling

Monitoring never breaks a job:

- No function throws into your code. Every CronRadar error is caught and written to stderr with a `[CronRadar.BullMQ]` or `[CronRadar.NodeCron]` prefix (the base SDK uses `[CronRadar]`), and the job runs as it would without CronRadar.
- A job's own error is re-thrown unchanged, so node-cron and BullMQ handle it as before, retries included.
- Requests time out after 5 seconds and never delay a job's start; a node-cron task's result is returned before its completion is reported.
- If a queue's schedulers cannot be read, for example because Redis is down, nothing is sent for that queue, so its monitors are not paused; the next sync tries again.
- A network failure, `4xx` or `5xx` is logged and dropped. Runs reported while CronRadar was unreachable are not resent; missed-run detection covers them. The registry is resent within 15 minutes.
- If CronRadar rejects one job's schedule, the other jobs still sync, but CronRadar pauses nothing in that request until it is fixed.
- Without `CRONRADAR_API_KEY`, a warning is logged and nothing is sent.

## Troubleshooting

Every message is on stderr. Check what CronRadar holds, without putting the key in a URL:

```bash
curl --fail-with-body --silent --show-error --user "$CRONRADAR_API_KEY:" "https://cron.life/api/v1/sync/status"
```

| Symptom                                                      | Cause and fix                                                                                                    |
| ------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| `CRONRADAR_API_KEY is not set`                               | Set the variable in the process's environment, not only in your shell.                                           |
| `Failed to reconcile monitors ... HTTP 401`                  | The key is wrong or revoked. Rotate it in the application's settings.                                            |
| `HTTP 429` on sync                                           | Sync allows 10 requests a minute per key, one per queue. With more queues, the rest sync on the next interval.    |
| `repeats every 60000 ms, which has no cron form`             | A BullMQ `every` scheduler. See **Schedules**.                                                                   |
| `runs on a seconds field other than 0`                       | The job runs more than once a minute; CronRadar watches at minute resolution.                                   |
| `has no matching queue in monitorBullMQ({ queues })`         | Add the worker's queue to `queues`; workers alone cannot list schedulers.                                        |
| `were scheduled without monitorNodeCron`                     | Some code calls node-cron directly. Schedule those tasks with the copy `monitorNodeCron` returned.               |
| `runs a file, which CronRadar cannot observe`                | node-cron 4 background tasks run in another process. Use `wrap()` from `cronradar` in that file.                 |
| `runs in Asia/Tokyo, but the CronRadar application ...`       | See **Time zones**.                                                                                              |
| A BullMQ job's runs are not reported                         | The worker is not in `workers`, or the job was added with `queue.add` rather than by a scheduler.                |
| Monitors paused after a deploy of another service            | Two services report node-cron tasks to one application. See **Configuration**.                                   |
| `created_plan_limited` or `HTTP 402`                         | The plan's monitor limit or budget is reached; the extra jobs stay paused. Opt some out, or change plan.         |

## Verification

1. **Authentication and discovery.** Start your process, wait a few seconds, and run the `sync/status` command from **Troubleshooting**. Each scheduler appears by key, such as `reports-nightly-report`, with its schedule, and each node-cron task by its name.
2. **A completed run.** Run one scheduled job now. For BullMQ, upsert a scheduler that runs every minute and remove it afterwards:

   ```js
   import { Queue } from 'bullmq'

   const queue = new Queue('reports', { connection: { host: 'localhost', port: 6379 } })
   await queue.upsertJobScheduler('cronradar-check', { pattern: '* * * * *' }, { name: 'build' })
   await queue.close()
   ```

   Within two minutes the monitor `reports-cronradar-check` shows a run with result `success` in the app. Then run `await queue.removeJobScheduler('cronradar-check')`; the next sync pauses its monitor. For node-cron, call a task's function the way node-cron would with `await task.execute()` (node-cron 4) and look for the same result.
3. **A failed run.** Schedule a task that throws, such as `cron.schedule('* * * * *', () => { throw new Error('cronradar check') }, { name: 'cronradar-fail-check' })`. Within two minutes the monitor `cronradar-fail-check` is `critical` with the message `cronradar check`. Remove the task and its monitor afterwards.
4. **Notification delivery.** Send a test alert from the application's Alerts tab in the [dashboard](https://app.cronradar.com) and confirm it arrives.

## Migration

From `cronradar.wrap()` or manual `startJob` and `completeJob` calls around scheduled jobs:

1. Keep the monitor's history: for node-cron, set the task's `name` to the key you use now. BullMQ keys are `<queue>-<scheduler id>`; if yours differs, the new monitor starts fresh, and you pause or delete the old one in the dashboard once the new one reports.
2. Add `monitorBullMQ` or `monitorNodeCron`, deploy, and check one run of each job in the app.
3. Remove the manual calls. Until then a node-cron task with the same key is reported twice, which is harmless.

From a node-cron wrapper of another monitoring tool: remove its wrapper, schedule with `monitorNodeCron(nodeCron)`, and compare alerts for a few days before removing the old monitors.

To undo, remove the `monitorBullMQ` or `monitorNodeCron` call and uninstall the package. The monitors stay with their history but nothing reports to them, so they alert as missed runs: pause or delete them in the dashboard first.

```bash
npm uninstall cronradar-node-discovery
```

## Links

- CronRadar: [cronradar.com](https://cronradar.com)
- Documentation: [docs.cronradar.com](https://docs.cronradar.com)
- Ingestion API contract: [docs.cronradar.com/openapi.json](https://docs.cronradar.com/openapi.json)
- Cron schedule syntax: [docs.cronradar.com/schedules](https://docs.cronradar.com/schedules)
- Base SDK: [npmjs.com/package/cronradar](https://www.npmjs.com/package/cronradar)
- Source and issues: [github.com/notinferred/cronradar-node-discovery](https://github.com/notinferred/cronradar-node-discovery)
- License: [MIT](LICENSE)
