// Compile-only: the real BullMQ and node-cron objects satisfy the shapes CronRadar accepts.
import { Queue, Worker } from 'bullmq'
import cron from 'node-cron'
import * as cronNamespace from 'node-cron'
import { monitorBullMQ, monitorNodeCron, type BullMQMonitoring } from '../src'

declare const queue: Queue
declare const worker: Worker

const monitoring: BullMQMonitoring = monitorBullMQ({ queues: [queue], workers: [worker], skip: ['internal'], timeZone: 'UTC' })
monitoring.stop()

const monitoredCron: typeof cron = monitorNodeCron(cron, { skip: ['internal'] })
const task = monitoredCron.schedule('0 2 * * *', () => undefined, { name: 'nightly-backup', timezone: 'UTC' })
task.stop()
monitorNodeCron(cronNamespace).createTask('*/5 * * * *', () => undefined)
