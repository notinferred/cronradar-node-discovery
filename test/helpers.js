'use strict'

const https = require('node:https')
const { EventEmitter } = require('node:events')

process.env.CRONRADAR_API_KEY = 'ck_app_test_secret'

/**
 * Replaces https.request, which the cronradar base SDK calls, so no request leaves the process.
 * `respond` returns a status code or an Error for each recorded request.
 */
function mockHttp(t, respond = () => 200) {
  const requests = []
  t.mock.method(https, 'request', (options, callback) => {
    const request = new EventEmitter()
    let body = ''
    request.write = (chunk) => {
      body += chunk
    }
    request.destroy = () => {}
    request.end = () => {
      const url = new URL(options.path, 'https://cron.life')
      const record = {
        method: options.method,
        path: url.pathname,
        query: Object.fromEntries(url.searchParams),
        headers: options.headers,
        body: body ? JSON.parse(body) : undefined,
      }
      requests.push(record)
      setImmediate(() => {
        const outcome = respond(record)
        if (outcome instanceof Error) {
          request.emit('error', outcome)
          return
        }
        const response = new EventEmitter()
        response.statusCode = outcome
        response.headers = {}
        callback(response)
        response.emit('end')
      })
    }
    return request
  })
  return requests
}

/** Collects stderr lines written through console.error. */
function captureErrors(t) {
  const lines = []
  t.mock.method(console, 'error', (...args) => lines.push(args.join(' ')))
  return lines
}

function settle(ms = 30) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

const syncs = (requests) => requests.filter((r) => r.path === '/api/v1/sync')
const pings = (requests) =>
  requests
    .filter((r) => r.path.startsWith('/api/v1/ping/'))
    .map((r) => {
      const [, , , , key, , event] = r.path.split('/')
      return { key: decodeURIComponent(key), event: event ?? 'heartbeat', query: r.query }
    })

module.exports = { mockHttp, captureErrors, settle, syncs, pings }
