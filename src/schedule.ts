export type Translation = { schedule: string } | { reason: string }

const MACROS = new Set(['@yearly', '@annually', '@monthly', '@weekly', '@daily', '@midnight', '@hourly'])
const UNSUPPORTED_TOKEN = /[LW#]/i
const DAY_OF_WEEK = 4

/**
 * Translates a node-cron or BullMQ (cron-parser) pattern to the five-field form CronRadar accepts.
 * A pattern whose meaning five fields cannot keep returns the reason instead.
 */
export function toCronRadarSchedule(pattern: string): Translation {
  const trimmed = pattern.trim()
  if (trimmed.startsWith('@')) {
    const macro = trimmed.toLowerCase()
    return MACROS.has(macro) ? { schedule: macro } : { reason: `"${trimmed}" is not a recurring schedule` }
  }

  let fields = trimmed.split(/\s+/)
  if (fields.length === 6) {
    if (!/^0+$/.test(fields[0])) {
      return { reason: `"${trimmed}" runs on a seconds field other than 0, which five-field cron cannot express` }
    }
    fields = fields.slice(1)
  }
  if (fields.length !== 5) {
    return { reason: `"${trimmed}" is not a five-field cron expression` }
  }

  // node-cron and cron-parser read "?" as "any", and 7 as Sunday; CronRadar spells them * and 0.
  fields = fields.map((field) => (field === '?' ? '*' : field))
  if (fields.some((field) => UNSUPPORTED_TOKEN.test(field.replace(/[a-z]{3}/gi, '')))) {
    return { reason: `"${trimmed}" uses L, W or #, which five-field cron cannot express` }
  }
  const dayOfWeek = sundayAsZero(fields[DAY_OF_WEEK])
  if (dayOfWeek === null) {
    return { reason: `"${trimmed}" uses 7 for Sunday inside a step; write Sunday as 0` }
  }
  fields[DAY_OF_WEEK] = dayOfWeek
  return { schedule: fields.join(' ') }
}

function sundayAsZero(field: string): string | null {
  const parts: string[] = []
  for (const part of field.split(',')) {
    if (part === '7') parts.push('0')
    else if (part === '0-7') parts.push('0-6')
    else if (/^[1-6]-7$/.test(part)) parts.push(`${part[0]}-6`, '0')
    else if (/(^|\D)7(\D|$)/.test(part)) return null
    else parts.push(part)
  }
  return [...new Set(parts)].join(',')
}
