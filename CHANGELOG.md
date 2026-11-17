# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0]

### Added

- `monitorBullMQ({ queues, workers })`: discovers every Job Scheduler of each queue (and legacy repeatable jobs), reconciles them to CronRadar at start and every 15 minutes with one source per queue, and reports start on `active`, completion on `completed`, and failure on `failed` once no retries are left.
- `monitorNodeCron(cron)`: returns a copy of node-cron 3 or 4 whose `schedule` and `createTask` register, sync and report every task, without changing the node-cron module.
- Schedule translation to five-field cron: shortcuts pass through, a seconds field of `0` is dropped, `?` becomes `*` and `7` becomes `0`; seconds other than `0`, `L`, `W`, `#` and BullMQ `every` intervals are skipped with a warning.
- Opt-out with `skip` lists, `{ cronradar: false }` in node-cron options or a BullMQ job template.
- Time-zone warnings against an optional `timeZone`, or when jobs span more than one zone.
