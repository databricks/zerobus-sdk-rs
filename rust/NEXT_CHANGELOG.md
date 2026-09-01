# NEXT CHANGELOG

## Release v2.9.0

### Major Changes

### New Features and Improvements

- Added a pluggable telemetry seam for Arrow Flight streams (Beta, `arrow-flight`).
  Register a `StatsExporter` via `stream_builder().stats_exporter(...)` to receive
  `StreamStat` events: `BatchSent { offset, stats }` — emitted when a batch is encoded
  and sent, where `stats` is a `BatchStats` carrying `records`, `wire_bytes` (actual
  on-wire bytes, after IPC compression), and `uncompressed_bytes` (Arrow payload size,
  codec-independent); `BatchAcked { offset }` (durability only); and `Reconnected`.
  `BatchSent` fires at send time, so it counts retransmits and fires even if the batch
  later fails. A built-in `channel_exporter(capacity)` forwards events to a bounded
  channel (dropping and counting when full) so a slow consumer never stalls ingestion.
  `record` runs inline on IO tasks, so keep it lightweight. New public items:
  `StatsExporter`, `StreamStat`, `BatchStats`, `ChannelExporter`, `channel_exporter`.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
