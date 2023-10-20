## Event streams

This package provides a utility for exposing an event stream over WebSockets and Webhooks.
- Connectivity:
  - WebSockets support for inbound connections
  - Webhooks support for outbound connections
- Reliability:
  - Workload managed mode: at-least-once delivery
  - Broadcast mode: at-most-once delivery
  - Batching for performance
  - Checkpointing for the at-least-once delivery assurance
- Convenience for packaging into apps:
  - Plug-in persistence (including allowing you multiple streams with CRUD.Scoped())
  - Out-of-the-box CRUD on event streams, using DB backed storage
  - Server-side `topicFilter` event filtering (regular expression)
- Semi-opinionated:
  - How batches are spelled
  - How WebSocket flow control payloads are spelled (`start`,`ack`,`nack`,`batch`)
- Flexibility:
  - Bring your own message payload (note `topic` and `sequenceId` always added)
  - Bring your own configuration type (must implement DB `Scan` & `Value` functions)

## Example

A simple in-memory command line pub/sub example is provided:
- See [ffpubsub.go](../../examples/ffpubsub.go)