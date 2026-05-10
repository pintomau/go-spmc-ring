# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] — 2025-05-10

### Added
- Initial public release of `ringring`
- Single-writer, multiple-reader ring buffer (`RingBuffer[T]`)
- Lock-free reader lifecycle management (`BitmapReaderPool[T]`)
- Pipeline support (`Stage[T]`, `SetGatingStage`, `SetGatingBarrier`)
- Batch publish APIs (`PublishBatch`, `PublishBatchFunc`, `Reserve`/`Commit`)
- Configurable wait strategies (Spin, Yield, Sleep, Hybrid)
- Latency measurement CLI (`cmd/latency`)
- Cache-line padding for false-sharing elimination (64/128 byte support)
- Comprehensive benchmarks and property-based simulation tests
- Deterministic simulation via `-rbSeed` flag
- Pipeline staging with barrier-group semantics
