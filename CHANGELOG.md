# Changelog – cachly SDK (swift)

**Language:** Swift  
**Package:** `CachlySDK` via **Swift Package Manager**

> Full cross-SDK release notes: [../CHANGELOG.md](../CHANGELOG.md)

---

## [0.2.0] – 2026-04-07

### Added

- **`mset(_ items: [MSetItem]) async throws`** – bulk set with per-key TTL via Redis pipeline
- **`mget(_ keys: [String]) async throws -> [Any?]`** – bulk get in one round-trip; nil on miss per key
- **`lock(_ key: String, options: LockOptions) async throws -> LockHandle?`** – distributed lock (SET NX PX + Lua release)
  - Returns `nil` when retries exhausted
  - `LockHandle.release()` for early, token-fenced unlock
  - Auto-expires after TTL to prevent deadlocks
- **`streamSet(_ key: String, chunks: AsyncStream<String>, options: StreamSetOptions?) async throws`** – cache token stream via RPUSH
- **`streamGet(_ key: String) async throws -> AsyncStream<String>?`** – replay stream; `nil` on miss

### Fixed

- `Known limitations` section updated – bulk ops now implemented

---

## [0.1.0-beta.1] – 2026-04-07

Initial beta release.

### Added

- `set(key:value:ttl:)` – store a value with optional TTL
- `get(key:)` – retrieve a value by key
- `delete(key:)` – remove a key
- `clear(namespace:)` – flush namespace or entire cache
- **Semantic cache:** `client.semantic.set(...)`, `.get(...)`, `.clear()`
- Namespace support via `withNamespace(_:)`
- API-key-based authentication
- TLS by default, EU data residency (German servers)
- Swift Concurrency (`async/await`) support

### Known limitations

- ~~Bulk operations (`mset` / `mget`) not yet implemented~~ ✅ resolved in v0.2.0
- Pub/Sub not yet supported

---

## [Unreleased]

See [../CHANGELOG.md](../CHANGELOG.md) for upcoming features.

