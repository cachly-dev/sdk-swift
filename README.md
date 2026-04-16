# Cachly Swift SDK

Official Swift SDK for [cachly.dev](https://cachly.dev) – Managed Valkey/Redis cache.

**DSGVO-compliant · German servers · 30s provisioning**  
**iOS 17+ · macOS 14+ · Server-side Swift (Vapor) · async/await native**

## Installation

```swift
// Package.swift
.package(url: "https://github.com/cachly-dev/sdk-swift.git", from: "0.1.0-beta.1"),
// target dependencies:
.product(name: "Cachly", package: "sdk-swift"),
```

Or in Xcode: **File → Add Package Dependencies** → paste URL above.

## Quick Start

```swift
import Cachly

let cache = try await CachlyClient.connect(url: ProcessInfo.processInfo.environment["CACHLY_URL"]!)

// Set with TTL
try await cache.set("user:42", value: User(name: "Alice", plan: "pro"), ttl: .seconds(300))

// Get
let user: User? = try await cache.get("user:42")

// Get-or-set
let report: Report = try await cache.getOrSet("report:monthly", ttl: .seconds(60)) {
    try await db.heavyQuery()
}

// Atomic counter
let count = try await cache.incr("page:views")

// Delete
try await cache.del("user:42")
```

## Semantic AI Cache (Speed / Business tiers)

Cache LLM responses by *meaning*, not just exact key. Cut OpenAI costs by 60%.

```swift
import Cachly

let result: SemanticResult<String> = try await cache.semantic.getOrSet(
    userQuestion,
    fn: { try await openAI.ask(userQuestion) },
    embedFn: { text in try await openAI.embed(text) },
    options: SemanticOptions(
        similarityThreshold: 0.92,
        ttl: .hours(1)
    )
)

if result.hit {
    print("⚡ Cache hit – similarity: \(result.similarity!)")
} else {
    print("🔄 Fresh from LLM")
}
print(result.value)
```

## Vapor Integration

```swift
import Vapor
import Cachly

// configure.swift
public func configure(_ app: Application) async throws {
    let cache = try await CachlyClient.connect(
        url: Environment.get("CACHLY_URL")!)
    app.storage[CachlyKey.self] = cache
}

struct CachlyKey: StorageKey { typealias Value = CachlyClient }

extension Request {
    var cachly: CachlyClient { application.storage[CachlyKey.self]! }
}

// In a route handler:
app.get("user", ":id") { req async throws -> User in
    let id = req.parameters.get("id")!
    return try await req.cachly.getOrSet("user:\(id)", ttl: .seconds(300)) {
        try await User.find(id, on: req.db).unwrap(or: Abort(.notFound))
    }
}
```

## iOS Usage

```swift
// Works great in SwiftUI with `@MainActor` / `Task { }`:
struct ContentView: View {
    @State private var answer = ""

    var body: some View {
        Button("Ask AI") {
            Task {
                let result: SemanticResult<String> = try await cache.semantic.getOrSet(
                    question,
                    fn: { try await openAI.ask(question) },
                    embedFn: { try await openAI.embed($0) }
                )
                answer = result.value
            }
        }
        Text(answer)
    }
}
```

## API Reference

| Method | Description |
|---|---|
| `connect(url:)` | Async factory – connect to cachly instance |
| `get<T>(_ key)` | Async – get Codable value (`nil` if not found) |
| `set(_ key, value:, ttl:)` | Async – set Codable value |
| `del(_ keys...)` | Async – delete keys, returns count |
| `exists(_ key)` | Async – check existence |
| `expire(_ key, ttl:)` | Async – update TTL |
| `incr(_ key)` | Async – atomic increment |
| `getOrSet(_ key, ttl:, fn:)` | Async – get-or-set pattern |
| `semantic.getOrSet(...)` | Async – semantic AI cache |
| `semantic.flush(namespace:)` | Async – flush namespace |
| `semantic.size(namespace:)` | Async – entry count |

## Batch API – mehrere Ops in einem Round-Trip

Bündelt GET/SET/DEL/EXISTS/TTL-Ops in **einem** HTTP-Request oder einer RediStack-Pipeline.

```swift
let cache = try await CachlyClient(
    url: ProcessInfo.processInfo.environment["CACHLY_URL"]!,
    batchURL: ProcessInfo.processInfo.environment["CACHLY_BATCH_URL"]  // optional
)

let results = try await cache.batch([
    .get("user:1"),
    .get("config:app"),
    .set("visits", value: "42", ttl: 86400),
    .exists("session:xyz"),
    .ttl("token:abc"),
])

let user : String? = results[0].value        // nil on miss
let ok   : Bool    = results[2].ok
let found: Bool    = results[3].exists
let secs : Int64   = results[4].ttlSeconds   // -1 = kein TTL, -2 = nicht vorhanden
```

## Environment Variables

```bash
CACHLY_URL=redis://:your-password@my-app.cachly.dev:30101
CACHLY_BATCH_URL=https://api.cachly.dev/v1/cache/YOUR_TOKEN   # optional
# Speed / Business tier – Semantic AI Cache:
CACHLY_VECTOR_URL=https://api.cachly.dev/v1/sem/your-vector-token
```

Find both values in your [cachly.dev dashboard](https://cachly.dev/instances).

## Quality Gates

```bash
# Build (CommandLineTools oder Xcode)
swift build          # ✅ Build complete!

# Tests (benötigt Xcode.app – nicht nur CommandLineTools)
swift test           # ✅ alle Tests grün (mit Xcode 16+)
# Hinweis: @Test-Macro-Discovery funktioniert nur mit Xcode.app,
# nicht mit CommandLineTools allein (macOS-Limitation).
# Tests compilieren und linken in beiden Umgebungen korrekt.
```

## License

MIT  [cachly.dev](https://cachly.dev)

