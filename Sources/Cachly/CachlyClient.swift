import Foundation
import NIOCore
import NIOPosix
@preconcurrency import RediStack

// MARK: - Types

/// Three-level confidence band for a semantic cache hit.
public enum SemanticConfidence: String, Sendable {
    /// similarity ≥ highConfidenceThreshold (default 0.97). Serve directly.
    case high
    /// similarity ≥ threshold. Consider A/B logging.
    case medium
    /// Cache miss (similarity < threshold).
    case uncertain
}

/// Result of a ``SemanticCache/getOrSet(_:fn:embedFn:options:)`` call.
public struct SemanticResult<Value> {
    public let value: Value
    public let hit: Bool
    /// Cosine similarity score (0–1). `nil` on cache miss.
    public let similarity: Double?
    /// Confidence band. `nil` on cache miss.
    public let confidence: SemanticConfidence?

    public init(value: Value, hit: Bool, similarity: Double?, confidence: SemanticConfidence? = nil) {
        self.value      = value
        self.hit        = hit
        self.similarity = similarity
        self.confidence = confidence
    }
}

/// Options for a semantic cache operation.
public struct SemanticOptions: Sendable {
    /// Cosine similarity threshold (0–1). Default: `0.85`
    public var similarityThreshold: Double
    /// TTL for new cache entries. `nil` = no expiry.
    public var ttl: TimeAmount?
    /// Valkey key namespace prefix. Default: `"cachly:sem"`
    public var namespace: String
    /// Similarity ≥ this → `.high`. Default: `0.97`.
    public var highConfidenceThreshold: Double
    /// Strip filler words before embedding. Default: `true`. +8–12% hit-rate uplift.
    public var normalizePrompt: Bool
    /// §1 – use the server-side F1-calibrated threshold instead of `similarityThreshold`.
    /// Requires vectorUrl. Default: `false`.
    public var useAdaptiveThreshold: Bool
    /// §4 – auto-detect namespace from the prompt using text heuristics.
    /// Ignored when `namespace` is set to a custom value. Default: `false`.
    public var autoNamespace: Bool
    /// §7 – quantize embedding before sending to the pgvector API.
    /// `"int8"` reduces JSON payload ~8x with <1% quality loss.
    /// `""` or `"float32"` = full precision (default).
    public var quantize: String
    /// §3 – enable Hybrid BM25+Vector RRF fusion search.
    /// Passes `hybrid: true` and the normalised prompt to the pgvector API.
    /// +30 % precision for named entities, −20 % false-positive rate. Default: `false`.
    public var useHybrid: Bool

    public init(
        similarityThreshold: Double = 0.85,
        ttl: TimeAmount? = nil,
        namespace: String = "cachly:sem",
        highConfidenceThreshold: Double = 0.97,
        normalizePrompt: Bool = true,
        useAdaptiveThreshold: Bool = false,
        autoNamespace: Bool = false,
        quantize: String = "",
        useHybrid: Bool = false
    ) {
        self.similarityThreshold     = similarityThreshold
        self.ttl                     = ttl
        self.namespace               = namespace
        self.highConfidenceThreshold = highConfidenceThreshold
        self.normalizePrompt         = normalizePrompt
        self.useAdaptiveThreshold    = useAdaptiveThreshold
        self.autoNamespace           = autoNamespace
        self.quantize                = quantize
        self.useHybrid               = useHybrid
    }
}

// MARK: - CachlyError

public enum CachlyError: Error, LocalizedError {
    case serializationFailed(String)
    case connectionFailed(Error)
    case embedFunctionFailed(Error)

    public var errorDescription: String? {
        switch self {
        case .serializationFailed(let msg): return "Serialization failed: \(msg)"
        case .connectionFailed(let e):      return "Connection failed: \(e)"
        case .embedFunctionFailed(let e):   return "Embed function failed: \(e)"
        }
    }
}

// MARK: - New Result Types (SDK Feature Gap)

/// Cache statistics returned by `SemanticCache.stats(namespace:)`.
public struct CacheStats: Sendable {
    public let hits: Int
    public let misses: Int
    public let hitRate: Double
    public let total: Int
    public let namespaces: [[String: Any]]
}

/// One entry for bulk indexing via `SemanticCache.batchIndex(_:vectorUrl:)`.
public struct BatchIndexEntry: Sendable {
    public let id: String
    public let prompt: String
    public let embedding: [Double]
    public let namespace: String
    public let expiresAt: String?
    public init(id: String, prompt: String, embedding: [Double], namespace: String, expiresAt: String? = nil) {
        self.id = id; self.prompt = prompt; self.embedding = embedding
        self.namespace = namespace; self.expiresAt = expiresAt
    }
}

/// Result of `SemanticCache.batchIndex(_:vectorUrl:)`.
public struct BatchIndexResult: Sendable {
    public let indexed: Int
    public let skipped: Int
}

/// A single guardrail violation.
public struct GuardrailViolation: Sendable {
    public let type: String
    public let pattern: String
    public let action: String
}

/// Result of `SemanticCache.checkGuardrail(_:namespace:vectorUrl:)`.
public struct GuardrailCheckResult: Sendable {
    public let safe: Bool
    public let violations: [GuardrailViolation]
}

/// Tags associated with a cache key.
public struct TagsResult: Sendable {
    public let key: String
    public let tags: [String]
    public let ok: Bool
}

/// Result of `CachlyClient.invalidateTag(_:)`.
public struct InvalidateTagResult: Sendable {
    public let tag: String
    public let keysDeleted: Int
    public let keys: [String]
    public let durationMs: Int
}

/// One entry in the SWR (Stale-While-Revalidate) registry.
public struct SwrEntry: Sendable {
    public let key: String
    public let fetcherHint: String?
    public let staleFor: String?
    public let refreshAt: String?
}

/// Result of `CachlyClient.swrCheck()`.
public struct SwrCheckResult: Sendable {
    public let staleKeys: [SwrEntry]
    public let count: Int
    public let checkedAt: String
}

/// Result of `CachlyClient.bulkWarmup(_:)`.
public struct BulkWarmupResult: Sendable {
    public let warmed: Int
    public let skipped: Int
    public let durationMs: Int
}

/// Result of `SemanticCache.snapshotWarmup(namespace:limit:vectorUrl:)`.
public struct SnapshotWarmupResult: Sendable {
    public let warmed: Int
    public let durationMs: Int
}

/// LLM proxy statistics.
public struct LlmProxyStatsResult: Sendable {
    public let totalRequests: Int
    public let cacheHits: Int
    public let cacheMisses: Int
    public let estimatedSavedUsd: Double
    public let avgLatencyMsCached: Int
    public let avgLatencyMsUncached: Int
}

/// A Pub/Sub message.
public struct PubSubMessage: Sendable {
    public let channel: String
    public let message: String
    public let at: String
}

/// A workflow checkpoint.
public struct WorkflowCheckpoint: Sendable {
    public let id: String
    public let runId: String
    public let stepIndex: Int
    public let stepName: String
    public let agentName: String
    public let status: String
    public let state: String?
    public let output: String?
    public let durationMs: Int?
    public let createdAt: String?
}

/// Summary of a workflow run.
public struct WorkflowRun: Sendable {
    public let runId: String
    public let steps: Int
    public let latestStatus: String
    public let checkpoints: [WorkflowCheckpoint]
}

/// One entry for `CachlyClient.bulkWarmup(_:)`.
public struct BulkWarmupEntry: Sendable {
    public let key: String
    public let value: String
    public let ttl: Int?
    public init(key: String, value: String, ttl: Int? = nil) {
        self.key = key; self.value = value; self.ttl = ttl
    }
}

// MARK: - SemanticCache

/**
 Cache LLM responses by *meaning*, not just exact key.

 ### With pgvector (recommended – set `vectorUrl` on `CachlyClient.connect`)
 1. Embedding → `POST {vectorUrl}/search` → pgvector HNSW nearest-neighbour query
 2. On hit: fetch `{ns}:val:{id}` from Valkey
 3. On miss: write value to Valkey, `POST {vectorUrl}/entries` to index embedding

 ### Without pgvector (legacy fallback)
 Linear SCAN over `{ns}:emb:*` keys in Valkey. Suitable up to ~1 000 entries.
 Set `CACHLY_VECTOR_URL` to opt in to the scalable path.
 */
public actor SemanticCache {
    private let redis: RedisClient
    /// Base URL of the Cachly pgvector API: `https://api.cachly.dev/v1/sem/{vector_token}`
    private let vectorUrl: URL?

    /// Edge Worker URL for routing semantic-search reads (optional).
    private var edgeUrl: URL?

    init(redis: RedisClient, vectorUrl: URL?, edgeUrl: URL? = nil) {
        self.redis     = redis
        self.vectorUrl = vectorUrl
        self.edgeUrl   = edgeUrl
    }

    // MARK: Public API

    // ── §1 Adaptive Threshold ─────────────────────────────────────────────────

    /// Record whether a cache hit was accepted as correct (§1 Adaptive Threshold).
    /// Requires vectorUrl. No-op otherwise.
    ///
    /// - Parameters:
    ///   - hitId:      Entry UUID returned on a cache hit.
    ///   - accepted:   `true` if the cached answer was correct.
    ///   - similarity: Cosine similarity of the hit.
    ///   - namespace:  Key namespace.
    public func feedback(
        hitId: String,
        accepted: Bool,
        similarity: Double,
        namespace: String = "cachly:sem"
    ) async {
        guard let vUrl = vectorUrl, !hitId.isEmpty else { return }
        let url = vUrl.appendingPathComponent("feedback")
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let body: [String: Any] = [
            "hit_id": hitId, "accepted": accepted,
            "similarity": similarity, "namespace": namespace,
        ]
        req.httpBody = try? JSONSerialization.data(withJSONObject: body)
        _ = try? await URLSession.shared.data(for: req) // best-effort
    }

    /// Return the server-side F1-calibrated threshold for a namespace (§1).
    /// Falls back to `0.85` when no calibration data exists or vectorUrl is nil.
    public func adaptiveThreshold(namespace: String = "cachly:sem") async -> Double {
        guard let vUrl = vectorUrl else { return 0.85 }
        var url = vUrl.appendingPathComponent("threshold")
        url = appendingQuery(url: url, name: "namespace", value: namespace)
        guard let (data, _) = try? await URLSession.shared.data(from: url),
              let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let t = json["threshold"] as? Double else { return 0.85 }
        return t
    }

    // ── §4 Namespace Auto-Detection ───────────────────────────────────────────

    /// §4 – Classify a prompt into one of 5 semantic namespaces using text heuristics.
    ///
    /// Overhead: < 0.1 ms. No embedding required.
    /// Returns one of: `cachly:sem:code`, `:translation`, `:summary`, `:qa`, `:creative`
    public static func detectNamespace(_ prompt: String) -> String {
        let s = prompt.trimmingCharacters(in: .whitespaces).lowercased()
        let codeKw      = ["function ", "def ", "class ", "import ", "const ", "let ", "var ",
                           "return ", " => ", "void ", "public class", "func ", "#include", "package ",
                           "struct ", "interface ", "async def", "lambda ", "#!/"]
        let translKw    = ["translate", "übersetze", "auf deutsch", "auf englisch",
                           "in english", "in german", "ins deutsche", "ins englische", "übersetz",
                           "traduce", "traduis", "vertaal"]
        let summaryKw   = ["summarize", "summarise", "summary", "zusammenfass", "tl;dr", "tldr",
                           "key points", "stichpunkte", "fasse zusammen", "give me a brief",
                           "kurze zusammenfassung", "in a nutshell"]
        let qaPrefixes  = ["what ", "who ", "where ", "when ", "why ", "how ", "which ",
                           "is ", "are ", "was ", "were ", "does ", "do ", "did ",
                           "can ", "could ", "would ", "should ", "will ",
                           "wer ", "wie ", "wo ", "wann ", "warum ", "welche", "wieso "]
        if codeKw.contains(where: { s.contains($0) })        { return "cachly:sem:code" }
        if translKw.contains(where: { s.contains($0) })      { return "cachly:sem:translation" }
        if summaryKw.contains(where: { s.contains($0) })     { return "cachly:sem:summary" }
        if qaPrefixes.contains(where: { s.hasPrefix($0) })   { return "cachly:sem:qa" }
        if s.trimmingCharacters(in: .whitespaces).hasSuffix("?") { return "cachly:sem:qa" }
        return "cachly:sem:creative"
    }

    // ── §7 int8 Quantization ──────────────────────────────────────────────────

    /// Scalar-quantize a float64 embedding to int8 range [-128, 127] (§7).
    /// Reduces API JSON payload ~8x (1536-dim: 12 KB → 1.5 KB) with <1% quality loss.
    public static func quantizeEmbedding(_ vec: [Double]) -> [String: Any] {
        guard !vec.isEmpty else { return ["values": [Int](), "min": 0.0, "max": 0.0] }
        let minVal = vec.min()!
        let maxVal = vec.max()!
        let rng = maxVal - minVal
        let values: [Int]
        if rng > 0 {
            let scale = 255.0 / rng
            values = vec.map { v in
                let q = Int((scale * (v - minVal)).rounded()) - 128
                return max(-128, min(127, q))
            }
        } else {
            values = [Int](repeating: 0, count: vec.count)
        }
        return ["values": values, "min": minVal, "max": maxVal]
    }

    // ── getOrSet ──────────────────────────────────────────────────────────────

    /// Return a cached response for semantically similar prompts, or call `fn` and cache the result.
    ///
    /// - Parameters:
    ///   - prompt:  The user query / input text.
    ///   - fn:      Async closure executed on cache miss (e.g. LLM call).
    ///   - embedFn: Async closure that converts text to a floating-point vector.
    ///   - options: Threshold, TTL, namespace.
    public func getOrSet<T: Codable>(
        _ prompt: String,
        fn: () async throws -> T,
        embedFn: (String) async throws -> [Double],
        options: SemanticOptions = SemanticOptions()
    ) async throws -> SemanticResult<T> {
        let textForEmbed = options.normalizePrompt ? normalizePrompt(prompt) : prompt
        var opts = options

        // §4 – auto-detect namespace when requested and default ns is unchanged.
        if opts.autoNamespace && opts.namespace == "cachly:sem" {
            opts.namespace = SemanticCache.detectNamespace(prompt)
        }

        // §1 – fetch adaptive threshold when requested.
        if options.useAdaptiveThreshold, vectorUrl != nil {
            opts.similarityThreshold = await adaptiveThreshold(namespace: opts.namespace)
        }

        let queryEmbed = try await embedFn(textForEmbed)
        if let vUrl = edgeUrl ?? vectorUrl {
            return try await getOrSetViaAPI(textForEmbed, embed: queryEmbed, fn: fn, options: opts, baseUrl: vUrl)
        }
        return try await getOrSetViaScan(textForEmbed, originalPrompt: prompt, embed: queryEmbed, fn: fn, options: opts)
    }

    // ── §8 Cache-Warming ──────────────────────────────────────────────────────

    /// §8 – One entry to pre-warm into the semantic cache.
    public struct WarmupEntry<T: Codable & Sendable>: Sendable {
        public let prompt: String
        public let fn: @Sendable () async throws -> T
        /// Optional namespace override; `nil` falls back to `SemanticOptions.namespace`.
        public let namespace: String?
        public init(prompt: String, fn: @Sendable @escaping () async throws -> T, namespace: String? = nil) {
            self.prompt    = prompt
            self.fn        = fn
            self.namespace = namespace
        }
    }

    /// §8 – Result of a ``warmup(_:embedFn:options:)`` call.
    public struct WarmupResult: Sendable {
        public let warmed: Int   // freshly computed and indexed
        public let skipped: Int  // already present (cache hit during warmup)
    }

    /// §8 – Pre-warm the semantic cache with a list of prompt/fn pairs.
    ///
    /// Uses threshold=0.98 so already-cached entries are skipped.
    public func warmup<T: Codable & Sendable>(
        _ entries: [WarmupEntry<T>],
        embedFn: (String) async throws -> [Double],
        options: SemanticOptions = SemanticOptions(similarityThreshold: 0.98)
    ) async -> WarmupResult {
        var warmed = 0, skipped = 0
        let warmOpts = options.similarityThreshold <= 0
            ? SemanticOptions(similarityThreshold: 0.98)
            : options
        for entry in entries {
            var entryOpts = warmOpts
            if let ns = entry.namespace { entryOpts.namespace = ns }
            do {
                let result = try await getOrSet(entry.prompt, fn: entry.fn, embedFn: embedFn, options: entryOpts)
                result.hit ? (skipped += 1) : (warmed += 1)
            } catch {
                skipped += 1
            }
        }
        return WarmupResult(warmed: warmed, skipped: skipped)
    }

    // ── §8 Import from JSONL log ──────────────────────────────────────────────

    /// §8 – Import prompts from a JSONL file and warm the cache in batches.
    ///
    /// Each line must be a JSON object. The prompt is extracted from `promptField`
    /// (default: `"prompt"`). `responseFn` is called for every cache miss.
    ///
    /// - Parameters:
    ///   - filePath:    Path to a JSONL file.
    ///   - responseFn:  Async factory called on cache miss; receives the prompt.
    ///   - embedFn:     Embedding function.
    ///   - promptField: JSON key holding the prompt text (default: `"prompt"`).
    ///   - batchSize:   Prompts per batch (default: 50).
    ///   - options:     Semantic options forwarded to `warmup` (threshold defaults to 0.98).
    /// - Returns: `WarmupResult` with warmed/skipped counts.
    public func importFromLog<T: Codable & Sendable>(
        filePath: String,
        responseFn: @Sendable (String) async throws -> T,
        embedFn: (String) async throws -> [Double],
        promptField: String = "prompt",
        batchSize: Int = 50,
        options: SemanticOptions = SemanticOptions(similarityThreshold: 0.98)
    ) async -> WarmupResult {
        let field = promptField.isEmpty ? "prompt" : promptField
        let bs = batchSize > 0 ? batchSize : 50
        let warmOpts = options.similarityThreshold <= 0
            ? SemanticOptions(similarityThreshold: 0.98) : options

        guard let rawData = try? Data(contentsOf: URL(fileURLWithPath: filePath)),
              let content = String(data: rawData, encoding: .utf8) else {
            return WarmupResult(warmed: 0, skipped: 0)
        }
        let lines = content.components(separatedBy: .newlines).filter { !$0.trimmingCharacters(in: .whitespaces).isEmpty }

        var totalWarmed = 0, totalSkipped = 0

        for batchStart in stride(from: 0, to: lines.count, by: bs) {
            let batchEnd = min(batchStart + bs, lines.count)
            var entries: [WarmupEntry<T>] = []
            for line in lines[batchStart..<batchEnd] {
                guard let data = line.data(using: .utf8),
                      let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                      let prompt = obj[field] as? String, !prompt.isEmpty else {
                    totalSkipped += 1; continue
                }
                let p = prompt
                entries.append(WarmupEntry(prompt: p, fn: { try await responseFn(p) }))
            }
            let result = await warmup(entries, embedFn: embedFn, options: warmOpts)
            totalWarmed  += result.warmed
            totalSkipped += result.skipped
        }
        return WarmupResult(warmed: totalWarmed, skipped: totalSkipped)
    }

    /// Remove a single semantic cache entry.
    /// `key` may be a bare UUID or the legacy `{ns}:emb:{uuid}` format.
    /// Always deletes both the pgvector index entry and the Valkey value key.
    @discardableResult
    public func invalidate(_ key: String, namespace: String = "cachly:sem") async throws -> Bool {
        let id = uuidFrom(key: key)
        let vk = "\(namespace):val:\(id)"
        _ = try await redis.delete([RedisKey(vk)]).get()

        if let vUrl = vectorUrl {
            let url = vUrl.appendingPathComponent("entries").appendingPathComponent(id)
            var req = URLRequest(url: url)
            req.httpMethod = "DELETE"
            let (_, resp) = try await URLSession.shared.data(for: req)
            return (resp as? HTTPURLResponse)?.statusCode == 200
        }
        // Legacy: also delete the emb key from Valkey
        let embKey = "\(namespace):emb:\(id)"
        let deleted = try await redis.delete([RedisKey(embKey)]).get()
        return deleted > 0
    }

    /// List all cached entries. Returns tuples of (id, prompt).
    public func entries(namespace: String = "cachly:sem") async throws -> [(id: String, prompt: String)] {
        if let vUrl = vectorUrl {
            var url = vUrl.appendingPathComponent("entries")
            url = appendingQuery(url: url, name: "namespace", value: namespace)
            let (data, _) = try await URLSession.shared.data(from: url)
            struct Resp: Decodable { let data: [Entry] }
            struct Entry: Decodable { let id: String; let prompt: String }
            let resp = try JSONDecoder().decode(Resp.self, from: data)
            return resp.data.map { (id: $0.id, prompt: $0.prompt) }
        }
        // Legacy SCAN path
        let embKeys = try await scanAll(pattern: "\(namespace):emb:*")
        var result: [(id: String, prompt: String)] = []
        for embKey in embKeys {
            guard let rawStr = try? await redis.get(RedisKey(embKey), as: String.self).get(),
                  let data = rawStr.data(using: .utf8),
                  let entry = try? JSONDecoder().decode(SemEmbEntry.self, from: data)
            else { continue }
            result.append((id: uuidFrom(key: embKey), prompt: entry.originalPrompt ?? entry.prompt))
        }
        return result
    }

    /// Delete all entries in the namespace.
    /// - Returns: Number of logical entries deleted.
    @discardableResult
    public func flush(namespace: String = "cachly:sem") async throws -> Int {
        // Always clean up Valkey val keys.
        let valKeys = try await scanAll(pattern: "\(namespace):val:*")
        if !valKeys.isEmpty {
            _ = try await redis.delete(valKeys.map { RedisKey($0) }).get()
        }

        if let vUrl = vectorUrl {
            var url = vUrl.appendingPathComponent("flush")
            url = appendingQuery(url: url, name: "namespace", value: namespace)
            var req = URLRequest(url: url)
            req.httpMethod = "DELETE"
            let (data, _) = try await URLSession.shared.data(for: req)
            struct Resp: Decodable { let deleted: Int }
            let resp = (try? JSONDecoder().decode(Resp.self, from: data))
            return resp?.deleted ?? valKeys.count
        }
        // Legacy: also delete emb keys from Valkey.
        let embKeys = try await scanAll(pattern: "\(namespace):emb:*")
        if !embKeys.isEmpty {
            _ = try await redis.delete(embKeys.map { RedisKey($0) }).get()
        }
        return embKeys.count
    }

    /// O(1) with pgvector, O(n) SCAN otherwise.
    public func size(namespace: String = "cachly:sem") async throws -> Int {
        if let vUrl = vectorUrl {
            var url = vUrl.appendingPathComponent("size")
            url = appendingQuery(url: url, name: "namespace", value: namespace)
            let (data, _) = try await URLSession.shared.data(from: url)
            struct Resp: Decodable { let size: Int }
            return (try? JSONDecoder().decode(Resp.self, from: data))?.size ?? 0
        }
        return try await scanAll(pattern: "\(namespace):emb:*").count
    }

    // MARK: Private – pgvector API path

    private func getOrSetViaAPI<T: Codable>(
        _ prompt: String,
        embed: [Double],
        fn: () async throws -> T,
        options: SemanticOptions,
        baseUrl: URL
    ) async throws -> SemanticResult<T> {
        let ns = options.namespace
        let useInt8 = options.quantize.lowercased() == "int8"

        // 1. Search via pgvector
        let searchUrl = baseUrl.appendingPathComponent("search")
        var searchReq = URLRequest(url: searchUrl)
        searchReq.httpMethod = "POST"
        searchReq.setValue("application/json", forHTTPHeaderField: "Content-Type")

        // §7 – build search body with optional int8 quantization.
        var searchBody: [String: Any] = ["namespace": ns, "threshold": options.similarityThreshold]
        if useInt8 {
            searchBody["embedding_q8"] = SemanticCache.quantizeEmbedding(embed)
        } else {
            searchBody["embedding"] = embed
        }
        // §3 – hybrid BM25+Vector RRF: include prompt and set hybrid=true.
        if options.useHybrid && !prompt.isEmpty {
            searchBody["hybrid"] = true
            searchBody["prompt"] = prompt
        }
        searchReq.httpBody = try JSONSerialization.data(withJSONObject: searchBody)
        let (searchData, _) = try await URLSession.shared.data(for: searchReq)

        if let resp = try? JSONDecoder().decode(SearchResp.self, from: searchData),
           resp.found, let id = resp.id {
            let vk = "\(ns):val:\(id)"
            if let rawStr = try? await redis.get(RedisKey(vk), as: String.self).get(),
               let data = rawStr.data(using: .utf8),
               let value = try? JSONDecoder().decode(T.self, from: data) {
                let sim = resp.similarity ?? 0.0
                let conf = confidenceBand(sim, threshold: options.similarityThreshold,
                                          highThreshold: options.highConfidenceThreshold)
                return SemanticResult(value: value, hit: true, similarity: sim, confidence: conf)
            }
            // Orphaned index entry – fall through to miss.
        }

        // 2. Cache miss – run fn, persist value to Valkey, index embedding.
        let value = try await fn()
        let id = UUID().uuidString
        let vk = "\(ns):val:\(id)"
        let valPayload = String(data: try JSONEncoder().encode(value), encoding: .utf8) ?? "{}"

        if let ttl = options.ttl {
            let secs = Int(ttl.nanoseconds / 1_000_000_000)
            _ = try await redis.setex(RedisKey(vk), to: valPayload, expirationInSeconds: secs).get()
        } else {
            _ = try await redis.set(RedisKey(vk), to: valPayload).get()
        }

        // §7 – POST embedding to pgvector index (fire-and-forget).
        Task {
            var body: [String: Any] = ["id": id, "prompt": prompt, "namespace": ns]
            if useInt8 {
                body["embedding_q8"] = SemanticCache.quantizeEmbedding(embed)
            } else {
                body["embedding"] = embed
            }
            if let ttl = options.ttl {
                let secs = Double(ttl.nanoseconds) / 1_000_000_000.0
                body["expires_at"] = ISO8601DateFormatter().string(from: Date().addingTimeInterval(secs))
            }
            let entriesUrl = baseUrl.appendingPathComponent("entries")
            var req = URLRequest(url: entriesUrl)
            req.httpMethod = "POST"
            req.setValue("application/json", forHTTPHeaderField: "Content-Type")
            req.httpBody = try? JSONSerialization.data(withJSONObject: body)
            _ = try? await URLSession.shared.data(for: req)
        }

        return SemanticResult(value: value, hit: false, similarity: nil)
    }

    // MARK: Private – legacy SCAN path

    private func getOrSetViaScan<T: Codable>(
        _ prompt: String,
        originalPrompt: String,
        embed: [Double],
        fn: () async throws -> T,
        options: SemanticOptions
    ) async throws -> SemanticResult<T> {
        let ns = options.namespace
        let embKeys = try await scanAll(pattern: "\(ns):emb:*")

        var bestSim = -Double.infinity
        var bestValKey: String?

        for embKey in embKeys {
            guard let rawStr = try? await redis.get(RedisKey(embKey), as: String.self).get(),
                  let data = rawStr.data(using: .utf8),
                  let entry = try? JSONDecoder().decode(SemEmbEntry.self, from: data)
            else { continue }
            let sim = cosineSimilarity(embed, entry.embedding)
            if sim > bestSim {
                bestSim = sim
                bestValKey = valKeyFromEmb(embKey)
            }
        }

        if bestSim >= options.similarityThreshold, let vk = bestValKey {
            if let rawStr = try? await redis.get(RedisKey(vk), as: String.self).get(),
               let data = rawStr.data(using: .utf8),
               let value = try? JSONDecoder().decode(T.self, from: data) {
                let conf = confidenceBand(bestSim, threshold: options.similarityThreshold,
                                          highThreshold: options.highConfidenceThreshold)
                return SemanticResult(value: value, hit: true, similarity: bestSim, confidence: conf)
            }
        }

        // Miss
        let value = try await fn()
        let id = UUID().uuidString
        let vk = "\(ns):val:\(id)"
        let embKey = "\(ns):emb:\(id)"
        let valPayload = String(data: try JSONEncoder().encode(value), encoding: .utf8) ?? "{}"
        let entry = SemEmbEntry(embedding: embed, prompt: prompt,
                                originalPrompt: originalPrompt.isEmpty ? prompt : originalPrompt)
        let embPayload = String(data: try JSONEncoder().encode(entry), encoding: .utf8) ?? "{}"

        if let ttl = options.ttl {
            let secs = Int(ttl.nanoseconds / 1_000_000_000)
            _ = try await redis.setex(RedisKey(vk), to: valPayload, expirationInSeconds: secs).get()
            _ = try await redis.setex(RedisKey(embKey), to: embPayload, expirationInSeconds: secs).get()
        } else {
            _ = try await redis.set(RedisKey(vk), to: valPayload).get()
            _ = try await redis.set(RedisKey(embKey), to: embPayload).get()
        }
        return SemanticResult(value: value, hit: false, similarity: nil)
    }

    // MARK: Private – Valkey SCAN helper

    private func scanAll(pattern: String) async throws -> [String] {
        var keys: [String] = []
        var cursor = "0"
        repeat {
            let resp = try await redis.send(command: "SCAN", with: [
                RESPValue(from: cursor),
                RESPValue(from: "MATCH"),
                RESPValue(from: pattern),
                RESPValue(from: "COUNT"),
                RESPValue(from: "100"),
            ]).get()
            guard case .array(let parts) = resp, parts.count == 2 else { break }
            if case .bulkString(let buf) = parts[0],
               let cursorBuf = buf,
               let newCursor = String(bytes: cursorBuf.readableBytesView, encoding: .utf8) {
                cursor = newCursor
            }
            if case .array(let keyRESPs) = parts[1] {
                for k in keyRESPs {
                    if case .bulkString(let mb) = k,
                       let kb = mb,
                       let ks = String(bytes: kb.readableBytesView, encoding: .utf8) {
                        keys.append(ks)
                    }
                }
            }
        } while cursor != "0"
        return keys
    }

    // MARK: - New API Methods (SDK Feature Gap)

    /// Set the F1-calibrated similarity threshold for a namespace.
    /// `POST /v1/sem/:token/threshold`
    public func setThreshold(_ threshold: Double, namespace: String = "cachly:sem", vectorUrl: URL) async throws {
        let body: [String: Any] = ["namespace": namespace, "threshold": threshold]
        _ = try await httpPost(url: vectorUrl.appendingPathComponent("threshold"), body: body)
    }

    /// Return cache statistics. `GET /v1/sem/:token/stats`
    public func stats(namespace: String = "cachly:sem", vectorUrl: URL) async throws -> CacheStats {
        let url = appendingQuery(url: vectorUrl.appendingPathComponent("stats"), name: "namespace", value: namespace)
        guard let (data, _) = try? await URLSession.shared.data(from: url),
              let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            return CacheStats(hits: 0, misses: 0, hitRate: 0, total: 0, namespaces: [])
        }
        return CacheStats(
            hits:       (json["hits"]     as? Int) ?? 0,
            misses:     (json["misses"]   as? Int) ?? 0,
            hitRate:    (json["hit_rate"] as? Double) ?? 0,
            total:      (json["total"]    as? Int) ?? 0,
            namespaces: (json["namespaces"] as? [[String: Any]]) ?? []
        )
    }

    /// SSE-streaming semantic search. Returns text chunks as an AsyncThrowingStream.
    /// `POST /v1/sem/:token/search/stream`
    public func streamSearch(
        prompt: String,
        embedFn: (String) async throws -> [Double],
        namespace: String = "cachly:sem",
        threshold: Double = 0.85,
        vectorUrl: URL
    ) -> AsyncThrowingStream<String, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    let textForEmbed = normalizePrompt(prompt)
                    let embedding = try await embedFn(textForEmbed)
                    let body: [String: Any] = [
                        "embedding": embedding, "namespace": namespace,
                        "threshold": threshold, "prompt": prompt,
                    ]
                    let url = vectorUrl.appendingPathComponent("search/stream")
                    var req = URLRequest(url: url)
                    req.httpMethod = "POST"
                    req.setValue("application/json", forHTTPHeaderField: "Content-Type")
                    req.setValue("text/event-stream", forHTTPHeaderField: "Accept")
                    req.httpBody = try JSONSerialization.data(withJSONObject: body)
                    let (bytes, _) = try await URLSession.shared.bytes(for: req)
                    for try await line in bytes.lines {
                        guard line.hasPrefix("data:") else { continue }
                        let data = line.dropFirst("data:".count).trimmingCharacters(in: .whitespaces)
                        guard !data.isEmpty, data != "{}" else { continue }
                        guard let d = data.data(using: .utf8),
                              let json = try? JSONSerialization.jsonObject(with: d) as? [String: Any],
                              let text = json["text"] as? String, !text.isEmpty else { continue }
                        continuation.yield(text)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    /// Bulk-index up to 500 entries. `POST /v1/sem/:token/entries/batch`
    public func batchIndex(_ entries: [BatchIndexEntry], vectorUrl: URL) async throws -> BatchIndexResult {
        guard entries.count <= 500 else { throw CachlyError.serializationFailed("batchIndex: max 500 entries") }
        let body: [String: Any] = ["entries": entries.map { e -> [String: Any] in
            var d: [String: Any] = ["id": e.id, "prompt": e.prompt, "embedding": e.embedding, "namespace": e.namespace]
            if let exp = e.expiresAt { d["expires_at"] = exp }
            return d
        }]
        let resp = try await httpPost(url: vectorUrl.appendingPathComponent("entries/batch"), body: body)
        return BatchIndexResult(
            indexed: (resp?["indexed"] as? Int) ?? 0,
            skipped: (resp?["skipped"] as? Int) ?? 0
        )
    }

    /// Create a new vector index. `POST /v1/sem/:token/indexes`
    public func createIndex(
        namespace: String, dimensions: Int = 1536,
        model: String = "text-embedding-3-small", metric: String = "cosine",
        hybridEnabled: Bool = false, vectorUrl: URL
    ) async throws {
        _ = try await httpPost(url: vectorUrl.appendingPathComponent("indexes"), body: [
            "namespace": namespace, "dimensions": dimensions, "model": model,
            "metric": metric, "hybrid_enabled": hybridEnabled,
        ])
    }

    /// Delete an index. `DELETE /v1/sem/:token/indexes/:namespace`
    public func deleteIndex(namespace: String, vectorUrl: URL) async throws {
        let url = vectorUrl.appendingPathComponent("indexes/\(namespace)")
        var req = URLRequest(url: url)
        req.httpMethod = "DELETE"
        _ = try await URLSession.shared.data(for: req)
    }

    /// Attach metadata to an entry. `POST /v1/sem/:token/metadata`
    public func setMetadata(entryId: String, metadata: [String: Any], vectorUrl: URL) async throws {
        _ = try await httpPost(url: vectorUrl.appendingPathComponent("metadata"),
                               body: ["entry_id": entryId, "metadata": metadata])
    }

    /// Semantic search with metadata filter. `POST /v1/sem/:token/search/filtered`
    public func filteredSearch(
        prompt: String, embedFn: (String) async throws -> [Double],
        namespace: String = "cachly:sem", threshold: Double = 0.85,
        filter: [String: Any] = [:], limit: Int = 5, vectorUrl: URL
    ) async throws -> [String: Any]? {
        let embedding = try await embedFn(normalizePrompt(prompt))
        return try await httpPost(url: vectorUrl.appendingPathComponent("search/filtered"), body: [
            "prompt": prompt, "embedding": embedding, "namespace": namespace,
            "threshold": threshold, "filter": filter, "limit": limit,
        ])
    }

    /// Configure content-safety guardrails. `POST /v1/sem/:token/guardrails`
    public func setGuardrail(
        namespace: String = "cachly:sem", piiAction: String = "block",
        toxicAction: String = "flag", toxicThreshold: Double = 0.8, vectorUrl: URL
    ) async throws {
        _ = try await httpPost(url: vectorUrl.appendingPathComponent("guardrails"), body: [
            "namespace": namespace, "pii_action": piiAction,
            "toxic_action": toxicAction, "toxic_threshold": toxicThreshold,
        ])
    }

    /// Remove guardrail configuration. `DELETE /v1/sem/:token/guardrails/:namespace`
    public func deleteGuardrail(namespace: String, vectorUrl: URL) async throws {
        let url = vectorUrl.appendingPathComponent("guardrails/\(namespace)")
        var req = URLRequest(url: url); req.httpMethod = "DELETE"
        _ = try await URLSession.shared.data(for: req)
    }

    /// Check text against guardrails. `POST /v1/sem/:token/guardrails/check`
    public func checkGuardrail(text: String, namespace: String = "cachly:sem", vectorUrl: URL) async throws -> GuardrailCheckResult {
        let resp = try await httpPost(url: vectorUrl.appendingPathComponent("guardrails/check"),
                                     body: ["text": text, "namespace": namespace])
        let safe = (resp?["safe"] as? Bool) ?? true
        let rawViolations = (resp?["violations"] as? [[String: Any]]) ?? []
        let violations = rawViolations.map {
            GuardrailViolation(type: $0["type"] as? String ?? "",
                               pattern: $0["pattern"] as? String ?? "",
                               action: $0["action"] as? String ?? "")
        }
        return GuardrailCheckResult(safe: safe, violations: violations)
    }

    /// Re-warm from existing entries. `POST /v1/sem/:token/warmup/snapshot`
    public func snapshotWarmup(namespace: String = "cachly:sem", limit: Int = 100, vectorUrl: URL) async throws -> SnapshotWarmupResult {
        let resp = try await httpPost(url: vectorUrl.appendingPathComponent("warmup/snapshot"),
                                     body: ["namespace": namespace, "limit": limit])
        return SnapshotWarmupResult(
            warmed: (resp?["warmed"] as? Int) ?? 0,
            durationMs: (resp?["duration_ms"] as? Int) ?? 0
        )
    }

    // MARK: - Private HTTP helper

    @discardableResult
    private func httpPost(url: URL, body: [String: Any]) async throws -> [String: Any]? {
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (data, resp) = try await URLSession.shared.data(for: req)
        if let httpResp = resp as? HTTPURLResponse, httpResp.statusCode >= 400 {
            throw CachlyError.serializationFailed("cachly API POST \(url) → \(httpResp.statusCode)")
        }
        return try? JSONSerialization.jsonObject(with: data) as? [String: Any]
    }
}

// MARK: - CachlyClient

/// Official Swift client for [cachly.dev](https://cachly.dev) managed Valkey/Redis instances.
///
/// ### Quick start
/// ```swift
/// // Basic cache
/// let cache = try await CachlyClient.connect(url: "redis://:password@host:6379")
///
/// // With pgvector semantic index (get CACHLY_VECTOR_URL from the dashboard)
/// let cache = try await CachlyClient.connect(
///     url: "redis://:password@host:6379",
///     vectorUrl: "https://api.cachly.dev/v1/sem/YOUR_VECTOR_TOKEN"
/// )
///
/// // Semantic AI cache
/// let result: SemanticResult<String> = try await cache.semantic.getOrSet(
///     question,
///     fn:      { try await llm.ask(question) },
///     embedFn: { try await openAI.embed($0) }
/// )
/// ```
public final class CachlyClient: @unchecked Sendable {

    private let redis: RedisClient

    /// Semantic cache helper for AI/LLM workloads.
    public let semantic: SemanticCache

    private let batchUrl: URL?
    private let pubsubUrl: URL?
    private let workflowUrl: URL?
    private let llmProxyUrl: URL?
    private let edgeApiUrl: URL?

    private init(redis: RedisClient, vectorUrl: URL?, batchUrl: URL? = nil,
                 pubsubUrl: URL? = nil, workflowUrl: URL? = nil, llmProxyUrl: URL? = nil,
                 edgeApiUrl: URL? = nil, edgeReadUrl: URL? = nil) {
        self.redis       = redis
        self.semantic    = SemanticCache(redis: redis, vectorUrl: vectorUrl, edgeUrl: edgeReadUrl)
        self.batchUrl    = batchUrl
        self.pubsubUrl   = pubsubUrl
        self.workflowUrl = workflowUrl
        self.llmProxyUrl = llmProxyUrl
        self.edgeApiUrl  = edgeApiUrl
    }

    /// Pub/Sub client. `nil` when `pubsubUrl` was not supplied to `connect`.
    public var pubSub: PubSubClient? { pubsubUrl.map { PubSubClient(pubsubUrl: $0) } }

    /// Workflow checkpoint client. `nil` when `workflowUrl` was not supplied to `connect`.
    public var workflow: WorkflowClient? { workflowUrl.map { WorkflowClient(workflowUrl: $0) } }

    /// Edge Cache management client. `nil` when `edgeApiUrl` was not supplied to `connect`.
    public var edge: EdgeCacheClient? { edgeApiUrl.map { EdgeCacheClient(edgeApiUrl: $0) } }

    // MARK: Connect

    public static func connect(
        url urlString: String,
        vectorUrl vectorUrlString: String? = nil,
        batchUrl batchUrlString: String? = nil,
        pubsubUrl pubsubUrlString: String? = nil,
        workflowUrl workflowUrlString: String? = nil,
        llmProxyUrl llmProxyUrlString: String? = nil,
        edgeApiUrl edgeApiUrlString: String? = nil,
        edgeUrl edgeUrlString: String? = nil
    ) async throws -> CachlyClient {
        guard let url = URL(string: urlString) else {
            throw CachlyError.connectionFailed(URLError(.badURL))
        }
        let password = url.password
        let host = url.host ?? "localhost"
        let port = url.port ?? 6379

        let eventLoop = MultiThreadedEventLoopGroup.singleton.next()
        let config = try RedisConnection.Configuration(
            hostname: host,
            port: port,
            password: password,
            initialDatabase: nil
        )
        let conn = try await RedisConnection.make(configuration: config, boundEventLoop: eventLoop).get()
        let vUrl    = vectorUrlString.flatMap   { URL(string: $0) }
        let bUrl    = batchUrlString.flatMap    { URL(string: $0.trimmingCharacters(in: ["/"])) }
        let psUrl   = pubsubUrlString.flatMap   { URL(string: $0.trimmingCharacters(in: ["/"])) }
        let wfUrl   = workflowUrlString.flatMap { URL(string: $0.trimmingCharacters(in: ["/"])) }
        let llmUrl  = llmProxyUrlString.flatMap { URL(string: $0.trimmingCharacters(in: ["/"])) }
        let eApiUrl = edgeApiUrlString.flatMap  { URL(string: $0.trimmingCharacters(in: ["/"])) }
        let eReadUrl = edgeUrlString.flatMap    { URL(string: $0.trimmingCharacters(in: ["/"])) }
        return CachlyClient(redis: conn, vectorUrl: vUrl, batchUrl: bUrl,
                            pubsubUrl: psUrl, workflowUrl: wfUrl, llmProxyUrl: llmUrl,
                            edgeApiUrl: eApiUrl, edgeReadUrl: eReadUrl)
    }

    // MARK: Basic operations

    /// Get a JSON-decoded value. Returns `nil` when the key does not exist.
    public func get<T: Decodable>(_ key: String) async throws -> T? {
        guard let raw = try? await redis.get(RedisKey(key), as: String.self).get(),
              let data = raw.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(T.self, from: data)
    }

    /// Set a JSON-encoded value with an optional TTL.
    public func set<T: Encodable>(_ key: String, value: T, ttl: TimeAmount? = nil) async throws {
        let json = String(data: try JSONEncoder().encode(value), encoding: .utf8) ?? "{}"
        if let ttl {
            _ = try await redis.setex(RedisKey(key), to: json,
                                       expirationInSeconds: Int(ttl.nanoseconds / 1_000_000_000)).get()
        } else {
            _ = try await redis.set(RedisKey(key), to: json).get()
        }
    }

    /// Delete one or more keys. Returns the number of keys actually deleted.
    @discardableResult
    public func del(_ keys: String...) async throws -> Int {
        try await redis.delete(keys.map { RedisKey($0) }).get()
    }

    /// Check whether a key exists.
    public func exists(_ key: String) async throws -> Bool {
        try await redis.exists(RedisKey(key)).get() > 0
    }

    /// Update the TTL for an existing key.
    public func expire(_ key: String, ttl: TimeAmount) async throws {
        _ = try await redis.expire(RedisKey(key),
                                   after: .seconds(Int64(ttl.nanoseconds / 1_000_000_000))).get()
    }

    /// Return the remaining TTL for a key in seconds.
    /// Returns `-1` if the key has no expiry, `-2` if the key does not exist.
    public func ttl(_ key: String) async throws -> Int {
        let result = try await redis.send(command: "TTL", with: [RESPValue(bulk: key)]).get()
        return Int(result.int ?? -2)
    }

    /// Check connectivity to the cache server.
    /// Returns `true` if the server responds to PING.
    public func ping() async -> Bool {
        do {
            let result = try await redis.send(command: "PING", with: []).get()
            return result.string?.uppercased() == "PONG"
        } catch {
            return false
        }
    }

    /// Atomic counter increment. Returns the new value.
    @discardableResult
    public func incr(_ key: String) async throws -> Int {
        try await redis.increment(RedisKey(key)).get()
    }

    /// Return a cached value or call `fn`, persist and return the result.
    public func getOrSet<T: Codable>(_ key: String, ttl: TimeAmount? = nil, fn: () async throws -> T) async throws -> T {
        if let cached: T = try? await get(key) { return cached }
        let value = try await fn()
        try await set(key, value: value, ttl: ttl)
        return value
    }

    // MARK: Bulk operations

    /// One item in a bulk ``mset(_:)`` call.
    public struct MSetItem {
        public let key: String
        /// TTL for this entry. `nil` = no expiry.
        public let ttl: TimeAmount?
        /// Pre-encoded JSON string (encoded at construction time with the concrete type).
        let _encodedJSON: String

        /// Create an `MSetItem` with any `Encodable` value.
        public init<T: Encodable>(key: String, value: T, ttl: TimeAmount? = nil) {
            self.key = key
            self.ttl = ttl
            self._encodedJSON = (try? String(data: JSONEncoder().encode(value), encoding: .utf8)) ?? "null"
        }
    }

    /// Set multiple key-value pairs via individual SET commands (pipeline-equivalent).
    /// Supports per-key TTL – unlike native MSET.
    ///
    /// ```swift
    /// try await cache.mset([
    ///     .init(key: "user:1", value: alice, ttl: .seconds(300)),
    ///     .init(key: "user:2", value: bob),
    /// ])
    /// ```
    public func mset(_ items: [MSetItem]) async throws {
        try await withThrowingTaskGroup(of: Void.self) { group in
            for item in items {
                group.addTask { [item] in
                    try await self.setRawJSON(item.key, json: item._encodedJSON, ttl: item.ttl)
                }
            }
            try await group.waitForAll()
        }
    }

    /// Internal helper: write a pre-encoded JSON string to Redis.
    private func setRawJSON(_ key: String, json: String, ttl: TimeAmount? = nil) async throws {
        if let ttl {
            _ = try await redis.setex(RedisKey(key), to: json,
                                       expirationInSeconds: Int(ttl.nanoseconds / 1_000_000_000)).get()
        } else {
            _ = try await redis.set(RedisKey(key), to: json).get()
        }
    }

    /// Retrieve multiple keys in one round-trip (native MGET).
    /// Returns an array in the same order as `keys`; missing keys are `nil`.
    ///
    /// ```swift
    /// let [alice, bob] = try await cache.mget(["user:1", "user:2"]) as [User?]
    /// ```
    public func mget<T: Decodable>(_ keys: [String]) async throws -> [T?] {
        guard !keys.isEmpty else { return [] }
        let redisKeys = keys.map { RedisKey($0) }
        let raws = try await redis.mget(redisKeys, as: String.self).get()
        return raws.map { raw in
            guard let raw, let data = raw.data(using: .utf8) else { return nil }
            return try? JSONDecoder().decode(T.self, from: data)
        }
    }

    // MARK: Distributed lock

    /// Handle returned by a successful ``lock(_:ttl:retries:retryDelay:)`` call.
    /// Call ``LockHandle/release()`` in a `defer` block to free the lock early.
    public final class LockHandle: @unchecked Sendable {
        private let redis: RedisClient
        private let lockKey: RedisKey
        /// Unique fencing token for this lock acquisition.
        public let token: String
        private var released = false

        fileprivate init(redis: RedisClient, lockKey: RedisKey, token: String) {
            self.redis = redis; self.lockKey = lockKey; self.token = token
        }

        private static let releaseScript = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
        """

        /// Release the lock atomically. No-op if already expired or released.
        public func release() async throws {
            guard !released else { return }
            released = true
            // RediStack 1.6.x: use send(command:with:) – no eval() shorthand available
            _ = try? await redis.send(
                command: "EVAL",
                with: [
                    RESPValue(from: LockHandle.releaseScript),
                    RESPValue(from: 1),           // numkeys
                    RESPValue(from: lockKey.rawValue), // KEYS[1]
                    RESPValue(from: token),        // ARGV[1]
                ]
            ).get()
        }
    }

    /// Acquire a distributed lock using Redis `SET NX PX` (Redlock-lite pattern).
    ///
    /// Returns a ``LockHandle`` on success, or `nil` when all attempts are exhausted.
    /// The lock auto-expires after `ttl` to prevent deadlocks on process crash.
    ///
    /// ```swift
    /// guard let lock = try await cache.lock("job:invoice:42", ttl: .milliseconds(5000), retries: 5)
    /// else { throw CachlyError.custom("Resource busy") }
    /// defer { Task { try? await lock.release() } }
    /// try await processInvoice()
    /// ```
    public func lock(
        _ key: String,
        ttl: TimeAmount,
        retries: Int = 3,
        retryDelay: TimeAmount = .milliseconds(50)
    ) async throws -> LockHandle? {
        let lockKey = RedisKey("cachly:lock:\(key)")
        let token   = UUID().uuidString
        let ttlMs   = Int(ttl.nanoseconds / 1_000_000)

        for attempt in 0...retries {
            // RediStack 1.6.x: send(command:with:) returns EventLoopFuture<RESPValue>
            let rawResult = try? await redis.send(
                command: "SET",
                with: [
                    RESPValue(from: lockKey.rawValue),
                    RESPValue(from: token),
                    RESPValue(from: "NX"),
                    RESPValue(from: "PX"),
                    RESPValue(from: ttlMs),
                ]
            ).get()
            if rawResult?.string == "OK" {
                return LockHandle(redis: redis, lockKey: lockKey, token: token)
            }
            if attempt < retries {
                let delayNs = UInt64(retryDelay.nanoseconds)
                try await Task.sleep(nanoseconds: delayNs)
            }
        }
        return nil
    }

    // MARK: Batch API

    /// A single operation in a ``batch(_:)`` call.
    public struct BatchOp: Sendable, Encodable {
        public let op: String
        public let key: String
        public let value: String?
        public let ttl: Int?

        public static func get(_ key: String) -> BatchOp { BatchOp(op: "get", key: key, value: nil, ttl: nil) }
        public static func set(_ key: String, value: String, ttl: Int? = nil) -> BatchOp { BatchOp(op: "set", key: key, value: value, ttl: ttl) }
        public static func del(_ key: String) -> BatchOp { BatchOp(op: "del", key: key, value: nil, ttl: nil) }
        public static func exists(_ key: String) -> BatchOp { BatchOp(op: "exists", key: key, value: nil, ttl: nil) }
        public static func ttl(_ key: String) -> BatchOp { BatchOp(op: "ttl", key: key, value: nil, ttl: nil) }
    }

    /// Result of a single operation in a ``batch(_:)`` call.
    public struct BatchOpResult: Sendable {
        /// Value for "get" ops. `nil` = key not found.
        public let value: String?
        /// `true` if a "get" key existed.
        public let found: Bool
        /// `true` for successful "set" and "del" (≥1 key deleted).
        public let ok: Bool
        /// Result of "exists" op.
        public let exists: Bool
        /// Remaining TTL in seconds for "ttl" op. `-1` = no expiry, `-2` = not found.
        public let ttlSeconds: Int
    }

    /// Execute multiple cache operations in a **single round-trip**.
    ///
    /// When `batchUrl` is configured, all ops are sent via `POST {batchUrl}/batch` (one HTTP call).
    /// Otherwise the ops are executed concurrently via individual Redis commands.
    ///
    /// ```swift
    /// let results = try await cache.batch([
    ///     .get("user:1"),
    ///     .get("config:app"),
    ///     .set("visits", value: "\(Date().timeIntervalSince1970)", ttl: 86400),
    /// ])
    /// let user   = results[0].value   // String?
    /// let config = results[1].value   // String?
    /// let ok     = results[2].ok      // Bool
    /// ```
    public func batch(_ ops: [BatchOp]) async throws -> [BatchOpResult] {
        guard !ops.isEmpty else { return [] }
        if let bUrl = batchUrl {
            return try await batchViaHTTP(ops, baseURL: bUrl)
        }
        return try await batchViaRedis(ops)
    }

    private func batchViaHTTP(_ ops: [BatchOp], baseURL: URL) async throws -> [BatchOpResult] {
        struct ServerResult: Decodable {
            var value: String?
            var found: Bool?
            var ok: Bool?
            var deleted: Int?
            var exists: Bool?
            var ttl_seconds: Int?
            var error: String?
        }
        struct ServerResponse: Decodable { let results: [ServerResult] }

        var req = URLRequest(url: baseURL.appendingPathComponent("batch"))
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.httpBody = try JSONEncoder().encode(["ops": ops])
        let (data, _) = try await URLSession.shared.data(for: req)
        let resp = try JSONDecoder().decode(ServerResponse.self, from: data)

        return zip(ops, resp.results).map { op, r in
            switch op.op {
            case "get":
                return BatchOpResult(value: r.value, found: r.found ?? false, ok: false, exists: false, ttlSeconds: -2)
            case "set":
                return BatchOpResult(value: nil, found: false, ok: r.ok ?? false, exists: false, ttlSeconds: -2)
            case "del":
                return BatchOpResult(value: nil, found: false, ok: (r.deleted ?? 0) > 0, exists: false, ttlSeconds: -2)
            case "exists":
                return BatchOpResult(value: nil, found: false, ok: false, exists: r.exists ?? false, ttlSeconds: -2)
            case "ttl":
                return BatchOpResult(value: nil, found: false, ok: false, exists: false, ttlSeconds: r.ttl_seconds ?? -2)
            default:
                return BatchOpResult(value: nil, found: false, ok: false, exists: false, ttlSeconds: -2)
            }
        }
    }

    private func batchViaRedis(_ ops: [BatchOp]) async throws -> [BatchOpResult] {
        try await withThrowingTaskGroup(of: (Int, BatchOpResult).self) { group in
            for (i, op) in ops.enumerated() {
                group.addTask { [i, op] in
                    let res: BatchOpResult
                    switch op.op {
                    case "get":
                        let raw = try? await self.redis.get(RedisKey(op.key), as: String.self).get()
                        res = BatchOpResult(value: raw ?? nil, found: raw != nil, ok: false, exists: false, ttlSeconds: -2)
                    case "set":
                        if let secs = op.ttl {
                            _ = try? await self.redis.setex(RedisKey(op.key), to: op.value ?? "", expirationInSeconds: secs).get()
                        } else {
                            _ = try? await self.redis.set(RedisKey(op.key), to: op.value ?? "").get()
                        }
                        res = BatchOpResult(value: nil, found: false, ok: true, exists: false, ttlSeconds: -2)
                    case "del":
                        let n = (try? await self.redis.delete([RedisKey(op.key)]).get()) ?? 0
                        res = BatchOpResult(value: nil, found: false, ok: n > 0, exists: false, ttlSeconds: -2)
                    case "exists":
                        let n = (try? await self.redis.exists(RedisKey(op.key)).get()) ?? 0
                        res = BatchOpResult(value: nil, found: false, ok: false, exists: n > 0, ttlSeconds: -2)
                    case "ttl":
                        let t = (try? await self.redis.ttl(RedisKey(op.key)).get()) ?? -2
                        res = BatchOpResult(value: nil, found: false, ok: false, exists: false, ttlSeconds: t)
                    default:
                        res = BatchOpResult(value: nil, found: false, ok: false, exists: false, ttlSeconds: -2)
                    }
                    return (i, res)
                }
            }
            var output = [BatchOpResult?](repeating: nil, count: ops.count)
            for try await (i, r) in group { output[i] = r }
            return output.compactMap { $0 }
        }
    }

    // MARK: Streaming cache

    /// Cache a streaming response chunk-by-chunk via Redis `RPUSH`.
    /// Replay with ``streamGet(_:replayDelay:)``.
    ///
    /// ```swift
    /// try await cache.streamSet("chat:42", chunks: tokenStream, ttl: .seconds(3600))
    /// ```
    public func streamSet(
        _ key: String,
        chunks: AsyncThrowingStream<String, Error>,
        ttl: TimeAmount? = nil
    ) async throws {
        let listKey = RedisKey("cachly:stream:\(key)")
        _ = try await redis.delete([listKey]).get()
        for try await chunk in chunks {
            _ = try await redis.rpush([chunk], into: listKey).get()
        }
        if let ttl {
            _ = try await redis.expire(listKey, after: .seconds(Int64(ttl.nanoseconds / 1_000_000_000))).get()
        }
    }

    /// Retrieve a cached stream as an array of chunks.
    /// Returns `nil` on cache miss (key absent or empty list).
    ///
    /// ```swift
    /// if let chunks = try await cache.streamGet("chat:42") {
    ///     for chunk in chunks { print(chunk, terminator: "") }
    /// }
    /// ```
    public func streamGet(_ key: String) async throws -> [String]? {
        let listKey = RedisKey("cachly:stream:\(key)")
        let len = try await redis.llen(of: listKey).get()
        guard len > 0 else { return nil }
        let raws = try await redis.lrange(from: listKey, indices: 0...(-1)).get()
        return raws.compactMap { $0.string }
    }

    // MARK: - Tag-based invalidation

    /// Associate a key with tags. `POST /v1/cache/:token/tags`
    public func setTags(key: String, tags: [String]) async throws -> TagsResult {
        guard let base = batchUrl else { throw CachlyError.serializationFailed("batchUrl required for setTags") }
        let resp = try await httpPost(base.appendingPathComponent("tags"), body: ["key": key, "tags": tags])
        return TagsResult(
            key:  resp?["key"]  as? String ?? key,
            tags: resp?["tags"] as? [String] ?? [],
            ok:   resp?["ok"]   as? Bool ?? true
        )
    }

    /// Delete all keys with given tag. `POST /v1/cache/:token/invalidate`
    public func invalidateTag(_ tag: String) async throws -> InvalidateTagResult {
        guard let base = batchUrl else { throw CachlyError.serializationFailed("batchUrl required for invalidateTag") }
        let resp = try await httpPost(base.appendingPathComponent("invalidate"), body: ["tag": tag])
        return InvalidateTagResult(
            tag:        resp?["tag"]          as? String ?? tag,
            keysDeleted:(resp?["keys_deleted"] as? Int) ?? 0,
            keys:       resp?["keys"]          as? [String] ?? [],
            durationMs: (resp?["duration_ms"] as? Int) ?? 0
        )
    }

    /// Get tags for a key. `GET /v1/cache/:token/tags/:key`
    public func getTags(key: String) async throws -> TagsResult {
        guard let base = batchUrl else { throw CachlyError.serializationFailed("batchUrl required for getTags") }
        let url = base.appendingPathComponent("tags/\(key)")
        let (data, _) = try await URLSession.shared.data(from: url)
        let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
        return TagsResult(key: json?["key"] as? String ?? key,
                          tags: json?["tags"] as? [String] ?? [], ok: true)
    }

    /// Remove tag associations for a key. `DELETE /v1/cache/:token/tags/:key`
    public func deleteTags(key: String) async throws {
        guard let base = batchUrl else { throw CachlyError.serializationFailed("batchUrl required for deleteTags") }
        var req = URLRequest(url: base.appendingPathComponent("tags/\(key)"))
        req.httpMethod = "DELETE"
        _ = try await URLSession.shared.data(for: req)
    }

    // MARK: - Stale-While-Revalidate (SWR)

    /// Register a key for SWR. `POST /v1/cache/:token/swr/register`
    public func swrRegister(key: String, ttlSeconds: Int, staleWindowSeconds: Int, fetcherHint: String? = nil) async throws {
        guard let base = batchUrl else { throw CachlyError.serializationFailed("batchUrl required for swrRegister") }
        var body: [String: Any] = ["key": key, "ttl_seconds": ttlSeconds, "stale_window_seconds": staleWindowSeconds]
        if let hint = fetcherHint { body["fetcher_hint"] = hint }
        _ = try await httpPost(base.appendingPathComponent("swr/register"), body: body)
    }

    /// Query stale keys. `POST /v1/cache/:token/swr/check`
    public func swrCheck() async throws -> SwrCheckResult {
        guard let base = batchUrl else { throw CachlyError.serializationFailed("batchUrl required for swrCheck") }
        let resp = try await httpPost(base.appendingPathComponent("swr/check"), body: [:])
        let rawKeys = (resp?["stale_keys"] as? [[String: Any]]) ?? []
        let staleKeys = rawKeys.map { k -> SwrEntry in
            SwrEntry(key: k["key"] as? String ?? "",
                     fetcherHint: k["fetcher_hint"] as? String,
                     staleFor: k["stale_for"] as? String,
                     refreshAt: k["refresh_at"] as? String)
        }
        return SwrCheckResult(staleKeys: staleKeys,
                              count: (resp?["count"] as? Int) ?? staleKeys.count,
                              checkedAt: resp?["checked_at"] as? String ?? "")
    }

    /// Remove a key from the SWR registry. `DELETE /v1/cache/:token/swr/:key`
    public func swrRemove(key: String) async throws {
        guard let base = batchUrl else { throw CachlyError.serializationFailed("batchUrl required for swrRemove") }
        var req = URLRequest(url: base.appendingPathComponent("swr/\(key)"))
        req.httpMethod = "DELETE"
        _ = try await URLSession.shared.data(for: req)
    }

    // MARK: - Bulk Warmup

    /// Bulk-warm the KV cache. `POST /v1/cache/:token/warm`
    public func bulkWarmup(_ entries: [BulkWarmupEntry]) async throws -> BulkWarmupResult {
        guard let base = batchUrl else { throw CachlyError.serializationFailed("batchUrl required for bulkWarmup") }
        let body: [String: Any] = ["entries": entries.map { e -> [String: Any] in
            var d: [String: Any] = ["key": e.key, "value": e.value]
            if let ttl = e.ttl { d["ttl"] = ttl }
            return d
        }]
        let resp = try await httpPost(base.appendingPathComponent("warm"), body: body)
        return BulkWarmupResult(
            warmed:     (resp?["warmed"]      as? Int) ?? 0,
            skipped:    (resp?["skipped"]     as? Int) ?? 0,
            durationMs: (resp?["duration_ms"] as? Int) ?? 0
        )
    }

    // MARK: - LLM Proxy Stats

    /// Return LLM proxy statistics. `GET /v1/llm-proxy/:token/stats`
    public func llmProxyStats() async throws -> LlmProxyStatsResult {
        guard let base = llmProxyUrl else { throw CachlyError.serializationFailed("llmProxyUrl required") }
        let (data, _) = try await URLSession.shared.data(from: base.appendingPathComponent("stats"))
        let json = (try? JSONSerialization.jsonObject(with: data) as? [String: Any]) ?? [:]
        return LlmProxyStatsResult(
            totalRequests:        (json["total_requests"]          as? Int) ?? 0,
            cacheHits:            (json["cache_hits"]              as? Int) ?? 0,
            cacheMisses:          (json["cache_misses"]            as? Int) ?? 0,
            estimatedSavedUsd:    (json["estimated_saved_usd"]     as? Double) ?? 0,
            avgLatencyMsCached:   (json["avg_latency_ms_cached"]   as? Int) ?? 0,
            avgLatencyMsUncached: (json["avg_latency_ms_uncached"] as? Int) ?? 0
        )
    }

    // MARK: - Private HTTP helper

    @discardableResult
    private func httpPost(_ url: URL, body: [String: Any]) async throws -> [String: Any]? {
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (data, resp) = try await URLSession.shared.data(for: req)
        if let httpResp = resp as? HTTPURLResponse, httpResp.statusCode >= 400 {
            throw CachlyError.serializationFailed("cachly API POST \(url) → \(httpResp.statusCode)")
        }
        return try? JSONSerialization.jsonObject(with: data) as? [String: Any]
    }
}

// MARK: - Internal helpers

/// Decodable helper for `/search` API responses – must live outside generic functions.
private struct SearchResp: Decodable {
    let found: Bool
    let id: String?
    let similarity: Double?
}

private struct SemEmbEntry: Codable {
    let embedding: [Double]
    let prompt: String
    let originalPrompt: String?

    enum CodingKeys: String, CodingKey {
        case embedding
        case prompt
        case originalPrompt = "original_prompt"
    }
}

private let defaultFillerWords: [String] = [
    // EN
    "please", "hey", "hi", "hello",
    "could you", "can you", "would you", "will you",
    "just", "quickly", "briefly", "simply",
    "tell me", "show me", "give me", "help me", "assist me",
    "explain to me", "describe to me",
    "i need", "i want", "i would like", "i'd like", "i'm looking for",
    // DE
    "bitte", "mal eben", "schnell", "kurz", "einfach",
    "kannst du", "könntest du", "könnten sie", "würden sie", "würdest du",
    "hallo", "hi", "hey",
    "sag mir", "zeig mir", "gib mir", "hilf mir", "erkläre mir", "erklär mir",
    "ich brauche", "ich möchte", "ich hätte gerne", "ich suche",
    // FR
    "s'il vous plaît", "svp", "stp", "bonjour", "salut", "allô",
    "pouvez-vous", "pourriez-vous", "peux-tu", "pourrais-tu",
    "dis-moi", "dites-moi", "montre-moi", "montrez-moi",
    "j'ai besoin de", "je voudrais", "je cherche", "je souhaite",
    "expliquez-moi", "explique-moi", "aidez-moi", "aide-moi",
    // ES
    "por favor", "hola", "oye",
    "puedes", "podrías", "podría usted", "me puedes", "me podrías",
    "dime", "dígame", "muéstrame", "muéstreme", "dame", "deme",
    "necesito", "quisiera", "me gustaría", "quiero saber",
    "ayúdame", "ayúdeme", "explícame", "explíqueme",
    // IT
    "per favore", "perfavore", "ciao", "salve", "ehi",
    "potresti", "mi potresti", "potrebbe", "mi potrebbe",
    "dimmi", "mi dica", "mostrami", "dammi", "mi dia",
    "ho bisogno di", "vorrei", "mi piacerebbe",
    "aiutami", "mi aiuti", "spiegami", "mi spieghi",
    // PT
    "por favor", "olá", "oi", "ei",
    "pode", "poderia", "você poderia", "você pode", "podes",
    "me diga", "diga-me", "me mostre", "mostre-me", "me dê", "dê-me",
    "preciso de", "gostaria de", "quero saber", "estou procurando",
    "me ajude", "ajude-me", "explique-me", "me explique",
]

/// Strip filler words, lowercase, collapse whitespace. +8–12% semantic hit-rate uplift.
private func normalizePrompt(_ text: String, fillerWords: [String] = defaultFillerWords) -> String {
    var s = text.trimmingCharacters(in: .whitespaces).lowercased()
    for fw in fillerWords {
        s = s.replacingOccurrences(of: fw, with: "", options: [.caseInsensitive])
    }
    // Collapse multiple spaces
    while s.contains("  ") { s = s.replacingOccurrences(of: "  ", with: " ") }
    s = s.trimmingCharacters(in: .whitespaces)
    if s.hasSuffix("!") || s.hasSuffix("?") { s = String(s.dropLast()) + "?" }
    return s
}

/// Returns the confidence band for a similarity score.
private func confidenceBand(_ sim: Double, threshold: Double, highThreshold: Double) -> SemanticConfidence {
    if sim >= highThreshold { return .high }
    if sim >= threshold     { return .medium }
    return .uncertain
}

/// Derive `{ns}:val:{uuid}` from `{ns}:emb:{uuid}`.
private func valKeyFromEmb(_ embKey: String) -> String {
    guard let last = embKey.lastIndex(of: ":") else { return embKey }
    let uuidPart = embKey[last...]
    let nsType   = embKey[..<last]
    guard let secondLast = nsType.lastIndex(of: ":") else { return embKey }
    let ns = nsType[..<secondLast]
    return "\(ns):val\(uuidPart)"
}

/// Extract UUID from either a bare UUID string or a `{ns}:emb:{uuid}` / `{ns}:val:{uuid}` key.
private func uuidFrom(key: String) -> String {
    guard let last = key.lastIndex(of: ":") else { return key }
    return String(key[key.index(after: last)...])
}

/// Append a single query item to a URL.
private func appendingQuery(url: URL, name: String, value: String) -> URL {
    var comps = URLComponents(url: url, resolvingAgainstBaseURL: false) ?? URLComponents()
    var items = comps.queryItems ?? []
    items.append(URLQueryItem(name: name, value: value))
    comps.queryItems = items
    return comps.url ?? url
}

func cosineSimilarity(_ a: [Double], _ b: [Double]) -> Double {
    guard a.count == b.count else { return 0 }
    var dot = 0.0, normA = 0.0, normB = 0.0
    for i in 0 ..< a.count {
        dot   += a[i] * b[i]
        normA += a[i] * a[i]
        normB += b[i] * b[i]
    }
    let denom = normA.squareRoot() * normB.squareRoot()
    return denom == 0 ? 0 : dot / denom
}

// MARK: - PubSubClient

/// Pub/Sub client. Obtain via `CachlyClient.pubSub`.
public final class PubSubClient: @unchecked Sendable {
    private let pubsubUrl: URL
    init(pubsubUrl: URL) { self.pubsubUrl = pubsubUrl }

    private func httpPost(_ url: URL, body: [String: Any]) async throws -> [String: Any]? {
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (data, resp) = try await URLSession.shared.data(for: req)
        if let httpResp = resp as? HTTPURLResponse, httpResp.statusCode >= 400 {
            throw CachlyError.serializationFailed("cachly pubsub POST \(url) → \(httpResp.statusCode)")
        }
        return try? JSONSerialization.jsonObject(with: data) as? [String: Any]
    }

    /// Publish a message. `POST /v1/pubsub/:token/publish`
    public func publish(channel: String, message: String) async throws {
        _ = try await httpPost(pubsubUrl.appendingPathComponent("publish"),
                               body: ["channel": channel, "message": message])
    }

    /// Subscribe to channels via SSE. `POST /v1/pubsub/:token/subscribe`
    /// Returns an AsyncThrowingStream of `PubSubMessage`.
    public func subscribe(channels: [String]) -> AsyncThrowingStream<PubSubMessage, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    var req = URLRequest(url: pubsubUrl.appendingPathComponent("subscribe"))
                    req.httpMethod = "POST"
                    req.setValue("application/json", forHTTPHeaderField: "Content-Type")
                    req.setValue("text/event-stream", forHTTPHeaderField: "Accept")
                    req.httpBody = try JSONSerialization.data(withJSONObject: ["channels": channels])
                    let (bytes, _) = try await URLSession.shared.bytes(for: req)
                    for try await line in bytes.lines {
                        guard line.hasPrefix("data:") else { continue }
                        let data = line.dropFirst("data:".count).trimmingCharacters(in: .whitespaces)
                        guard !data.isEmpty, data != "{}",
                              let d = data.data(using: .utf8),
                              let json = try? JSONSerialization.jsonObject(with: d) as? [String: Any] else { continue }
                        continuation.yield(PubSubMessage(
                            channel: json["channel"] as? String ?? "",
                            message: json["message"] as? String ?? "",
                            at:      json["at"]      as? String ?? ""
                        ))
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    /// List active channels. `GET /v1/pubsub/:token/channels`
    public func channels() async throws -> [[String: Any]] {
        let (data, _) = try await URLSession.shared.data(from: pubsubUrl.appendingPathComponent("channels"))
        let json = (try? JSONSerialization.jsonObject(with: data) as? [String: Any]) ?? [:]
        return json["channels"] as? [[String: Any]] ?? []
    }

    /// Pub/Sub statistics. `GET /v1/pubsub/:token/stats`
    public func stats() async throws -> [String: Any] {
        let (data, _) = try await URLSession.shared.data(from: pubsubUrl.appendingPathComponent("stats"))
        return (try? JSONSerialization.jsonObject(with: data) as? [String: Any]) ?? [:]
    }
}

// MARK: - WorkflowClient

/// Workflow checkpoint client. Obtain via `CachlyClient.workflow`.
public final class WorkflowClient: @unchecked Sendable {
    private let workflowUrl: URL
    init(workflowUrl: URL) { self.workflowUrl = workflowUrl }

    private func httpPost(_ url: URL, body: [String: Any]) async throws -> [String: Any]? {
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (data, resp) = try await URLSession.shared.data(for: req)
        if let httpResp = resp as? HTTPURLResponse, httpResp.statusCode >= 400 {
            throw CachlyError.serializationFailed("cachly workflow POST \(url) → \(httpResp.statusCode)")
        }
        return try? JSONSerialization.jsonObject(with: data) as? [String: Any]
    }

    private func httpDelete(_ url: URL) async throws {
        var req = URLRequest(url: url); req.httpMethod = "DELETE"
        _ = try await URLSession.shared.data(for: req)
    }

    private func toCheckpoint(_ json: [String: Any]) -> WorkflowCheckpoint {
        WorkflowCheckpoint(
            id:        json["id"]         as? String ?? "",
            runId:     json["run_id"]     as? String ?? "",
            stepIndex: json["step_index"] as? Int    ?? 0,
            stepName:  json["step_name"]  as? String ?? "",
            agentName: json["agent_name"] as? String ?? "",
            status:    json["status"]     as? String ?? "",
            state:     json["state"]      as? String,
            output:    json["output"]     as? String,
            durationMs: json["duration_ms"] as? Int,
            createdAt: json["created_at"] as? String
        )
    }

    private func toRun(_ json: [String: Any]) -> WorkflowRun {
        let cps = (json["checkpoints"] as? [[String: Any]] ?? []).map { toCheckpoint($0) }
        return WorkflowRun(
            runId:        json["run_id"]        as? String ?? "",
            steps:        json["steps"]         as? Int    ?? cps.count,
            latestStatus: json["latest_status"] as? String ?? "",
            checkpoints:  cps
        )
    }

    /// Save a checkpoint. `POST /v1/workflow/:token/checkpoints`
    public func saveCheckpoint(
        runId: String, stepIndex: Int, stepName: String, agentName: String, status: String,
        state: String? = nil, output: String? = nil, durationMs: Int? = nil
    ) async throws -> WorkflowCheckpoint {
        var body: [String: Any] = [
            "run_id": runId, "step_index": stepIndex, "step_name": stepName,
            "agent_name": agentName, "status": status,
        ]
        if let s = state      { body["state"] = s }
        if let o = output     { body["output"] = o }
        if let d = durationMs { body["duration_ms"] = d }
        let resp = try await httpPost(workflowUrl.appendingPathComponent("checkpoints"), body: body)
        return toCheckpoint(resp ?? [:])
    }

    /// List all runs. `GET /v1/workflow/:token/runs`
    public func listRuns() async throws -> [WorkflowRun] {
        let (data, _) = try await URLSession.shared.data(from: workflowUrl.appendingPathComponent("runs"))
        let json = (try? JSONSerialization.jsonObject(with: data) as? [String: Any]) ?? [:]
        return (json["runs"] as? [[String: Any]] ?? []).map { toRun($0) }
    }

    /// Get checkpoints for a run. `GET /v1/workflow/:token/runs/:runId`
    public func getRun(_ runId: String) async throws -> WorkflowRun {
        let (data, _) = try await URLSession.shared.data(from: workflowUrl.appendingPathComponent("runs/\(runId)"))
        let json = (try? JSONSerialization.jsonObject(with: data) as? [String: Any]) ?? [:]
        return toRun(json)
    }

    /// Get the latest checkpoint. `GET /v1/workflow/:token/runs/:runId/latest`
    public func latestCheckpoint(runId: String) async throws -> WorkflowCheckpoint {
        let (data, _) = try await URLSession.shared.data(
            from: workflowUrl.appendingPathComponent("runs/\(runId)/latest"))
        let json = (try? JSONSerialization.jsonObject(with: data) as? [String: Any]) ?? [:]
        return toCheckpoint(json)
    }

    /// Delete a run. `DELETE /v1/workflow/:token/runs/:runId`
    public func deleteRun(_ runId: String) async throws {
        try await httpDelete(workflowUrl.appendingPathComponent("runs/\(runId)"))
    }
}

// MARK: - EdgeCacheClient (Feature #5)

/// Edge Cache configuration returned by ``EdgeCacheClient/getConfig()``.
public struct EdgeCacheConfig: Sendable {
    public let id: String
    public let instanceId: String
    public let enabled: Bool
    /// Cache TTL in seconds (0–86400).
    public let edgeTTL: Int
    public let workerURL: String
    public let cloudflareZoneID: String
    public let purgeOnWrite: Bool
    public let cacheSearchResults: Bool
    public let totalHits: Int
    public let totalMisses: Int
    /// Percentage: `totalHits / (totalHits + totalMisses) * 100`.
    public let hitRate: Double

    init(_ json: [String: Any]) {
        id                  = json["id"]                   as? String ?? ""
        instanceId          = json["instance_id"]          as? String ?? ""
        enabled             = json["enabled"]              as? Bool   ?? false
        edgeTTL             = json["edge_ttl"]             as? Int    ?? 60
        workerURL           = json["worker_url"]           as? String ?? "https://edge.cachly.dev"
        cloudflareZoneID    = json["cloudflare_zone_id"]   as? String ?? ""
        purgeOnWrite        = json["purge_on_write"]       as? Bool   ?? true
        cacheSearchResults  = json["cache_search_results"] as? Bool   ?? true
        totalHits           = json["total_hits"]           as? Int    ?? 0
        totalMisses         = json["total_misses"]         as? Int    ?? 0
        hitRate             = json["hit_rate"]             as? Double ?? 0.0
    }
}

/// Options for updating edge cache configuration.
/// Only non-`nil` fields are sent to the API.
public struct EdgeCacheConfigUpdate: Sendable {
    public var enabled: Bool?
    public var edgeTTL: Int?
    public var workerURL: String?
    public var cloudflareZoneID: String?
    public var purgeOnWrite: Bool?
    public var cacheSearchResults: Bool?

    public init(
        enabled: Bool? = nil,
        edgeTTL: Int? = nil,
        workerURL: String? = nil,
        cloudflareZoneID: String? = nil,
        purgeOnWrite: Bool? = nil,
        cacheSearchResults: Bool? = nil
    ) {
        self.enabled            = enabled
        self.edgeTTL            = edgeTTL
        self.workerURL          = workerURL
        self.cloudflareZoneID   = cloudflareZoneID
        self.purgeOnWrite       = purgeOnWrite
        self.cacheSearchResults = cacheSearchResults
    }

    func toDict() -> [String: Any] {
        var d: [String: Any] = [:]
        if let v = enabled            { d["enabled"]              = v }
        if let v = edgeTTL            { d["edge_ttl"]             = v }
        if let v = workerURL          { d["worker_url"]           = v }
        if let v = cloudflareZoneID   { d["cloudflare_zone_id"]   = v }
        if let v = purgeOnWrite       { d["purge_on_write"]       = v }
        if let v = cacheSearchResults { d["cache_search_results"] = v }
        return d
    }
}

/// Options for purging edge cache entries.
public struct EdgePurgeOptions: Sendable {
    /// Namespace patterns to purge (e.g. `["cachly:sem:qa"]`). `nil` = purge all.
    public var namespaces: [String]?
    /// Exact Cloudflare Worker URLs to purge.
    public var urls: [String]?

    public init(namespaces: [String]? = nil, urls: [String]? = nil) {
        self.namespaces = namespaces
        self.urls       = urls
    }

    func toDict() -> [String: Any] {
        var d: [String: Any] = [:]
        if let v = namespaces { d["namespaces"] = v }
        if let v = urls       { d["urls"]       = v }
        return d
    }
}

/// Result of a ``EdgeCacheClient/purge(_:)`` call.
public struct EdgePurgeResult: Sendable {
    /// Number of URLs accepted for purging by Cloudflare.
    public let purged: Int
    /// The exact URLs submitted for purging.
    public let urls: [String]

    init(_ json: [String: Any]) {
        purged = json["purged"] as? Int      ?? 0
        urls   = json["urls"]   as? [String] ?? []
    }
}

/// Hit/miss statistics returned by ``EdgeCacheClient/stats()``.
public struct EdgeCacheStats: Sendable {
    public let enabled: Bool
    public let workerURL: String
    public let edgeTTL: Int
    public let totalHits: Int
    public let totalMisses: Int
    /// Percentage: `totalHits / (totalHits + totalMisses) * 100`.
    public let hitRate: Double

    init(_ json: [String: Any]) {
        enabled     = json["enabled"]      as? Bool   ?? false
        workerURL   = json["worker_url"]   as? String ?? "https://edge.cachly.dev"
        edgeTTL     = json["edge_ttl"]     as? Int    ?? 60
        totalHits   = json["total_hits"]   as? Int    ?? 0
        totalMisses = json["total_misses"] as? Int    ?? 0
        hitRate     = json["hit_rate"]     as? Double ?? 0.0
    }
}

/// Edge Cache management client for cachly.dev (Feature #5).
///
/// Manages the Cloudflare Edge Cache for a cachly instance — configure TTL,
/// purge stale entries by namespace, and monitor hit/miss statistics.
///
/// Obtained via ``CachlyClient/edge`` when `edgeApiUrl` is configured in `connect`.
///
/// ```swift
/// let cache = try await CachlyClient.connect(
///     url: CACHLY_URL,
///     vectorUrl: CACHLY_VECTOR_URL,
///     edgeUrl: CACHLY_EDGE_URL,         // https://edge.cachly.dev/v1/sem/{token}
///     edgeApiUrl: CACHLY_EDGE_API_URL,  // https://api.cachly.dev/v1/edge/{token}
/// )
///
/// // Enable with 120s TTL
/// try await cache.edge?.setConfig(EdgeCacheConfigUpdate(enabled: true, edgeTTL: 120))
///
/// // Purge a namespace after a bulk write
/// try await cache.edge?.purge(EdgePurgeOptions(namespaces: ["cachly:sem:qa"]))
///
/// // Check hit rate
/// if let s = try await cache.edge?.stats() {
///     print("Edge hit rate: \(s.hitRate)%")
/// }
/// ```
public final class EdgeCacheClient: @unchecked Sendable {
    private let edgeApiUrl: URL

    init(edgeApiUrl: URL) { self.edgeApiUrl = edgeApiUrl }

    // MARK: Private HTTP helpers

    private func httpGet(_ url: URL) async throws -> [String: Any] {
        let (data, resp) = try await URLSession.shared.data(from: url)
        if let r = resp as? HTTPURLResponse, r.statusCode >= 400 {
            throw CachlyError.serializationFailed("cachly edge GET \(url) → \(r.statusCode)")
        }
        return (try? JSONSerialization.jsonObject(with: data) as? [String: Any]) ?? [:]
    }

    private func httpPut(_ url: URL, body: [String: Any]) async throws -> [String: Any] {
        var req = URLRequest(url: url)
        req.httpMethod = "PUT"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (data, resp) = try await URLSession.shared.data(for: req)
        if let r = resp as? HTTPURLResponse, r.statusCode >= 400 {
            throw CachlyError.serializationFailed("cachly edge PUT \(url) → \(r.statusCode)")
        }
        return (try? JSONSerialization.jsonObject(with: data) as? [String: Any]) ?? [:]
    }

    private func httpDelete(_ url: URL) async throws -> [String: Any] {
        var req = URLRequest(url: url); req.httpMethod = "DELETE"
        let (data, resp) = try await URLSession.shared.data(for: req)
        if let r = resp as? HTTPURLResponse, r.statusCode >= 400 {
            throw CachlyError.serializationFailed("cachly edge DELETE \(url) → \(r.statusCode)")
        }
        return (try? JSONSerialization.jsonObject(with: data) as? [String: Any]) ?? [:]
    }

    private func httpPost(_ url: URL, body: [String: Any]) async throws -> [String: Any] {
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (data, resp) = try await URLSession.shared.data(for: req)
        if let r = resp as? HTTPURLResponse, r.statusCode >= 400 {
            throw CachlyError.serializationFailed("cachly edge POST \(url) → \(r.statusCode)")
        }
        return (try? JSONSerialization.jsonObject(with: data) as? [String: Any]) ?? [:]
    }

    // MARK: Public API

    /// Get current edge cache configuration.
    /// Returns defaults with `enabled = false` when not yet configured.
    public func getConfig() async throws -> EdgeCacheConfig {
        EdgeCacheConfig(try await httpGet(edgeApiUrl.appendingPathComponent("config")))
    }

    /// Update edge cache configuration.
    ///
    /// - Parameter update: Fields to change; `nil` fields are left unchanged.
    /// - Returns: Updated ``EdgeCacheConfig``.
    public func setConfig(_ update: EdgeCacheConfigUpdate) async throws -> EdgeCacheConfig {
        EdgeCacheConfig(try await httpPut(edgeApiUrl.appendingPathComponent("config"), body: update.toDict()))
    }

    /// Disable and remove the edge cache configuration.
    @discardableResult
    public func deleteConfig() async throws -> [String: Any] {
        try await httpDelete(edgeApiUrl.appendingPathComponent("config"))
    }

    /// Purge cached entries from Cloudflare CDN.
    ///
    /// - No options → purges **all** cached entries for this instance.
    /// - `namespaces` → purges search results for those namespaces only.
    /// - `urls` → purges exact cache-key URLs.
    ///
    /// - Parameter options: ``EdgePurgeOptions`` or `nil` for a full purge.
    /// - Returns: ``EdgePurgeResult`` with count and URLs purged.
    @discardableResult
    public func purge(_ options: EdgePurgeOptions? = nil) async throws -> EdgePurgeResult {
        EdgePurgeResult(try await httpPost(edgeApiUrl.appendingPathComponent("purge"), body: options?.toDict() ?? [:]))
    }

    /// Return edge cache hit/miss statistics for this instance.
    public func stats() async throws -> EdgeCacheStats {
        EdgeCacheStats(try await httpGet(edgeApiUrl.appendingPathComponent("stats")))
    }
}
