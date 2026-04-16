import Testing
@testable import Cachly

// MARK: - cosineSimilarity

@Suite("cosineSimilarity")
struct CosineSimilarityTests {

    @Test func identicalVectors_returnsOne() {
        let v = [1.0, 2.0, 3.0]
        #expect(abs(cosineSimilarity(v, v) - 1.0) < 1e-9)
    }

    @Test func orthogonalVectors_returnsZero() {
        #expect(abs(cosineSimilarity([1, 0], [0, 1]) - 0.0) < 1e-9)
    }

    @Test func zeroVector_returnsZero() {
        #expect(cosineSimilarity([0, 0], [1, 2]) == 0.0)
    }

    @Test func oppositeVectors_returnsNegativeOne() {
        #expect(abs(cosineSimilarity([1, 0], [-1, 0]) - (-1.0)) < 1e-9)
    }

    @Test func similarVectors_highSimilarity() {
        let sim = cosineSimilarity([0.9, 0.1, 0.0], [0.85, 0.15, 0.0])
        #expect(sim > 0.99)
    }

    @Test func differentLength_returnsZero() {
        #expect(cosineSimilarity([1, 2], [1]) == 0.0)
    }
}

// MARK: - SemanticResult

@Suite("SemanticResult")
struct SemanticResultTests {

    @Test func hitResult_containsSimilarity() {
        let r = SemanticResult(value: "answer", hit: true, similarity: 0.97)
        #expect(r.hit)
        #expect(abs((r.similarity ?? 0) - 0.97) < 1e-9)
    }

    @Test func missResult_nilSimilarity() {
        let r = SemanticResult(value: "answer", hit: false, similarity: nil)
        #expect(!r.hit)
        #expect(r.similarity == nil)
    }
}

// MARK: - SemanticOptions

@Suite("SemanticOptions")
struct SemanticOptionsTests {

    @Test func defaults() {
        let opts = SemanticOptions()
        #expect(abs(opts.similarityThreshold - 0.85) < 1e-9)
        #expect(opts.ttl == nil)
        #expect(opts.namespace == "cachly:sem")
    }

    @Test func customValues() {
        let opts = SemanticOptions(similarityThreshold: 0.92, ttl: .hours(1), namespace: "my:ns")
        #expect(abs(opts.similarityThreshold - 0.92) < 1e-9)
        #expect(opts.ttl != nil)
        #expect(opts.namespace == "my:ns")
    }

    // ── §3 useHybrid ──────────────────────────────────────────────────────────

    @Test func useHybrid_defaultsFalse() {
        let opts = SemanticOptions()
        #expect(opts.useHybrid == false)
    }

    @Test func useHybrid_canBeSetTrue() {
        let opts = SemanticOptions(useHybrid: true)
        #expect(opts.useHybrid == true)
    }

    @Test func useHybrid_independentOfOtherFields() {
        let opts = SemanticOptions(similarityThreshold: 0.90, useHybrid: true)
        #expect(opts.useHybrid == true)
        #expect(abs(opts.similarityThreshold - 0.90) < 1e-9)
        // Other fields still at their defaults
        #expect(opts.namespace == "cachly:sem")
        #expect(opts.quantize == "")
    }
}

// MARK: - §4 Namespace Auto-Detection

@Suite("SemanticCache.detectNamespace (§4)")
struct DetectNamespaceTests {

    // ── Code ──────────────────────────────────────────────────────────────────

    @Test("python def → code")
    func pythonDef_returnsCode() {
        #expect(SemanticCache.detectNamespace("def process(data): return data * 2") == "cachly:sem:code")
    }

    @Test("js const arrow → code")
    func jsConst_returnsCode() {
        #expect(SemanticCache.detectNamespace("const handler = () => response.send(200)") == "cachly:sem:code")
    }

    @Test("ts class → code")
    func tsClass_returnsCode() {
        #expect(SemanticCache.detectNamespace("class UserService { constructor(private r: Repo) {} }") == "cachly:sem:code")
    }

    @Test("import statement → code")
    func importStatement_returnsCode() {
        #expect(SemanticCache.detectNamespace("import { useState } from \"react\"") == "cachly:sem:code")
    }

    @Test("shebang line → code")
    func shebang_returnsCode() {
        #expect(SemanticCache.detectNamespace("#!/usr/bin/env python3") == "cachly:sem:code")
    }

    @Test("go func → code")
    func goFunc_returnsCode() {
        #expect(SemanticCache.detectNamespace("func main() { fmt.Println(\"hi\") }") == "cachly:sem:code")
    }

    @Test("cpp include → code")
    func cppInclude_returnsCode() {
        #expect(SemanticCache.detectNamespace("#include <iostream> int main() {}") == "cachly:sem:code")
    }

    @Test("interface block → code")
    func interfaceBlock_returnsCode() {
        #expect(SemanticCache.detectNamespace("interface ICache { get(key: string): void }") == "cachly:sem:code")
    }

    @Test("struct block → code")
    func structBlock_returnsCode() {
        #expect(SemanticCache.detectNamespace("struct Config { host string }") == "cachly:sem:code")
    }

    @Test("lambda → code")
    func lambda_returnsCode() {
        #expect(SemanticCache.detectNamespace("transform = lambda x: x * 2") == "cachly:sem:code")
    }

    // ── Translation ───────────────────────────────────────────────────────────

    @Test("translate → translation")
    func translate_returnsTranslation() {
        #expect(SemanticCache.detectNamespace("translate this paragraph to Spanish") == "cachly:sem:translation")
    }

    @Test("übersetze → translation")
    func uebersetze_returnsTranslation() {
        #expect(SemanticCache.detectNamespace("übersetze diesen Text bitte") == "cachly:sem:translation")
    }

    @Test("auf deutsch → translation")
    func aufDeutsch_returnsTranslation() {
        #expect(SemanticCache.detectNamespace("Schreib das auf deutsch") == "cachly:sem:translation")
    }

    @Test("traduis → translation")
    func traduis_returnsTranslation() {
        #expect(SemanticCache.detectNamespace("traduis ce texte en anglais") == "cachly:sem:translation")
    }

    // ── Summary ───────────────────────────────────────────────────────────────

    @Test("summarize → summary")
    func summarize_returnsSummary() {
        #expect(SemanticCache.detectNamespace("summarize this article for me") == "cachly:sem:summary")
    }

    @Test("tl;dr → summary")
    func tldr_returnsSummary() {
        #expect(SemanticCache.detectNamespace("tl;dr of the following blog post:") == "cachly:sem:summary")
    }

    @Test("key points → summary")
    func keyPoints_returnsSummary() {
        #expect(SemanticCache.detectNamespace("what are the key points?") == "cachly:sem:summary")
    }

    @Test("in a nutshell → summary")
    func inANutshell_returnsSummary() {
        #expect(SemanticCache.detectNamespace("explain machine learning in a nutshell") == "cachly:sem:summary")
    }

    @Test("zusammenfass → summary")
    func zusammenfass_returnsSummary() {
        #expect(SemanticCache.detectNamespace("fasse zusammen was in dem Text steht") == "cachly:sem:summary")
    }

    // ── Q&A ───────────────────────────────────────────────────────────────────

    @Test("what is → qa")
    func whatIs_returnsQa() {
        #expect(SemanticCache.detectNamespace("what is the capital of France?") == "cachly:sem:qa")
    }

    @Test("how does → qa")
    func howDoes_returnsQa() {
        #expect(SemanticCache.detectNamespace("how does photosynthesis work?") == "cachly:sem:qa")
    }

    @Test("wer ist → qa")
    func werIst_returnsQa() {
        #expect(SemanticCache.detectNamespace("wer ist der aktuelle Bundeskanzler?") == "cachly:sem:qa")
    }

    @Test("wie funktioniert → qa")
    func wieFunktioniert_returnsQa() {
        #expect(SemanticCache.detectNamespace("wie funktioniert ein JWT?") == "cachly:sem:qa")
    }

    @Test("trailing ? → qa")
    func trailingQuestionMark_returnsQa() {
        #expect(SemanticCache.detectNamespace("Redis vs Memcached?") == "cachly:sem:qa")
    }

    // ── Creative ──────────────────────────────────────────────────────────────

    @Test("poem request → creative")
    func poem_returnsCreative() {
        #expect(SemanticCache.detectNamespace("Write a short poem about autumn") == "cachly:sem:creative")
    }

    @Test("story request → creative")
    func story_returnsCreative() {
        #expect(SemanticCache.detectNamespace("Tell me a fantasy story about dragons") == "cachly:sem:creative")
    }

    @Test("product copy → creative")
    func productCopy_returnsCreative() {
        #expect(SemanticCache.detectNamespace("Generate a product description for running shoes") == "cachly:sem:creative")
    }

    // ── Edge cases ────────────────────────────────────────────────────────────

    @Test("detection is case-insensitive")
    func caseInsensitive() {
        #expect(SemanticCache.detectNamespace("CONST X = 1") == "cachly:sem:code")
        #expect(SemanticCache.detectNamespace("TRANSLATE TO GERMAN") == "cachly:sem:translation")
        #expect(SemanticCache.detectNamespace("SUMMARIZE THIS") == "cachly:sem:summary")
        #expect(SemanticCache.detectNamespace("WHAT IS GRAVITY?") == "cachly:sem:qa")
    }

    @Test("trims surrounding whitespace")
    func trims_whitespace() {
        #expect(SemanticCache.detectNamespace("   what is dark matter?   ") == "cachly:sem:qa")
        #expect(SemanticCache.detectNamespace("  const x = 1  ") == "cachly:sem:code")
    }

    @Test("code takes priority over translation")
    func code_precedence_over_translation() {
        // Both "translate" and "def" present – code wins (checked first)
        #expect(SemanticCache.detectNamespace("translate this function def foo(): pass") == "cachly:sem:code")
    }

    @Test("code takes priority over summary")
    func code_precedence_over_summary() {
        #expect(SemanticCache.detectNamespace("summarize this class MyService {}") == "cachly:sem:code")
    }

    @Test("empty string → creative")
    func emptyString_returnsCreative() {
        #expect(SemanticCache.detectNamespace("") == "cachly:sem:creative")
    }
}

// MARK: - §4 SemanticOptions.autoNamespace

@Suite("SemanticOptions.autoNamespace (§4)")
struct SemanticOptionsAutoNamespaceTests {

    @Test("autoNamespace defaults to false")
    func autoNamespace_defaultsFalse() {
        let opts = SemanticOptions()
        #expect(opts.autoNamespace == false)
    }

    @Test("autoNamespace can be set to true")
    func autoNamespace_canBeTrue() {
        let opts = SemanticOptions(autoNamespace: true)
        #expect(opts.autoNamespace == true)
    }

    @Test("autoNamespace is independent of threshold")
    func autoNamespace_independentOfThreshold() {
        let opts = SemanticOptions(similarityThreshold: 0.90, autoNamespace: true)
        #expect(opts.autoNamespace == true)
        #expect(abs(opts.similarityThreshold - 0.90) < 1e-9)
        #expect(opts.namespace == "cachly:sem")
    }
}

// MARK: - §8 WarmupEntry / WarmupResult

@Suite("SemanticCache WarmupEntry and WarmupResult (§8)")
struct WarmupTypesTests {

    @Test("WarmupEntry stores prompt and fn")
    func warmupEntry_storesValues() async throws {
        let entry = SemanticCache.WarmupEntry(
            prompt: "What is Redis?",
            fn: { "Redis is an in-memory store." }
        )
        #expect(entry.prompt == "What is Redis?")
        #expect(entry.namespace == nil)
        let result = try await entry.fn()
        #expect(result == "Redis is an in-memory store.")
    }

    @Test("WarmupEntry accepts optional namespace override")
    func warmupEntry_namespaceOverride() {
        let entry = SemanticCache.WarmupEntry(
            prompt: "What is Redis?",
            fn: { "answer" },
            namespace: "my:custom:ns"
        )
        #expect(entry.namespace == "my:custom:ns")
    }

    @Test("WarmupEntry nil namespace when not set")
    func warmupEntry_nilNamespaceByDefault() {
        let entry = SemanticCache.WarmupEntry(
            prompt: "hello",
            fn: { "world" }
        )
        #expect(entry.namespace == nil)
    }

    @Test("WarmupResult exposes warmed and skipped counts")
    func warmupResult_fields() {
        let r = SemanticCache.WarmupResult(warmed: 3, skipped: 1)
        #expect(r.warmed == 3)
        #expect(r.skipped == 1)
    }

    @Test("WarmupResult zero counts")
    func warmupResult_zeroCounts() {
        let r = SemanticCache.WarmupResult(warmed: 0, skipped: 0)
        #expect(r.warmed == 0)
        #expect(r.skipped == 0)
    }
}
