import Foundation

/// JSON envelope shape every ingest endpoint expects: `{"records": [...]}`.
/// Hoisted to file scope because Swift doesn't allow a generic struct to be
/// nested inside a generic function (`EAService.postRecords<T>`).
private struct EAIngestEnvelope<U: Encodable>: Encodable {
    let records: [U]
}

/// Thin HTTP client for EA's HealthBeat ingest API.
///
/// One actor per `EAConfig` snapshot; instantiated by
/// {@link EABackendWriter} for the duration of a sync pass. Auth is a
/// bearer sync key (separate from any other EA credentials). Errors are
/// surfaced verbatim — nothing is swallowed.
actor EAService {
    enum EAError: Error, CustomStringConvertible, LocalizedError {
        case notConfigured
        case http(Int, String?)
        case transport(Error)
        case decoding(Error)
        case invalidURL

        var description: String {
            switch self {
            case .notConfigured:           return "EA destination is not configured"
            case .http(let code, let body):
                // Trim very long bodies (HTML error pages, full SQL dumps)
                // so they're still useful in a SwiftUI Text but don't blow
                // up the view.
                let snippet = body.map { $0.count > 600 ? String($0.prefix(600)) + "…" : $0 }
                return "EA HTTP \(code)\(snippet.map { ": \($0)" } ?? "")"
            case .transport(let e):        return "EA transport: \(e.localizedDescription)"
            case .decoding(let e):         return "EA decoding: \(e.localizedDescription)"
            case .invalidURL:              return "EA URL is invalid"
            }
        }

        // `localizedDescription` (and Foundation's NSError bridging) reads
        // from `errorDescription` for any `LocalizedError`. Without this,
        // `error.localizedDescription` falls back to "Module.Type error 0"
        // and our custom messages never surface.
        var errorDescription: String? { description }
    }

    struct IngestResponse: Decodable {
        let accepted: Int
        let rejected: Int
    }

    struct HealthResponse: Decodable {
        let status: String
        let server_time: String
        let schema_version: Int
        let max_batch: Int
    }

    private let config: EAConfig
    private let session: URLSession
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    init(config: EAConfig) {
        self.config = config
        let cfg = URLSessionConfiguration.ephemeral
        // `timeoutIntervalForRequest` is the idle timeout — clock resets
        // every time bytes flow on the socket. Most ingest POSTs finish in
        // under a second, but a single workout-route batch carries one
        // workout's complete GPS track as a JSON blob in `locations_json`
        // (a long hike at 1Hz can be 30K+ points / several MB). Once iOS
        // finishes uploading that body, the connection goes idle while
        // EA's PHP-FPM worker decodes the JSON and writes the row; on a
        // big route that can run well past 30s with nothing visible on
        // the wire. The original 30s value was tripping every long
        // workout (URLError -1005 / POSIX 53 ECONNABORTED) and stranding
        // the backfill at `health_workout_routes`. 180s is comfortably
        // above the worst case observed in practice.
        cfg.timeoutIntervalForRequest = 180
        // `timeoutIntervalForResource` is the hard upper bound on a
        // single request's full lifecycle (TLS handshake + upload + idle
        // + response). 10 minutes — same order as EA's gateway-queue
        // timeout — gives even pathological GPX uploads room without
        // letting a wedged server pin a worker forever.
        cfg.timeoutIntervalForResource = 600
        cfg.waitsForConnectivity = true
        self.session = URLSession(configuration: cfg)
        let enc = JSONEncoder()
        enc.outputFormatting = []
        self.encoder = enc
        self.decoder = JSONDecoder()
    }

    /// `GET /api/v1/healthbeat/health` — capability + liveness probe.
    func health() async throws -> HealthResponse {
        let req = try buildRequest(path: "/api/v1/healthbeat/health", method: "GET", bodyData: nil)
        return try await perform(req)
    }

    /// `POST /api/v1/healthbeat/<endpoint>` with `{ "records": [...] }`.
    /// `T` is one of the row types in `BackendRows.swift`.
    /// Splits batches > 500 records into multiple POSTs (matches EA's MAX_BATCH).
    @discardableResult
    func postRecords<T: Encodable>(endpoint: String, records: [T]) async throws -> Int {
        guard config.isConfigured else { throw EAError.notConfigured }

        var totalAccepted = 0
        let chunkSize = 500
        for chunk in records.chunked(into: chunkSize) {
            let body = try encoder.encode(EAIngestEnvelope(records: chunk))
            let req = try buildRequest(path: endpoint, method: "POST", bodyData: body)
            let res: IngestResponse = try await perform(req)
            totalAccepted += res.accepted
        }
        return totalAccepted
    }

    /// `POST /api/v1/healthbeat/reconcile` — delete stale rows in a (table,
    /// optional type, date-range) slice whose UUID isn't in the supplied
    /// list. Server returns `{deleted: N}`. Throws on misconfig / 4xx.
    @discardableResult
    func postReconcile(
        table: String, typeColumn: String?, typeValue: String?,
        since: String, until: String, validUUIDs: [String]
    ) async throws -> Int {
        guard config.isConfigured else { throw EAError.notConfigured }

        struct ReconcileBody: Encodable {
            let table: String
            let type_column: String?
            let type_value: String?
            let since: String
            let until: String
            let valid_uuids: [String]
        }
        struct ReconcileResponse: Decodable { let deleted: Int }

        // The reconcile endpoint accepts a single payload — chunk only if
        // the UUID list is huge enough to risk exceeding max_post_size
        // (~1MB by default). 5000 UUIDs * 36 chars ≈ 180KB, well under.
        let chunkSize = 5000
        var totalDeleted = 0
        let chunks = validUUIDs.chunked(into: chunkSize)
        // Walk every chunk; each pass narrows the surviving set further.
        // Order doesn't matter because we DELETE … NOT IN per chunk,
        // and the union of NOT-IN sets equals NOT-IN union.
        // (Important: a row absent from chunk A but present in chunk B
        // would be deleted by chunk A. So we cannot chunk this naively
        // — fall back to a single call when the list won't fit and let
        // the server fail loudly.)
        if chunks.count > 1 {
            throw EAError.http(413, "reconcile_too_many_uuids:\(validUUIDs.count) — split the time window instead")
        }
        let body = ReconcileBody(
            table: table, type_column: typeColumn, type_value: typeValue,
            since: since, until: until, valid_uuids: validUUIDs
        )
        let bodyData = try encoder.encode(body)
        let req = try buildRequest(path: "/api/v1/healthbeat/reconcile", method: "POST", bodyData: bodyData)
        let res: ReconcileResponse = try await perform(req)
        totalDeleted += res.deleted
        return totalDeleted
    }

    /// `GET /api/v1/healthbeat/geofences?since=…` — used by the bidirectional pull.
    func pullGeofences(since: String?) async throws -> [HBGeofenceDefinitionRow] {
        struct Wrapper: Decodable { let records: [HBGeofenceDefinitionRow] }
        let q = since.map { "?since=\($0)" } ?? ""
        let req = try buildRequest(path: "/api/v1/healthbeat/geofences" + q, method: "GET", bodyData: nil)
        let res: Wrapper = try await perform(req)
        return res.records
    }

    /// `GET /api/v1/healthbeat/place-categories?since=…`
    func pullCategories(since: String?) async throws -> [HBPlaceCategoryRow] {
        struct Wrapper: Decodable { let records: [HBPlaceCategoryRow] }
        let q = since.map { "?since=\($0)" } ?? ""
        let req = try buildRequest(path: "/api/v1/healthbeat/place-categories" + q, method: "GET", bodyData: nil)
        let res: Wrapper = try await perform(req)
        return res.records
    }

    // MARK: - Internals

    private func buildRequest(path: String, method: String, bodyData: Data?) throws -> URLRequest {
        guard let base = config.normalisedBaseURL,
              let url = URL(string: path, relativeTo: base) else {
            throw EAError.invalidURL
        }
        var req = URLRequest(url: url)
        req.httpMethod = method
        req.setValue("Bearer \(config.syncKey)", forHTTPHeaderField: "Authorization")
        req.setValue("application/json", forHTTPHeaderField: "Accept")
        if let bodyData {
            req.setValue("application/json", forHTTPHeaderField: "Content-Type")
            req.httpBody = bodyData
        }
        return req
    }

    private func perform<T: Decodable>(_ request: URLRequest) async throws -> T {
        // Retry only on transient 5xx; 4xx is the caller's fault — surface it.
        var lastError: EAError?
        for attempt in 0..<3 {
            do {
                let (data, response) = try await session.data(for: request)
                guard let http = response as? HTTPURLResponse else {
                    throw EAError.http(0, nil)
                }
                if http.statusCode >= 500 {
                    lastError = .http(http.statusCode, String(data: data, encoding: .utf8))
                    if attempt < 2 {
                        try? await Task.sleep(nanoseconds: UInt64(pow(2.0, Double(attempt)) * 1_000_000_000))
                        continue
                    }
                    throw lastError!
                }
                guard (200..<300).contains(http.statusCode) else {
                    throw EAError.http(http.statusCode, String(data: data, encoding: .utf8))
                }
                do {
                    return try decoder.decode(T.self, from: data)
                } catch {
                    throw EAError.decoding(error)
                }
            } catch let e as EAError {
                throw e
            } catch {
                throw EAError.transport(error)
            }
        }
        throw lastError ?? .http(0, nil)
    }
}

// Note: `Array.chunked(into:)` is already defined in SyncService.swift —
// no second declaration needed here.
