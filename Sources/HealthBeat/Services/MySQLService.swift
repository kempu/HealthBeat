import Foundation
import Network
import Security
import CommonCrypto

// Thread-safe one-shot flag to prevent double-resuming a CheckedContinuation
private final class OnceFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var fired = false
    func run(_ body: () -> Void) {
        lock.lock()
        defer { lock.unlock() }
        guard !fired else { return }
        fired = true
        body()
    }
}

// MARK: - Error types

enum MySQLError: Error, LocalizedError {
    case connectionFailed(String)
    case authFailed(String)
    case queryError(code: Int, message: String)
    case protocolError(String)
    case disconnected
    case timeout

    var errorDescription: String? {
        switch self {
        case .connectionFailed(let m): return "Connection failed: \(m)"
        case .authFailed(let m):       return "Authentication failed: \(m)"
        case .queryError(_, let m):    return "MySQL error: \(m)"
        case .protocolError(let m):    return "Protocol error: \(m)"
        case .disconnected:            return "Connection closed"
        case .timeout:                 return "Connection timed out"
        }
    }
}

// MARK: - MySQL capability flags

private struct MySQLCaps: OptionSet {
    let rawValue: UInt32
    static let longPassword          = MySQLCaps(rawValue: 1 << 0)
    static let longFlag              = MySQLCaps(rawValue: 1 << 2)
    static let connectWithDB         = MySQLCaps(rawValue: 1 << 3)
    static let protocol41            = MySQLCaps(rawValue: 1 << 9)
    static let ssl                   = MySQLCaps(rawValue: 1 << 11)
    static let transactions          = MySQLCaps(rawValue: 1 << 13)
    static let secureConnection      = MySQLCaps(rawValue: 1 << 15)
    static let multiStatements       = MySQLCaps(rawValue: 1 << 16)
    static let multiResults          = MySQLCaps(rawValue: 1 << 17)
    static let pluginAuth            = MySQLCaps(rawValue: 1 << 19)
    static let pluginAuthLenencData  = MySQLCaps(rawValue: 1 << 21)
    static let sessionTrack          = MySQLCaps(rawValue: 1 << 23)
    static let deprecateEOF          = MySQLCaps(rawValue: 1 << 24)
}

// MARK: - Parsed handshake

private struct MySQLHandshake {
    let serverVersion: String
    let connectionID: UInt32
    let authPluginData: Data   // 20 bytes challenge
    let serverCaps: MySQLCaps
    let authPluginName: String
}

// MARK: - MySQLService actor

actor MySQLService {

    // MARK: State

    private var connection: NWConnection?
    private var receiveBuffer = Data()
    private let dispatchQueue = DispatchQueue(label: "ee.klemens.healthbeat.mysql", qos: .utility)
    private var receiveWaiters: [(needed: Int, cont: CheckedContinuation<Void, Error>)] = []
    private var isReceiving = false
    private var connectionError: Error?

    // MARK: - Public API

    func connect(config: MySQLConfig) async throws {
        disconnect()
        connectionError = nil

        let host = NWEndpoint.Host(config.host)
        guard let nwPort = NWEndpoint.Port(rawValue: config.port) else {
            throw MySQLError.connectionFailed("Invalid port: \(config.port)")
        }
        let params = NWParameters.tcp
        params.prohibitedInterfaceTypes = []

        let conn = NWConnection(host: host, port: nwPort, using: params)
        self.connection = conn

        // Wait for connection to be ready.
        // NWConnection can fire .failed then .cancelled in sequence on refusal,
        // so guard against resuming the continuation more than once.
        // Both stateUpdateHandler assignment and conn.start() are dispatched to dispatchQueue
        // so all NWConnection API calls happen on its own queue, avoiding unsafeForcedSync warnings.
        let q = dispatchQueue
        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
            let once = OnceFlag()
            q.async {
                conn.stateUpdateHandler = { [weak self] state in
                    switch state {
                    case .ready:
                        once.run { cont.resume() }
                    case .failed(let err):
                        let mysqlErr = MySQLError.connectionFailed(err.localizedDescription)
                        once.run { cont.resume(throwing: mysqlErr) }
                        // After initial handshake, propagate failure to any pending receive waiters
                        // so in-flight queries fail immediately instead of hanging forever.
                        Task { await self?.handleConnectionFailure(mysqlErr) }
                    case .cancelled:
                        let mysqlErr = MySQLError.connectionFailed("Cancelled")
                        once.run { cont.resume(throwing: mysqlErr) }
                        Task { await self?.handleConnectionFailure(mysqlErr) }
                    default:
                        break
                    }
                }
                conn.start(queue: q)
            }
        }

        startReceiving()

        // Perform MySQL handshake + auth
        let handshake = try await readHandshake()
        try await sendAuth(config: config, handshake: handshake)
        try await readAuthResult(config: config, handshake: handshake)
    }

    func disconnect() {
        connection?.cancel()
        connection = nil
        receiveBuffer = Data()
        isReceiving = false
        // Fail all pending waiters
        for w in receiveWaiters {
            w.cont.resume(throwing: MySQLError.disconnected)
        }
        receiveWaiters = []
    }

    var isConnected: Bool {
        guard let conn = connection else { return false }
        if case .ready = conn.state { return true }
        return false
    }

    // Execute a query that returns rows. Returns array of [columnName: value] dicts.
    func query(_ sql: String) async throws -> [[String: String]] {
        try await sendQuery(sql)
        return try await readResultSet()
    }

    // Execute a statement with no result set (INSERT, UPDATE, CREATE, etc.)
    // Returns affected rows count.
    @discardableResult
    func execute(_ sql: String) async throws -> UInt64 {
        try await sendQuery(sql)
        return try await readOKOrError()
    }

    // MARK: - Connection failure handling

    /// Called when NWConnection transitions to .failed or .cancelled after the initial
    /// handshake. Propagates the error to any pending receive waiters so in-flight queries
    /// fail immediately instead of hanging until iOS kills the app.
    private func handleConnectionFailure(_ error: Error) {
        guard connectionError == nil else { return }
        connectionError = error
        for w in receiveWaiters { w.cont.resume(throwing: error) }
        receiveWaiters = []
    }

    // MARK: - Packet I/O

    private func startReceiving() {
        guard !isReceiving, let conn = connection else { return }
        isReceiving = true
        // Dispatch conn.receive() to the NWConnection's own DispatchQueue, not the actor's
        // executor. NWConnection uses DispatchQueue.sync internally; calling it from Swift's
        // cooperative thread pool triggers "unsafeForcedSync" warnings and stalls worker threads,
        // which causes severe slowdowns in async operations throughout the sync.
        let q = dispatchQueue
        q.async {
            conn.receive(minimumIncompleteLength: 1, maximumLength: 65536) { [weak self] data, _, isComplete, error in
                Task { [weak self] in
                    await self?.handleReceived(data: data, isComplete: isComplete, error: error)
                }
            }
        }
    }

    private func handleReceived(data: Data?, isComplete: Bool, error: NWError?) {
        isReceiving = false

        if let err = error {
            connectionError = MySQLError.connectionFailed(err.localizedDescription)
            for w in receiveWaiters { w.cont.resume(throwing: connectionError!) }
            receiveWaiters = []
            return
        }

        if let d = data, !d.isEmpty {
            receiveBuffer.append(d)
            satisfyWaiters()
        }

        if isComplete {
            // The server closed the connection. Fail any waiters that are still
            // waiting for bytes — otherwise they hang forever and the sync stalls
            // until iOS kills the app.
            if !receiveWaiters.isEmpty {
                connectionError = MySQLError.disconnected
                for w in receiveWaiters { w.cont.resume(throwing: MySQLError.disconnected) }
                receiveWaiters = []
            }
        } else {
            startReceiving()
        }
    }

    private func satisfyWaiters() {
        var remaining: [(needed: Int, cont: CheckedContinuation<Void, Error>)] = []
        for w in receiveWaiters {
            if receiveBuffer.count >= w.needed {
                w.cont.resume()
            } else {
                remaining.append(w)
            }
        }
        receiveWaiters = remaining
    }

    private func waitForBytes(_ n: Int) async throws {
        if receiveBuffer.count >= n { return }
        if let err = connectionError { throw err }
        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
            receiveWaiters.append((needed: n, cont: cont))
            startReceiving()
        }
    }

    private func consume(_ n: Int) -> Data {
        let chunk = Data(receiveBuffer.prefix(n))
        receiveBuffer.removeFirst(n)
        return chunk
    }

    // Read one MySQL packet: returns (sequenceNumber, payload)
    private func readPacket() async throws -> (seq: UInt8, payload: Data) {
        try await waitForBytes(4)
        let header = consume(4)
        let length = Int(header[0]) | Int(header[1]) << 8 | Int(header[2]) << 16
        let seq    = header[3]
        try await waitForBytes(length)
        let payload = consume(length)
        return (seq, payload)
    }

    private func sendPacket(seq: UInt8, payload: Data) async throws {
        guard let conn = connection else { throw MySQLError.disconnected }
        var pkt = Data(capacity: 4 + payload.count)
        let len = payload.count
        pkt.append(UInt8(len & 0xFF))
        pkt.append(UInt8((len >> 8) & 0xFF))
        pkt.append(UInt8((len >> 16) & 0xFF))
        pkt.append(seq)
        pkt.append(contentsOf: payload)
        // Same fix as startReceiving: dispatch conn.send() to the NWConnection's dispatch queue
        // to avoid calling NWConnection from Swift's cooperative thread pool.
        let q = dispatchQueue
        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
            q.async {
                conn.send(content: pkt, completion: .contentProcessed { err in
                    if let err = err { cont.resume(throwing: err) }
                    else { cont.resume() }
                })
            }
        }
    }

    // MARK: - Handshake

    private func readHandshake() async throws -> MySQLHandshake {
        let (_, payload) = try await readPacket()
        return try parseHandshake(payload)
    }

    private func parseHandshake(_ payload: Data) throws -> MySQLHandshake {
        var i = 0

        guard payload.count > 0, payload[i] == 10 else {
            throw MySQLError.protocolError("Expected protocol v10, got \(payload.first.map(String.init) ?? "empty")")
        }
        i += 1

        // Server version (null-terminated)
        let vEnd = payload[i...].firstIndex(of: 0) ?? payload.endIndex
        let serverVersion = String(data: payload[i..<vEnd], encoding: .utf8) ?? ""
        i = payload.index(after: vEnd)

        // Connection ID (4 LE bytes)
        let connectionID = UInt32(le4: payload, at: i); i += 4

        // auth-plugin-data part 1 (8 bytes)
        var challenge = Data(payload[i..<i+8]); i += 8
        i += 1 // filler 0x00

        // Capability flags (lower 2 bytes)
        let capLow = UInt32(payload[i]) | UInt32(payload[i+1]) << 8; i += 2
        let _charset = payload[i]; i += 1
        _ = _charset
        i += 2 // status flags

        // Capability flags (upper 2 bytes)
        let capHigh = UInt32(payload[i]) | UInt32(payload[i+1]) << 8; i += 2
        let serverCaps = MySQLCaps(rawValue: capLow | (capHigh << 16))

        // auth-plugin-data-len
        let authDataLen = Int(payload[i]); i += 1
        i += 10 // reserved

        // auth-plugin-data part 2
        let part2Len = max(13, authDataLen - 8)
        if i + part2Len <= payload.count {
            challenge.append(contentsOf: payload[i..<i+part2Len-1]) // drop trailing null
        }
        i += part2Len

        // auth-plugin-name
        let pluginEnd = i < payload.count ? (payload[i...].firstIndex(of: 0) ?? payload.endIndex) : payload.endIndex
        let pluginName = i < payload.count ? (String(data: payload[i..<pluginEnd], encoding: .utf8) ?? "mysql_native_password") : "mysql_native_password"

        return MySQLHandshake(
            serverVersion: serverVersion,
            connectionID: connectionID,
            authPluginData: challenge,
            serverCaps: serverCaps,
            authPluginName: pluginName
        )
    }

    // MARK: - Authentication

    private func sendAuth(config: MySQLConfig, handshake: MySQLHandshake) async throws {
        var clientCaps = MySQLCaps([
            .longPassword, .longFlag, .protocol41,
            .secureConnection, .pluginAuth, .transactions,
        ])
        if !config.database.isEmpty { clientCaps.insert(.connectWithDB) }

        var payload = Data()

        // Client capabilities (4 LE bytes)
        payload.appendUInt32LE(clientCaps.rawValue)
        // Max packet size (16MB)
        payload.appendUInt32LE(16_777_215)
        // Character set: utf8mb4 = 45
        payload.append(45)
        // Reserved 23 bytes
        payload.append(contentsOf: repeatElement(0, count: 23))

        // Username
        payload.appendNullTerminated(config.username)

        // Auth response
        let authData = computeAuth(
            plugin: handshake.authPluginName,
            password: config.password,
            challenge: handshake.authPluginData
        )
        payload.append(UInt8(authData.count))
        payload.append(contentsOf: authData)

        // Database
        if !config.database.isEmpty {
            payload.appendNullTerminated(config.database)
        }

        // Plugin name
        payload.appendNullTerminated(handshake.authPluginName)

        try await sendPacket(seq: 1, payload: payload)
    }

    private func readAuthResult(config: MySQLConfig, handshake: MySQLHandshake) async throws {
        let (seq, payload) = try await readPacket()
        guard !payload.isEmpty else { throw MySQLError.protocolError("Empty auth response") }

        switch payload[0] {
        case 0x00:
            return // OK — authenticated
        case 0xFF:
            throw MySQLError.authFailed(parseErrorMessage(payload))
        case 0xFE:
            // Auth switch request
            try await handleAuthSwitch(payload: payload, seq: seq, config: config)
        case 0x01:
            // caching_sha2_password result
            if payload.count >= 2 {
                if payload[1] == 0x03 {
                    // Fast auth success — read final OK
                    let (_, okPayload) = try await readPacket()
                    if okPayload[0] == 0xFF { throw MySQLError.authFailed(parseErrorMessage(okPayload)) }
                } else if payload[1] == 0x04 {
                    // Full auth needed
                    try await handleCSHA2FullAuth(config: config, challenge: handshake.authPluginData, seq: seq)
                }
            }
        default:
            throw MySQLError.protocolError("Unexpected auth response byte: 0x\(String(payload[0], radix: 16))")
        }
    }

    private func handleAuthSwitch(payload: Data, seq: UInt8, config: MySQLConfig) async throws {
        // payload: 0xFE + plugin_name (null) + plugin_data (null)
        var i = 1
        let nameEnd = payload[i...].firstIndex(of: 0) ?? payload.endIndex
        let pluginName = String(data: payload[i..<nameEnd], encoding: .utf8) ?? "mysql_native_password"
        i = payload.index(after: nameEnd)

        let dataEnd = payload[i...].firstIndex(of: 0) ?? payload.endIndex
        let challenge = Data(payload[i..<dataEnd])

        let authData = computeAuth(plugin: pluginName, password: config.password, challenge: challenge)
        try await sendPacket(seq: seq + 1, payload: authData)

        let (_, resp) = try await readPacket()
        if resp[0] == 0xFF { throw MySQLError.authFailed(parseErrorMessage(resp)) }
        // 0x00 = OK, 0x01 = more data (we ignore)
    }

    private func handleCSHA2FullAuth(config: MySQLConfig, challenge: Data, seq: UInt8) async throws {
        // Request public key: send 0x02
        try await sendPacket(seq: seq + 1, payload: Data([0x02]))
        let (_, keyPacket) = try await readPacket()

        // keyPacket starts with 0x01, rest is PEM
        guard keyPacket.count > 1 else { throw MySQLError.authFailed("Empty public key") }
        let pemData = Data(keyPacket[1...])
        guard let pem = String(data: pemData, encoding: .utf8) else {
            throw MySQLError.authFailed("Invalid public key encoding")
        }

        // Encrypt: XOR(password+\0, nonce) then RSA-OAEP-SHA1
        var pwd = config.password.data(using: .utf8)!
        pwd.append(0)
        let xored = Data(pwd.enumerated().map { idx, byte in
            byte ^ challenge[idx % challenge.count]
        })

        let encrypted = try rsaEncryptOAEP(plaintext: xored, pem: pem)
        try await sendPacket(seq: seq + 3, payload: encrypted)

        let (_, resp) = try await readPacket()
        if resp[0] == 0xFF { throw MySQLError.authFailed(parseErrorMessage(resp)) }
    }

    // MARK: - Auth algorithms

    private func computeAuth(plugin: String, password: String, challenge: Data) -> Data {
        if password.isEmpty { return Data() }
        switch plugin {
        case "mysql_native_password":
            return mysqlNativePassword(password: password, challenge: challenge)
        case "caching_sha2_password":
            return cachingSHA2Password(password: password, challenge: challenge)
        default:
            return mysqlNativePassword(password: password, challenge: challenge)
        }
    }

    private func mysqlNativePassword(password: String, challenge: Data) -> Data {
        let pwd  = password.data(using: .utf8)!
        let s1   = sha1(pwd)
        let s2   = sha1(s1)
        let s3   = sha1(challenge + s2)
        return Data(zip(s1, s3).map { $0 ^ $1 })
    }

    private func cachingSHA2Password(password: String, challenge: Data) -> Data {
        // SHA256(password) XOR SHA256(SHA256(SHA256(password)) + nonce)
        let pwd  = password.data(using: .utf8)!
        let s1   = sha256(pwd)
        let s2   = sha256(s1)
        let s3   = sha256(s2 + challenge)
        return Data(zip(s1, s3).map { $0 ^ $1 })
    }

    // MARK: - Query

    private func sendQuery(_ sql: String) async throws {
        guard let conn = connection, conn.state == .ready else { throw MySQLError.disconnected }
        var payload = Data([0x03]) // COM_QUERY
        payload.append(contentsOf: sql.utf8)
        try await sendPacket(seq: 0, payload: payload)
    }

    private func readOKOrError() async throws -> UInt64 {
        let (_, payload) = try await readPacket()
        guard !payload.isEmpty else { throw MySQLError.protocolError("Empty response") }
        if payload[0] == 0xFF {
            let (code, message) = parseErrorPacket(payload)
            throw MySQLError.queryError(code: code, message: message)
        }
        if payload[0] == 0x00 {
            // OK packet — parse affected rows (length-encoded int at offset 1)
            var i = 1
            let (affected, _) = decodeLenencInt(payload, at: &i)
            return affected
        }
        return 0
    }

    private func readResultSet() async throws -> [[String: String]] {
        let (_, firstPacket) = try await readPacket()
        guard !firstPacket.isEmpty else { throw MySQLError.protocolError("Empty result") }

        if firstPacket[0] == 0xFF {
            let (code, message) = parseErrorPacket(firstPacket)
            throw MySQLError.queryError(code: code, message: message)
        }
        if firstPacket[0] == 0x00 { return [] } // OK with no rows

        // Column count
        var i = 0
        let (colCount, _) = decodeLenencInt(firstPacket, at: &i)
        let numCols = Int(colCount)

        // Read column definitions
        var columns: [String] = []
        for _ in 0..<numCols {
            let (_, colDef) = try await readPacket()
            let name = parseColumnName(colDef)
            columns.append(name)
        }

        // Read EOF or OK (column list terminator) — only if server doesn't use DEPRECATE_EOF
        let (_, afterCols) = try await readPacket()
        // afterCols[0] == 0xFE is EOF, 0x00 is OK (deprecate_eof). Either way, move on.
        if afterCols[0] == 0xFF {
            let (code, message) = parseErrorPacket(afterCols)
            throw MySQLError.queryError(code: code, message: message)
        }

        // Read rows
        var rows: [[String: String]] = []
        while true {
            let (_, rowPacket) = try await readPacket()
            if rowPacket.isEmpty { break }
            // EOF or OK signals end of rows
            if rowPacket[0] == 0xFE || (rowPacket[0] == 0x00 && rowPacket.count < 9) { break }
            if rowPacket[0] == 0xFF {
                let (code, message) = parseErrorPacket(rowPacket)
                throw MySQLError.queryError(code: code, message: message)
            }

            var row: [String: String] = [:]
            var offset = 0
            for colName in columns {
                if offset >= rowPacket.count { break }
                if rowPacket[offset] == 0xFB {
                    // NULL
                    offset += 1
                    row[colName] = nil
                } else {
                    var idx = offset
                    let (len, _) = decodeLenencInt(rowPacket, at: &idx)
                    let valueData = rowPacket[idx..<min(idx + Int(len), rowPacket.count)]
                    row[colName] = String(data: valueData, encoding: .utf8) ?? ""
                    offset = idx + Int(len)
                }
            }
            rows.append(row)
        }
        return rows
    }

    // MARK: - Packet helpers

    private func parseHandshakeError(_ payload: Data) -> String {
        guard payload.count > 3 else { return "Unknown error" }
        let msgData = payload[3...]
        return String(data: msgData, encoding: .utf8) ?? "Unknown error"
    }

    private func parseErrorPacket(_ payload: Data) -> (code: Int, message: String) {
        // ERR packet: 0xFF + 2-byte error code (LE) + (optional '#' + 5-char sqlstate) + message
        guard payload.count > 3 else { return (0, "Unknown MySQL error") }
        let code = Int(payload[1]) | (Int(payload[2]) << 8)
        var offset = 3
        if payload[3] == UInt8(ascii: "#") {
            offset = 9 // skip '#' + 5-char sqlstate
        }
        let message = String(data: payload[offset...], encoding: .utf8) ?? "Unknown MySQL error"
        return (code, message)
    }

    private func parseErrorMessage(_ payload: Data) -> String {
        parseErrorPacket(payload).message
    }

    private func parseColumnName(_ def: Data) -> String {
        // Column definition packet fields (text protocol):
        // catalog(0), schema(1), table(2), org_table(3), name(4=alias), org_name(5)
        // Each is a length-encoded string. We want field index 4 (name/alias).
        var i = 0
        for field in 0..<4 {
            guard i < def.count else { return "col\(field)" }
            if def[i] == 0xFB { i += 1; continue }
            var idx = i
            let (len, _) = decodeLenencInt(def, at: &idx)
            i = idx + Int(len)
        }
        // Now i points to the "name" field (column alias)
        guard i < def.count else { return "?" }
        var idx = i
        let (len, _) = decodeLenencInt(def, at: &idx)
        let nameData = def[idx..<min(idx + Int(len), def.count)]
        return String(data: nameData, encoding: .utf8) ?? "?"
    }

    private func decodeLenencInt(_ data: Data, at i: inout Int) -> (UInt64, Int) {
        guard i < data.count else { return (0, 0) }
        let first = data[i]
        if first < 251 {
            i += 1
            return (UInt64(first), 1)
        } else if first == 252, i + 2 < data.count {
            let v = UInt64(data[i+1]) | UInt64(data[i+2]) << 8
            i += 3
            return (v, 3)
        } else if first == 253, i + 3 < data.count {
            let v = UInt64(data[i+1]) | UInt64(data[i+2]) << 8 | UInt64(data[i+3]) << 16
            i += 4
            return (v, 4)
        } else if first == 254, i + 8 < data.count {
            var v: UInt64 = 0
            for j in 0..<8 { v |= UInt64(data[i+1+j]) << (j * 8) }
            i += 9
            return (v, 9)
        }
        i += 1
        return (0, 1)
    }

    // MARK: - Crypto helpers

    private func sha1(_ data: Data) -> Data {
        var digest = [UInt8](repeating: 0, count: Int(CC_SHA1_DIGEST_LENGTH))
        data.withUnsafeBytes { ptr in
            _ = CC_SHA1(ptr.baseAddress, CC_LONG(data.count), &digest)
        }
        return Data(digest)
    }

    private func sha256(_ data: Data) -> Data {
        var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
        data.withUnsafeBytes { ptr in
            _ = CC_SHA256(ptr.baseAddress, CC_LONG(data.count), &digest)
        }
        return Data(digest)
    }

    private func rsaEncryptOAEP(plaintext: Data, pem: String) throws -> Data {
        // Strip PEM headers and decode base64
        let b64 = pem
            .components(separatedBy: "\n")
            .filter { !$0.hasPrefix("-----") && !$0.isEmpty }
            .joined()
        guard let derData = Data(base64Encoded: b64) else {
            throw MySQLError.authFailed("Cannot decode RSA public key")
        }

        let attributes: [String: Any] = [
            kSecAttrKeyType as String: kSecAttrKeyTypeRSA,
            kSecAttrKeyClass as String: kSecAttrKeyClassPublic,
        ]
        var error: Unmanaged<CFError>?
        guard let secKey = SecKeyCreateWithData(derData as CFData, attributes as CFDictionary, &error) else {
            throw MySQLError.authFailed("Cannot parse RSA key: \(error?.takeRetainedValue().localizedDescription ?? "unknown")")
        }

        guard let encrypted = SecKeyCreateEncryptedData(secKey, .rsaEncryptionOAEPSHA1, plaintext as CFData, &error) else {
            throw MySQLError.authFailed("RSA encryption failed: \(error?.takeRetainedValue().localizedDescription ?? "unknown")")
        }
        return encrypted as Data
    }
}

// MARK: - SQL string escaping (standalone utility)

enum MySQLEscape {
    static func escapeString(_ value: String) -> String {
        value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "'",  with: "\\'")
            .replacingOccurrences(of: "\"", with: "\\\"")
            .replacingOccurrences(of: "\0", with: "\\0")
            .replacingOccurrences(of: "\n", with: "\\n")
            .replacingOccurrences(of: "\r", with: "\\r")
    }

    static func quote(_ value: String?) -> String {
        guard let v = value else { return "NULL" }
        return "'\(escapeString(v))'"
    }

    static func quoteDouble(_ value: Double?) -> String {
        guard let v = value else { return "NULL" }
        if v.isNaN || v.isInfinite { return "NULL" }
        return String(v)
    }
}

// MARK: - Data extensions

private extension Data {
    mutating func appendUInt32LE(_ value: UInt32) {
        append(UInt8(value & 0xFF))
        append(UInt8((value >> 8) & 0xFF))
        append(UInt8((value >> 16) & 0xFF))
        append(UInt8((value >> 24) & 0xFF))
    }

    mutating func appendNullTerminated(_ string: String) {
        if let d = string.data(using: .utf8) { append(d) }
        append(0)
    }
}

private extension UInt32 {
    init(le4 data: Data, at offset: Int) {
        self = UInt32(data[offset])
            | UInt32(data[offset+1]) << 8
            | UInt32(data[offset+2]) << 16
            | UInt32(data[offset+3]) << 24
    }
}
