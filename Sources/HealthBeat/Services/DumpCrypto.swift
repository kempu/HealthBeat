import Foundation
import CryptoKit
import Compression
import Security

/// On-disk encryption for the Phase-1 health export dumps.
///
/// The dumps hold the user's complete health history, so they are encrypted at
/// the application layer (defence in depth on top of `NSFileProtectionComplete`):
/// a leaked or missed-by-cleanup `.enc` file is opaque without the per-run key,
/// which is delivered to EA only in a *separate* TLS request and never stored on
/// disk alongside the ciphertext.
///
/// ## File format (must byte-match the EA PHP decoder `ImportHealthBeatDump`)
///
/// ```
/// magic        : 8 bytes  ASCII "HBDUMP01"   (4-char tag + 2-digit version)
/// then repeated frames until EOF:
///   nonce        : 12 bytes        AES-GCM nonce (random per frame)
///   inflated_len :  4 bytes BE     size of the NDJSON chunk after inflate
///   ct_len       :  4 bytes BE     size of the GCM ciphertext (== deflated len)
///   ciphertext   : ct_len bytes    AES-256-GCM( deflate(ndjson_chunk) )
///   tag          : 16 bytes        AES-GCM auth tag
/// ```
///
/// Each frame's plaintext is one or more *complete* NDJSON lines, raw-DEFLATE
/// compressed (Apple `COMPRESSION_ZLIB` == zlib raw deflate == PHP
/// `gzdeflate`/`gzinflate`). Frames are flushed at ~`Self.chunkThreshold` bytes so
/// neither side ever holds the whole file in memory.
enum DumpCrypto {
    /// 8-byte file header: 6-char tag + 2-digit version.
    static let magic = Data("HBDUMP01".utf8)
    /// Flush a frame once the buffered (uncompressed) NDJSON reaches this size.
    static let chunkThreshold = 1 * 1024 * 1024  // 1 MiB

    enum CryptoError: Error, LocalizedError {
        case badMagic
        case truncated
        case compressFailed
        case decompressFailed

        var errorDescription: String? {
            switch self {
            case .badMagic:         return "Dump file header is not recognised"
            case .truncated:        return "Dump file is truncated or corrupt"
            case .compressFailed:   return "Dump compression failed"
            case .decompressFailed: return "Dump decompression failed"
            }
        }
    }

    // MARK: Raw DEFLATE (interoperable with PHP gzdeflate / gzinflate)

    static func deflate(_ input: Data) throws -> Data {
        guard !input.isEmpty else { return Data() }
        // Worst case, deflate can slightly exceed the input; give generous slack.
        var capacity = input.count + (input.count / 2) + 64
        while true {
            let out = input.withUnsafeBytes { src -> Data? in
                let dst = UnsafeMutablePointer<UInt8>.allocate(capacity: capacity)
                defer { dst.deallocate() }
                let written = compression_encode_buffer(
                    dst, capacity,
                    src.bindMemory(to: UInt8.self).baseAddress!, input.count,
                    nil, COMPRESSION_ZLIB
                )
                guard written > 0 else { return nil }
                return Data(bytes: dst, count: written)
            }
            if let out { return out }
            capacity *= 2
            if capacity > input.count * 8 + 1024 { throw CryptoError.compressFailed }
        }
    }

    static func inflate(_ input: Data, expectedSize: Int) throws -> Data {
        guard expectedSize > 0 else { return Data() }
        return try input.withUnsafeBytes { src -> Data in
            let dst = UnsafeMutablePointer<UInt8>.allocate(capacity: expectedSize)
            defer { dst.deallocate() }
            let written = compression_decode_buffer(
                dst, expectedSize,
                src.bindMemory(to: UInt8.self).baseAddress!, input.count,
                nil, COMPRESSION_ZLIB
            )
            guard written == expectedSize else { throw CryptoError.decompressFailed }
            return Data(bytes: dst, count: written)
        }
    }

    static func be32(_ v: UInt32) -> Data {
        var b = v.bigEndian
        return Data(bytes: &b, count: 4)
    }

    static func readBE32(_ d: Data) -> UInt32 {
        d.reduce(UInt32(0)) { ($0 << 8) | UInt32($1) }
    }
}

/// Streaming writer: append NDJSON lines, frames are flushed automatically.
/// Holds at most one ~1 MiB buffer + one frame in memory.
final class DumpFrameWriter {
    private let key: SymmetricKey
    private let handle: FileHandle
    private var buffer = Data()
    private var headerWritten = false

    init(fileURL: URL, key: SymmetricKey) throws {
        self.key = key
        // `.completeUntilFirstUserAuthentication` (not `.complete`): a background
        // `URLSession` upload and a background-task drain must be able to READ the
        // file while the device is locked. `.complete` makes it unreadable when
        // locked → "API MISUSE … not supported". The dump is also AES-GCM
        // encrypted by us, so this still keeps it safe at rest.
        FileManager.default.createFile(atPath: fileURL.path, contents: nil,
                                       attributes: [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication])
        self.handle = try FileHandle(forWritingTo: fileURL)
    }

    /// Append one already-encoded JSON object; a trailing newline is added.
    func append(line: Data) throws {
        if !headerWritten {
            try handle.write(contentsOf: DumpCrypto.magic)
            headerWritten = true
        }
        buffer.append(line)
        buffer.append(0x0A)  // '\n'
        if buffer.count >= DumpCrypto.chunkThreshold {
            try flushFrame()
        }
    }

    private func flushFrame() throws {
        guard !buffer.isEmpty else { return }
        let inflatedLen = UInt32(buffer.count)
        let deflated = try DumpCrypto.deflate(buffer)
        let nonce = AES.GCM.Nonce()  // random 96-bit
        let sealed = try AES.GCM.seal(deflated, using: key, nonce: nonce)
        // sealed.ciphertext.count == deflated.count
        var frame = Data()
        frame.append(Data(nonce))
        frame.append(DumpCrypto.be32(inflatedLen))
        frame.append(DumpCrypto.be32(UInt32(sealed.ciphertext.count)))
        frame.append(sealed.ciphertext)
        frame.append(sealed.tag)
        try handle.write(contentsOf: frame)
        buffer.removeAll(keepingCapacity: true)
    }

    /// Flush the final partial frame and close the file. Safe to call once.
    func close() throws {
        if !headerWritten {
            // Empty table: still emit a valid (header-only) file.
            try handle.write(contentsOf: DumpCrypto.magic)
            headerWritten = true
        }
        try flushFrame()
        try handle.close()
    }
}

/// Streaming reader: returns the inflated NDJSON bytes of each frame in order.
/// Holds at most one frame in memory.
final class DumpFrameReader {
    private let key: SymmetricKey
    private let handle: FileHandle
    /// Number of frames already returned — used as the resume cursor for the drain.
    private(set) var frameIndex = 0

    init(fileURL: URL, key: SymmetricKey) throws {
        self.key = key
        self.handle = try FileHandle(forReadingFrom: fileURL)
        let header = try readExact(DumpCrypto.magic.count)
        guard header == DumpCrypto.magic else { throw DumpCrypto.CryptoError.badMagic }
    }

    /// Returns the next frame's NDJSON bytes (one or more complete lines), or
    /// nil at clean EOF. Throws on a truncated/corrupt frame or failed GCM tag.
    func nextChunk() throws -> Data? {
        guard let nonceData = try readMaybe(12) else { return nil }  // clean EOF
        let inflatedLen = DumpCrypto.readBE32(try readExact(4))
        let ctLen = Int(DumpCrypto.readBE32(try readExact(4)))
        let ciphertext = try readExact(ctLen)
        let tag = try readExact(16)
        let sealed = try AES.GCM.SealedBox(nonce: AES.GCM.Nonce(data: nonceData),
                                           ciphertext: ciphertext, tag: tag)
        let deflated = try AES.GCM.open(sealed, using: key)
        let ndjson = try DumpCrypto.inflate(deflated, expectedSize: Int(inflatedLen))
        frameIndex += 1
        return ndjson
    }

    /// Skip `count` frames without decrypting their bodies (resume support).
    func skipFrames(_ count: Int) throws {
        for _ in 0..<count {
            guard let _ = try readMaybe(12) else { return }
            _ = try readExact(4)
            let ctLen = Int(DumpCrypto.readBE32(try readExact(4)))
            _ = try readExact(ctLen)
            _ = try readExact(16)
            frameIndex += 1
        }
    }

    func close() throws { try handle.close() }

    // MARK: byte readers

    private func readExact(_ n: Int) throws -> Data {
        guard n > 0 else { return Data() }
        let d = try handle.read(upToCount: n) ?? Data()
        guard d.count == n else { throw DumpCrypto.CryptoError.truncated }
        return d
    }

    /// Reads exactly `n` bytes, or returns nil if at clean EOF (0 bytes read).
    private func readMaybe(_ n: Int) throws -> Data? {
        let d = try handle.read(upToCount: n) ?? Data()
        if d.isEmpty { return nil }
        guard d.count == n else { throw DumpCrypto.CryptoError.truncated }
        return d
    }
}

/// Keychain storage for per-run dump keys. Hardware-protected, never leaves the
/// device except the deliberate TLS POST of the key to EA. The drain reads it
/// back to decrypt local files after an app restart.
enum DumpKeychain {
    private static let service = "ee.klemens.healthbeat.dumpkey"

    static func store(_ key: SymmetricKey, runID: String) throws {
        let data = key.withUnsafeBytes { Data($0) }
        // Delete any prior value for this run, then add.
        delete(runID: runID)
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: runID,
            // AfterFirstUnlock (not WhenUnlocked): the background drain + the EA
            // key-delivery POST must read the key while the device is locked.
            kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly,
            kSecValueData as String: data,
        ]
        let status = SecItemAdd(query as CFDictionary, nil)
        guard status == errSecSuccess else { throw keychainError(status) }
    }

    static func load(runID: String) -> SymmetricKey? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: runID,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        var out: AnyObject?
        guard SecItemCopyMatching(query as CFDictionary, &out) == errSecSuccess,
              let data = out as? Data else { return nil }
        return SymmetricKey(data: data)
    }

    @discardableResult
    static func delete(runID: String) -> Bool {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: runID,
        ]
        return SecItemDelete(query as CFDictionary) == errSecSuccess
    }

    private static func keychainError(_ status: OSStatus) -> NSError {
        let msg = SecCopyErrorMessageString(status, nil) as String? ?? "Keychain error \(status)"
        return NSError(domain: NSOSStatusErrorDomain, code: Int(status),
                       userInfo: [NSLocalizedDescriptionKey: msg])
    }
}
