import ActivityKit
import Foundation
import CryptoKit

/// Phase 2b — uploads the encrypted dump files to EA on a **background**
/// `URLSession`, so delivery survives app suspension/termination and the device
/// can be locked the whole time. Only ciphertext goes over this session; the
/// decryption key is sent afterwards in a separate foreground TLS request
/// (`EAService.postDumpKey`) once every file has landed, so file and key never
/// travel together.
///
/// Flow per run:
///   1. enqueue one `uploadTask(fromFile:)` per dump table → `POST /bulk-import/{run}/{table}`
///   2. as each finishes, decrement a persisted counter (survives relaunch)
///   3. when the counter hits zero with no failures, POST the run key, which
///      tells EA to dispatch its decrypt-and-import jobs.
final class EADumpUploader: NSObject, @unchecked Sendable {
    static let shared = EADumpUploader()

    private static let sessionID = "ee.klemens.healthbeat.dumpupload"
    /// Each table's encrypted dump is uploaded in slices of at most this many
    /// bytes, so a dropped connection (common over a flaky link / VPN to a home
    /// server) only re-sends one slice instead of the whole multi-hundred-MB file.
    /// The server reassembles the slices, in order, into the original `.enc`.
    private static let chunkBytes = 16 * 1024 * 1024   // 16 MiB
    /// Set by the app delegate's `handleEventsForBackgroundURLSession` so we can
    /// signal UIKit when all background events have been delivered.
    var backgroundCompletionHandler: (() -> Void)?

    private lazy var session: URLSession = {
        let cfg = URLSessionConfiguration.background(withIdentifier: Self.sessionID)
        cfg.sessionSendsLaunchEvents = true
        cfg.isDiscretionary = false
        cfg.allowsCellularAccess = true
        cfg.httpMaximumConnectionsPerHost = 2
        // Without these a stalled upload (e.g. a server/proxy that accepts the
        // connection for a too-large body but never responds) hangs on the
        // background session's 7-day default — the dashboard sits at "EA
        // uploading…" forever with no failure and no Retry. `…ForRequest` is an
        // IDLE timeout (resets while bytes flow, so it never kills a legitimately
        // large but progressing upload — only a truly stalled one);
        // `…ForResource` is a generous hard cap so any genuine hang eventually
        // surfaces as a failure the user can retry.
        cfg.timeoutIntervalForRequest = 120        // 2 min with no progress → fail
        cfg.timeoutIntervalForResource = 2 * 3600  // 2 h hard cap per file
        let q = OperationQueue()
        q.maxConcurrentOperationCount = 1   // serialise delegate callbacks
        return URLSession(configuration: cfg, delegate: self, delegateQueue: q)
    }()

    /// Call once at launch so the background session is recreated and reattached
    /// to its delegate before iOS replays any completion events.
    func reconnect() { _ = session }

    // MARK: Counters (persisted so they survive a relaunch mid-upload)

    private func remainingKey(_ runID: String) -> String { "eaUpload.remaining.\(runID)" }
    private func failedKey(_ runID: String) -> String { "eaUpload.failed.\(runID)" }
    private func totalKey(_ runID: String) -> String { "eaUpload.total.\(runID)" }
    private func failReasonKey(_ runID: String) -> String { "eaUpload.failReason.\(runID)" }
    /// Stashed by `runFullSync` before the EA upload begins so the uploader can
    /// drive the live activity across the same overall-progress slice the
    /// dashboard uses (base = progress when upload started, span = EA's weight).
    private func liveBaseKey(_ runID: String) -> String { "eaUpload.liveBase.\(runID)" }
    private func liveSpanKey(_ runID: String) -> String { "eaUpload.liveSpan.\(runID)" }

    /// Response bodies per upload task (for diagnosing a server-side failure).
    /// Touched only on the serial delegate queue, so no extra locking needed.
    private var responseBodies: [Int: Data] = [:]

    /// In-flight body-send fraction (0…1) per upload task, with its run, so the
    /// progress bar moves WITHIN a large file instead of only when a whole file
    /// lands. Removed when the task completes. Serial delegate queue only.
    private var taskProgress: [Int: (run: String, frac: Double)] = [:]

    // MARK: Enqueue

    /// Split every table dump into ≤`chunkBytes` slices and upload each as its own
    /// background task to `…/{table}/chunk/{i}`. The counters (`remaining`/`total`)
    /// count CHUNKS, not tables, so a single big table contributes many units of
    /// progress and a dropped slice only re-sends that slice. No-op if EA isn't
    /// configured. Throws only if the manifest is missing.
    func start(runID: String) throws {
        let cfg = EAConfig.load()
        guard cfg.isConfigured, let base = cfg.normalisedBaseURL else { return }
        guard let manifest = DumpStore.loadManifest(runID) else {
            throw NSError(domain: "EADumpUploader", code: 1,
                          userInfo: [NSLocalizedDescriptionKey: "Dump manifest for \(runID) is missing"])
        }

        let runDir = DumpStore.runDir(runID)
        let fm = FileManager.default

        // Plan slices per table up front so the run's total chunk count (the
        // progress denominator) is known before any upload starts.
        struct TablePlan { let entry: DumpManifest.TableEntry; let fileURL: URL; let chunks: Int }
        var plans: [TablePlan] = []
        for entry in manifest.tables {
            let fileURL = runDir.appendingPathComponent(entry.fileName)
            let size = ((try? fm.attributesOfItem(atPath: fileURL.path))?[.size] as? Int) ?? 0
            let chunks = max(1, Int((Double(size) / Double(Self.chunkBytes)).rounded(.up)))
            plans.append(TablePlan(entry: entry, fileURL: fileURL, chunks: chunks))
        }
        let totalChunks = plans.reduce(0) { $0 + $1.chunks }

        UserDefaults.standard.set(totalChunks, forKey: remainingKey(runID))
        UserDefaults.standard.set(totalChunks, forKey: totalKey(runID))
        UserDefaults.standard.set(false, forKey: failedKey(runID))

        // Cancel any leftover tasks for this run first (e.g. a stalled upload being
        // restarted) so we never duplicate-upload a slice. Their cancellation
        // callbacks are ignored (see `didCompleteWithError`) so they don't touch
        // the fresh counters. Slice + enqueue once the cancellation sweep returns.
        session.getAllTasks { [weak self] tasks in
            guard let self else { return }
            for t in tasks where t.taskDescription == runID { t.cancel() }
            for plan in plans {
                guard let handle = try? FileHandle(forReadingFrom: plan.fileURL) else { continue }
                defer { try? handle.close() }
                for i in 0..<plan.chunks {
                    let chunkData: Data
                    do {
                        try handle.seek(toOffset: UInt64(i) * UInt64(Self.chunkBytes))
                        chunkData = try handle.read(upToCount: Self.chunkBytes) ?? Data()
                    } catch { continue }
                    // Background `uploadTask` must read from a file, so each slice
                    // is written to its own part file (readable while the device is
                    // locked, like the run dir) and removed once it uploads.
                    let partURL = runDir.appendingPathComponent("\(plan.entry.table.eaTable).part\(i)")
                    do { try chunkData.write(to: partURL, options: [.completeFileProtectionUntilFirstUserAuthentication]) }
                    catch { continue }
                    guard let url = URL(string: "api/v1/healthbeat/bulk-import/\(runID)/\(plan.entry.table.eaTable)/chunk/\(i)",
                                        relativeTo: base) else { continue }
                    var req = URLRequest(url: url)
                    req.httpMethod = "POST"
                    req.setValue("Bearer \(cfg.syncKey)", forHTTPHeaderField: "Authorization")
                    req.setValue("application/octet-stream", forHTTPHeaderField: "Content-Type")
                    req.setValue(String(plan.chunks), forHTTPHeaderField: "X-HB-Chunk-Count")
                    req.setValue(String(plan.entry.rowCount), forHTTPHeaderField: "X-HB-Row-Count")
                    let task = self.session.uploadTask(with: req, fromFile: partURL)
                    task.taskDescription = runID
                    task.resume()
                }
            }
        }
    }

    // MARK: Completion → send key

    /// All files for the run have landed; hand EA the key so it can decrypt and
    /// import. Runs off the delegate queue.
    private func sendKeyIfReady(runID: String) {
        let remaining = UserDefaults.standard.integer(forKey: remainingKey(runID))
        guard remaining <= 0 else { return }
        if UserDefaults.standard.bool(forKey: failedKey(runID)) {
            postFailed(runID, reason: UserDefaults.standard.string(forKey: failReasonKey(runID)))
            return
        }
        guard let key = DumpKeychain.load(runID: runID) else {
            postFailed(runID, reason: "dump key missing from Keychain")
            return
        }
        let keyB64 = key.withUnsafeBytes { Data($0) }.base64EncodedString()
        Task {
            do {
                _ = try await EAService(config: EAConfig.load()).postDumpKey(runID: runID, keyBase64: keyB64)
                UserDefaults.standard.removeObject(forKey: remainingKey(runID))
                UserDefaults.standard.removeObject(forKey: failedKey(runID))
                UserDefaults.standard.removeObject(forKey: totalKey(runID))
                UserDefaults.standard.removeObject(forKey: failReasonKey(runID))
                UserDefaults.standard.removeObject(forKey: liveBaseKey(runID))
                UserDefaults.standard.removeObject(forKey: liveSpanKey(runID))
                // Files + key are with EA; its server-side import jobs run next.
                // Record the run so the app polls `bulkImportStatus` until the
                // import actually finishes — even if it was suspended when this
                // fired and missed the notification below.
                UserDefaults.standard.set(runID, forKey: "pendingEAImportRunID")
                // EA has the files + key. If the MySQL drain is also done (or
                // wasn't required), the local dump can be wiped. Note the live
                // activity is left running until the import poll completes it.
                _ = DumpStore.markEAKeyDelivered(runID)
                NotificationCenter.default.post(name: .eaDumpUploadKeyDelivered, object: runID)
            } catch {
                self.postFailed(runID, reason: "key delivery — \(error.localizedDescription)")
            }
        }
    }

    private func postFailed(_ runID: String, reason: String?) {
        NotificationCenter.default.post(name: .eaDumpUploadFailed, object: runID,
                                        userInfo: reason.map { ["reason": $0] })
    }

    /// Computes the run's byte-aware upload fraction (fully-uploaded files +
    /// partial progress of the in-flight ones) and broadcasts it to the dashboard
    /// bar and the live activity. Called on every body-send tick and completion,
    /// so a single large file shows continuous movement instead of looking frozen.
    /// The live-activity update works while backgrounded/locked — the foreground
    /// SyncService can't touch it during the upload phase, so the uploader must.
    private func postProgress(runID: String) {
        let total = max(1, UserDefaults.standard.integer(forKey: totalKey(runID)))
        let remaining = max(0, UserDefaults.standard.integer(forKey: remainingKey(runID)))
        let completed = total - remaining   // fully-landed files
        let inflight = taskProgress.values.filter { $0.run == runID }.map(\.frac).reduce(0, +)
        let frac = min(1.0, (Double(completed) + inflight) / Double(total))
        NotificationCenter.default.post(name: .eaDumpUploadProgress, object: runID,
                                        userInfo: ["fraction": frac])
        updateUploadLiveActivity(runID: runID, fraction: frac, completed: completed, total: total)
    }

    private func updateUploadLiveActivity(runID: String, fraction frac: Double, completed: Int, total: Int) {
        guard let activity = Activity<SyncActivityAttributes>.activities.first else { return }
        let base = UserDefaults.standard.double(forKey: liveBaseKey(runID))
        let span = UserDefaults.standard.double(forKey: liveSpanKey(runID))
        let uploaded = min(total, completed + 1)   // show the file currently uploading
        let prev = activity.content.state
        // The MySQL drain may be writing this same activity concurrently using the
        // full overall progress (export + EA + MySQL slices). Our value only counts
        // export + EA, so clamp to never regress the bar — full-sync progress is
        // monotonic, so whichever writer is further ahead wins.
        let progress = max(prev.progress, min(1.0, base + span * frac))
        let next = SyncActivityAttributes.ContentState(
            phase: "Upload", operation: "Uploading to EA (\(uploaded)/\(total))",
            recordsInserted: prev.recordsInserted, isFullSync: prev.isFullSync, progress: progress)
        Task { await activity.update(ActivityContent(state: next, staleDate: nil)) }
    }
}

extension EADumpUploader: URLSessionDataDelegate {
    /// Parses `(table, chunkIndex)` from a chunk-upload task's URL
    /// (`…/bulk-import/{run}/{table}/chunk/{index}`). nil for non-chunk URLs.
    private static func chunkTarget(_ task: URLSessionTask) -> (table: String, index: Int)? {
        let comps = task.originalRequest?.url?.pathComponents ?? []
        guard comps.count >= 3, comps[comps.count - 2] == "chunk",
              let idx = Int(comps[comps.count - 1]) else { return nil }
        return (comps[comps.count - 3], idx)
    }

    func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
        responseBodies[dataTask.taskIdentifier, default: Data()].append(data)
    }

    /// Body-send ticks → move the bar WITHIN a file so a large upload doesn't look
    /// frozen (the file-count fraction alone only jumps when a whole file lands).
    func urlSession(_ session: URLSession, task: URLSessionTask, didSendBodyData bytesSent: Int64,
                    totalBytesSent: Int64, totalBytesExpectedToSend: Int64) {
        guard let runID = task.taskDescription, totalBytesExpectedToSend > 0 else { return }
        taskProgress[task.taskIdentifier] = (runID, min(1.0, Double(totalBytesSent) / Double(totalBytesExpectedToSend)))
        postProgress(runID: runID)
    }

    func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
        guard let runID = task.taskDescription else { return }
        // A task we cancelled on restart (see `start`) — leave the fresh counters
        // alone; the replacement task drives progress.
        if (error as NSError?)?.code == NSURLErrorCancelled {
            responseBodies.removeValue(forKey: task.taskIdentifier)
            taskProgress.removeValue(forKey: task.taskIdentifier)
            return
        }
        let http = task.response as? HTTPURLResponse
        let body = responseBodies.removeValue(forKey: task.taskIdentifier)
        let ok = error == nil && (http.map { (200..<300).contains($0.statusCode) } ?? false)
        if ok {
            // Slice landed — delete its part file (reconstruct the path from the
            // chunk URL: …/{table}/chunk/{i}) to reclaim disk as we go.
            if let (table, idx) = Self.chunkTarget(task) {
                try? FileManager.default.removeItem(
                    at: DumpStore.runDir(runID).appendingPathComponent("\(table).part\(idx)"))
            }
        } else {
            // Capture WHY (status + table/chunk + server snippet) so it's diagnosable:
            // 404 = endpoint not deployed, 413 = chunk too large, 401 = bad key.
            let label = Self.chunkTarget(task).map { "\($0.table) chunk \($0.index)" }
                ?? (task.originalRequest?.url?.lastPathComponent ?? "?")
            let reason: String
            if let error {
                reason = "\(label): \(error.localizedDescription)"
            } else if let http {
                let snippet = body
                    .flatMap { String(data: $0.prefix(180), encoding: .utf8) }?
                    .trimmingCharacters(in: .whitespacesAndNewlines)
                reason = "\(label): HTTP \(http.statusCode)" + (snippet.map { " — \($0)" } ?? "")
            } else {
                reason = "\(label): unknown error"
            }
            print("[EADumpUploader] upload failed — \(reason)")
            UserDefaults.standard.set(true, forKey: failedKey(runID))
            UserDefaults.standard.set(reason, forKey: failReasonKey(runID))
        }
        let remaining = UserDefaults.standard.integer(forKey: remainingKey(runID)) - 1
        UserDefaults.standard.set(remaining, forKey: remainingKey(runID))
        // This file is fully accounted for in `completed` now — drop its in-flight
        // entry so it isn't double-counted, then broadcast the new progress.
        taskProgress.removeValue(forKey: task.taskIdentifier)
        postProgress(runID: runID)

        if remaining <= 0 { sendKeyIfReady(runID: runID) }
    }

    func urlSessionDidFinishEvents(forBackgroundURLSession session: URLSession) {
        // UIKit handed us a completion handler when it relaunched us for these
        // events; calling it lets the app snapshot and suspend cleanly.
        DispatchQueue.main.async {
            self.backgroundCompletionHandler?()
            self.backgroundCompletionHandler = nil
        }
    }
}

extension Notification.Name {
    static let eaDumpUploadProgress = Notification.Name("eaDumpUploadProgress")
    static let eaDumpUploadKeyDelivered = Notification.Name("eaDumpUploadKeyDelivered")
    static let eaDumpUploadFailed = Notification.Name("eaDumpUploadFailed")
}
