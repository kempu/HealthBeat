import SwiftUI

struct AudiogramDataView: View {
    @ObservedObject var vm: DataBrowserViewModel
    let config: MySQLConfig

    @State private var showFilters = false

    var body: some View {
        List {
            if showFilters {
                Section("Filters") {
                    DateFilterRow(label: "From", date: $vm.filterDateFrom)
                    DateFilterRow(label: "To", date: $vm.filterDateTo)

                    if !vm.availableSources.isEmpty {
                        Picker("Source", selection: Binding(
                            get: { vm.filterSource ?? "" },
                            set: { vm.filterSource = $0.isEmpty ? nil : $0 }
                        )) {
                            Text("All Sources").tag("")
                            ForEach(vm.availableSources, id: \.self) { source in
                                Text(source).tag(source)
                            }
                        }
                    }

                    HStack {
                        Button("Apply") { vm.loadData(config: config) }
                            .buttonStyle(.borderedProminent).controlSize(.small)
                        Button("Reset") { vm.resetFilters(); vm.loadData(config: config) }
                            .buttonStyle(.bordered).controlSize(.small)
                    }
                }
            }

            Section {
                if vm.isLoading && vm.audiogramRecords.isEmpty {
                    HStack { ProgressView(); Text("Loading…").foregroundStyle(.secondary) }
                } else if let err = vm.errorMessage {
                    Label(err, systemImage: "exclamationmark.triangle")
                        .foregroundStyle(.red).font(.caption)
                } else if vm.audiogramRecords.isEmpty {
                    Text("No audiograms found").foregroundStyle(.secondary)
                } else {
                    ForEach(vm.audiogramRecords) { record in
                        AudiogramRow(record: record)
                    }
                    if vm.isLoading {
                        HStack { Spacer(); ProgressView(); Text("Loading…").foregroundStyle(.secondary); Spacer() }
                    } else {
                        Button {
                            vm.loadNextPage(config: config)
                        } label: {
                            HStack { Spacer(); Label("Load More", systemImage: "arrow.down.circle"); Spacer() }
                        }
                        .foregroundStyle(.blue)
                    }
                }
            } header: {
                HStack {
                    Text("Audiograms")
                    Spacer()
                    if !vm.audiogramRecords.isEmpty {
                        Text("\(vm.totalLoaded) loaded").font(.caption).foregroundStyle(.secondary)
                    }
                }
            }
        }
        .navigationTitle("Audiograms")
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button {
                    withAnimation { showFilters.toggle() }
                } label: {
                    Image(systemName: showFilters ? "line.3.horizontal.decrease.circle.fill" : "line.3.horizontal.decrease.circle")
                }
            }
        }
        .onAppear { vm.loadSources(config: config) }
    }
}

struct AudiogramRow: View {
    let record: AudiogramRecord
    @State private var expanded = false

    private var summary: (left: [Int: Double], right: [Int: Double])? {
        guard let data = record.sensitivityPointsJSON.data(using: .utf8),
              let arr = try? JSONSerialization.jsonObject(with: data) as? [[String: Any]] else {
            return nil
        }
        var left: [Int: Double] = [:]
        var right: [Int: Double] = [:]
        for point in arr {
            // Expected payload shape: { frequencyHz, leftEarDb, rightEarDb }
            // (or aliases — handle the common variants defensively)
            let freq = (point["frequencyHz"] as? Double)
                ?? (point["frequency"] as? Double)
                ?? Double(point["frequencyHz"] as? Int ?? 0)
            let leftVal = (point["leftEarDb"] as? Double)
                ?? (point["leftEarSensitivity"] as? Double)
                ?? (point["leftEar"] as? Double)
            let rightVal = (point["rightEarDb"] as? Double)
                ?? (point["rightEarSensitivity"] as? Double)
                ?? (point["rightEar"] as? Double)
            if let l = leftVal { left[Int(freq)] = l }
            if let r = rightVal { right[Int(freq)] = r }
        }
        return (left, right)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Image(systemName: "ear.badge.waveform")
                    .foregroundStyle(.purple)
                Text(record.startDate, style: .date)
                    .font(.subheadline.weight(.medium))
                Spacer()
                Text("\(record.pointCount) freq").font(.caption).foregroundStyle(.secondary)
            }

            if expanded, let sum = summary {
                let freqs = Set(sum.left.keys).union(sum.right.keys).sorted()
                VStack(alignment: .leading, spacing: 2) {
                    HStack {
                        Text("Freq").font(.caption2.monospacedDigit())
                        Spacer()
                        Text("L").font(.caption2)
                        Spacer().frame(width: 12)
                        Text("R").font(.caption2)
                    }
                    .foregroundStyle(.secondary)
                    ForEach(freqs, id: \.self) { f in
                        HStack {
                            Text("\(f) Hz").font(.caption.monospacedDigit())
                            Spacer()
                            Text(sum.left[f].map { String(format: "%.0f dB", $0) } ?? "-")
                                .font(.caption.monospacedDigit())
                            Spacer().frame(width: 12)
                            Text(sum.right[f].map { String(format: "%.0f dB", $0) } ?? "-")
                                .font(.caption.monospacedDigit())
                        }
                    }
                }
                .padding(.top, 4)
            }

            HStack {
                Text(record.startDate, style: .time)
                Spacer()
                if let src = record.sourceName, !src.isEmpty {
                    Text(src).foregroundStyle(.secondary)
                }
            }
            .font(.caption)
            .foregroundStyle(.secondary)
        }
        .padding(.vertical, 2)
        .contentShape(Rectangle())
        .onTapGesture { withAnimation { expanded.toggle() } }
    }
}
