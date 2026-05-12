import SwiftUI

struct ECGDataView: View {
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
                if vm.isLoading && vm.ecgRecords.isEmpty {
                    HStack { ProgressView(); Text("Loading…").foregroundStyle(.secondary) }
                } else if let err = vm.errorMessage {
                    Label(err, systemImage: "exclamationmark.triangle")
                        .foregroundStyle(.red).font(.caption)
                } else if vm.ecgRecords.isEmpty {
                    Text("No ECG recordings found").foregroundStyle(.secondary)
                } else {
                    ForEach(vm.ecgRecords) { record in
                        ECGRow(record: record)
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
                    Text("Recordings")
                    Spacer()
                    if !vm.ecgRecords.isEmpty {
                        Text("\(vm.totalLoaded) loaded").font(.caption).foregroundStyle(.secondary)
                    }
                }
            }
        }
        .navigationTitle("ECG")
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

struct ECGRow: View {
    let record: ECGRecord

    var classificationColor: Color {
        switch (record.classification ?? "").lowercased() {
        case let c where c.contains("sinus"):       return .green
        case let c where c.contains("afib"),
             let c where c.contains("atrial"):      return .orange
        case let c where c.contains("inconclusive"): return .yellow
        case let c where c.contains("unclassif"):   return .gray
        default:                                    return .secondary
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Text(record.classification ?? "Unknown")
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(classificationColor)
                Spacer()
                if let bpm = record.averageHeartRate {
                    HStack(spacing: 3) {
                        Image(systemName: "heart.fill").font(.caption2).foregroundStyle(.red)
                        Text("\(Int(bpm)) bpm").font(.caption.monospacedDigit())
                    }
                }
            }
            HStack(spacing: 12) {
                if record.voltageMeasurementCount > 0 {
                    Label("\(record.voltageMeasurementCount) samples", systemImage: "waveform.path.ecg")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
                if let freq = record.samplingFrequency {
                    Text(String(format: "%.0f Hz", freq))
                        .font(.caption2.monospacedDigit())
                        .foregroundStyle(.secondary)
                }
            }
            HStack {
                Text(record.startDate, style: .date) + Text(" ") + Text(record.startDate, style: .time)
                Spacer()
                if let src = record.sourceName, !src.isEmpty {
                    Text(src).foregroundStyle(.secondary)
                }
            }
            .font(.caption)
            .foregroundStyle(.secondary)
        }
        .padding(.vertical, 2)
    }
}
