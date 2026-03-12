import SwiftUI
import Charts

struct SleepDataView: View {
    @ObservedObject var vm: DataBrowserViewModel
    let config: MySQLConfig

    @State private var showFilters = false

    // Group sleep records by night
    private var nights: [(Date, [HealthRecord])] {
        let cal = Calendar.current
        var grouped: [Date: [HealthRecord]] = [:]
        for r in vm.records {
            // Use noon of the day before as the "night key"
            let noon = cal.date(bySettingHour: 12, minute: 0, second: 0, of: r.startDate) ?? r.startDate
            grouped[noon, default: []].append(r)
        }
        return grouped.sorted { $0.key > $1.key }
    }

    var body: some View {
        List {
            // Filters
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
                        Button("Apply") {
                            vm.loadData(config: config)
                        }
                        .buttonStyle(.borderedProminent)
                        .controlSize(.small)

                        Button("Reset") {
                            vm.resetFilters()
                            vm.loadData(config: config)
                        }
                        .buttonStyle(.bordered)
                        .controlSize(.small)
                    }
                }
            }

            // Sleep data
            if vm.isLoading && vm.records.isEmpty {
                HStack { ProgressView(); Text("Loading…").foregroundStyle(.secondary) }
            } else if let err = vm.errorMessage {
                Label(err, systemImage: "exclamationmark.triangle")
                    .foregroundStyle(.red).font(.caption)
            } else if vm.records.isEmpty {
                Text("No sleep data found").foregroundStyle(.secondary)
            } else {
                ForEach(nights, id: \.0) { (night, records) in
                    Section {
                        SleepNightRow(records: records)
                    } header: {
                        HStack {
                            Text(night.formatted(date: .abbreviated, time: .omitted))
                            Spacer()
                            if night == nights.first?.0 {
                                Text("\(vm.totalLoaded) loaded")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                        }
                    }
                }
                if vm.isLoading {
                    HStack {
                        Spacer()
                        ProgressView()
                        Text("Loading…").foregroundStyle(.secondary)
                        Spacer()
                    }
                } else {
                    Button {
                        vm.loadNextPage(config: config)
                    } label: {
                        HStack {
                            Spacer()
                            Label("Load More", systemImage: "arrow.down.circle")
                            Spacer()
                        }
                    }
                    .foregroundStyle(.blue)
                }
            }
        }
        .navigationTitle("Sleep")
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button {
                    withAnimation { showFilters.toggle() }
                } label: {
                    Image(systemName: showFilters ? "line.3.horizontal.decrease.circle.fill" : "line.3.horizontal.decrease.circle")
                }
            }
        }
        .onAppear {
            vm.loadSources(config: config)
        }
    }
}

struct SleepNightRow: View {
    let records: [HealthRecord]

    private let stageColors: [String: Color] = [
        "Asleep Core": .blue,
        "Asleep Deep": .indigo,
        "Asleep REM": .purple,
        "Awake": .orange,
        "In Bed": .gray,
        "Asleep Unspecified": .blue,
    ]

    var totalHours: Double {
        records.reduce(0.0) { $0 + $1.endDate.timeIntervalSince($1.startDate) } / 3600
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            // Total time
            HStack {
                Image(systemName: "bed.double.fill")
                    .foregroundStyle(.indigo)
                Text(String(format: "%.1f hours", totalHours))
                    .font(.subheadline.weight(.semibold))
            }

            // Sleep stage timeline bar
            if let earliest = records.min(by: { $0.startDate < $1.startDate })?.startDate,
               let latest = records.max(by: { $0.endDate < $1.endDate })?.endDate {

                let totalSpan = latest.timeIntervalSince(earliest)
                if totalSpan > 0 {
                    GeometryReader { geo in
                        HStack(spacing: 0) {
                            ForEach(records.sorted { $0.startDate < $1.startDate }) { r in
                                let width = geo.size.width * CGFloat(r.endDate.timeIntervalSince(r.startDate) / totalSpan)
                                let label = r.valueLabel ?? "Unknown"
                                Rectangle()
                                    .fill(stageColors[label] ?? .gray)
                                    .frame(width: max(1, width))
                            }
                        }
                    }
                    .frame(height: 16)
                    .clipShape(RoundedRectangle(cornerRadius: 4))
                }
            }

            // Legend
            let stages = Dictionary(grouping: records) { $0.valueLabel ?? "Unknown" }
            HStack(spacing: 8) {
                ForEach(Array(stages.keys.sorted()), id: \.self) { stage in
                    HStack(spacing: 3) {
                        Circle()
                            .fill(stageColors[stage] ?? .gray)
                            .frame(width: 8, height: 8)
                        Text(stage)
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
        .padding(.vertical, 4)
    }
}
