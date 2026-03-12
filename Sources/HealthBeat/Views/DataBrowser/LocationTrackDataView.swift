import SwiftUI

struct LocationTrackDataView: View {
    @ObservedObject var vm: DataBrowserViewModel
    let config: MySQLConfig

    @State private var showFilters = false

    var body: some View {
        List {
            if showFilters {
                Section("Filters") {
                    DateFilterRow(label: "From", date: $vm.filterDateFrom)
                    DateFilterRow(label: "To", date: $vm.filterDateTo)
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

            Section {
                if vm.isLoading && vm.locationRecords.isEmpty {
                    HStack { ProgressView(); Text("Loading…").foregroundStyle(.secondary) }
                } else if let err = vm.errorMessage {
                    Label(err, systemImage: "exclamationmark.triangle")
                        .foregroundStyle(.red)
                        .font(.caption)
                } else if vm.locationRecords.isEmpty {
                    Text("No location records found")
                        .foregroundStyle(.secondary)
                } else {
                    ForEach(vm.locationRecords) { record in
                        LocationTrackRow(record: record)
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
            } header: {
                HStack {
                    Text("Records")
                    Spacer()
                    if !vm.locationRecords.isEmpty {
                        Text("\(vm.totalLoaded) loaded")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
        .navigationTitle("Location Tracks")
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button {
                    withAnimation { showFilters.toggle() }
                } label: {
                    Image(systemName: showFilters ? "line.3.horizontal.decrease.circle.fill" : "line.3.horizontal.decrease.circle")
                }
            }
        }
    }
}

struct LocationTrackRow: View {
    let record: LocationTrackRecord

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Text(String(format: "%.6f, %.6f", record.latitude, record.longitude))
                    .font(.subheadline.monospacedDigit())
                Spacer()
                if let acc = record.horizontalAccuracy, acc >= 0 {
                    Text(String(format: "±%.0fm", acc))
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }

            HStack(spacing: 16) {
                if let alt = record.altitude {
                    statBadge(value: String(format: "%.0f m alt", alt), icon: "mountain.2.fill", color: .green)
                }
                if let spd = record.speed, spd >= 0 {
                    statBadge(value: String(format: "%.1f m/s", spd), icon: "speedometer", color: .blue)
                }
            }

            Text(record.timestamp.formatted(date: .abbreviated, time: .shortened))
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .padding(.vertical, 2)
    }

    private func statBadge(value: String, icon: String, color: Color) -> some View {
        HStack(spacing: 3) {
            Image(systemName: icon).font(.caption2).foregroundStyle(color)
            Text(value).font(.caption).foregroundStyle(.secondary)
        }
    }
}
