import SwiftUI

struct CheckInDataView: View {
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
                if vm.isLoading && vm.checkInRecords.isEmpty {
                    HStack { ProgressView(); Text("Loading…").foregroundStyle(.secondary) }
                } else if let err = vm.errorMessage {
                    Label(err, systemImage: "exclamationmark.triangle")
                        .foregroundStyle(.red)
                        .font(.caption)
                } else if vm.checkInRecords.isEmpty {
                    Text("No check-in records found")
                        .foregroundStyle(.secondary)
                } else {
                    ForEach(vm.checkInRecords) { record in
                        CheckInRow(record: record)
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
                    if !vm.checkInRecords.isEmpty {
                        Text("\(vm.totalLoaded) loaded")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
        .navigationTitle("Check-ins")
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

struct CheckInRow: View {
    let record: CheckInRecord

    private var isArrive: Bool { record.eventType == "arrive" }

    var body: some View {
        HStack(spacing: 12) {
            Image(systemName: isArrive ? "arrow.down.circle.fill" : "arrow.up.circle.fill")
                .font(.title3)
                .foregroundStyle(isArrive ? .green : .orange)

            VStack(alignment: .leading, spacing: 3) {
                Text(record.placeName)
                    .font(.subheadline.weight(.medium))
                HStack(spacing: 8) {
                    if let placeType = record.placeType, !placeType.isEmpty {
                        Text(placeType)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Text("·")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    Text(record.timestamp.formatted(date: .abbreviated, time: .shortened))
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }

            Spacer()

            Text(isArrive ? "Arrived" : "Departed")
                .font(.caption.weight(.medium))
                .foregroundStyle(isArrive ? .green : .orange)
        }
        .padding(.vertical, 2)
    }
}
