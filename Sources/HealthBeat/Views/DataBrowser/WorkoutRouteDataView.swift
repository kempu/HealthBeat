import SwiftUI

struct WorkoutRouteDataView: View {
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
                        Button("Apply") { vm.loadData(config: config) }
                            .buttonStyle(.borderedProminent).controlSize(.small)
                        Button("Reset") { vm.resetFilters(); vm.loadData(config: config) }
                            .buttonStyle(.bordered).controlSize(.small)
                    }
                }
            }

            Section {
                if vm.isLoading && vm.workoutRouteRecords.isEmpty {
                    HStack { ProgressView(); Text("Loading…").foregroundStyle(.secondary) }
                } else if let err = vm.errorMessage {
                    Label(err, systemImage: "exclamationmark.triangle")
                        .foregroundStyle(.red).font(.caption)
                } else if vm.workoutRouteRecords.isEmpty {
                    Text("No workout routes found").foregroundStyle(.secondary)
                } else {
                    ForEach(vm.workoutRouteRecords) { record in
                        WorkoutRouteRow(record: record)
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
                    Text("Routes")
                    Spacer()
                    if !vm.workoutRouteRecords.isEmpty {
                        Text("\(vm.totalLoaded) loaded").font(.caption).foregroundStyle(.secondary)
                    }
                }
            }
        }
        .navigationTitle("Workout Routes")
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

struct WorkoutRouteRow: View {
    let record: WorkoutRouteRecord

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Image(systemName: "map.fill").foregroundStyle(.green)
                Text(record.startDate, style: .date)
                    .font(.subheadline.weight(.medium))
                Spacer()
                Text("\(record.locationCount) pts")
                    .font(.caption.monospacedDigit())
                    .foregroundStyle(.secondary)
            }
            HStack {
                Text(record.startDate, style: .time)
                Spacer()
                Text("Workout \(record.workoutUUID.prefix(8))…")
                    .font(.caption2.monospaced())
                    .foregroundStyle(.tertiary)
            }
            .font(.caption)
            .foregroundStyle(.secondary)
        }
        .padding(.vertical, 2)
    }
}
