import SwiftUI

struct ActivitySummaryDataView: View {
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
                if vm.isLoading && vm.activitySummaryRecords.isEmpty {
                    HStack { ProgressView(); Text("Loading…").foregroundStyle(.secondary) }
                } else if let err = vm.errorMessage {
                    Label(err, systemImage: "exclamationmark.triangle")
                        .foregroundStyle(.red).font(.caption)
                } else if vm.activitySummaryRecords.isEmpty {
                    Text("No daily activity summaries found").foregroundStyle(.secondary)
                } else {
                    ForEach(vm.activitySummaryRecords) { record in
                        ActivitySummaryRow(record: record)
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
                    Text("Daily Summaries")
                    Spacer()
                    if !vm.activitySummaryRecords.isEmpty {
                        Text("\(vm.totalLoaded) loaded").font(.caption).foregroundStyle(.secondary)
                    }
                }
            }
        }
        .navigationTitle("Activity Summaries")
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

struct ActivitySummaryRow: View {
    let record: ActivitySummaryRecord

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(record.date, style: .date)
                .font(.subheadline.weight(.semibold))

            HStack(spacing: 16) {
                ringStat(
                    icon: "flame.fill",
                    color: .red,
                    value: record.activeEnergyBurned.map { "\(Int($0))" } ?? "-",
                    goal: record.activeEnergyBurnedGoal.map { "\(Int($0))" } ?? nil,
                    unit: "kcal"
                )
                ringStat(
                    icon: "figure.run",
                    color: .green,
                    value: record.exerciseTimeMinutes.map { "\(Int($0))" } ?? "-",
                    goal: record.exerciseTimeGoalMinutes.map { "\(Int($0))" } ?? nil,
                    unit: "min"
                )
                ringStat(
                    icon: "figure.stand",
                    color: .blue,
                    value: record.standHours.map { "\($0)" } ?? "-",
                    goal: record.standHoursGoal.map { "\($0)" } ?? nil,
                    unit: "hr"
                )
            }
        }
        .padding(.vertical, 2)
    }

    private func ringStat(icon: String, color: Color, value: String, goal: String?, unit: String) -> some View {
        HStack(spacing: 4) {
            Image(systemName: icon)
                .font(.caption2)
                .foregroundStyle(color)
            VStack(alignment: .leading, spacing: 0) {
                if let g = goal {
                    Text("\(value)/\(g)")
                        .font(.caption.monospacedDigit())
                } else {
                    Text(value)
                        .font(.caption.monospacedDigit())
                }
                Text(unit)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
        }
    }
}
