import SwiftUI

struct WorkoutDataView: View {
    @ObservedObject var vm: DataBrowserViewModel
    let config: MySQLConfig

    @State private var showFilters = false

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

            // Records
            Section {
                if vm.isLoading && vm.workoutRecords.isEmpty {
                    HStack { ProgressView(); Text("Loading…").foregroundStyle(.secondary) }
                } else if let err = vm.errorMessage {
                    Label(err, systemImage: "exclamationmark.triangle")
                        .foregroundStyle(.red)
                        .font(.caption)
                } else if vm.workoutRecords.isEmpty {
                    Text("No workouts found")
                        .foregroundStyle(.secondary)
                } else {
                    ForEach(vm.workoutRecords) { workout in
                        WorkoutRow(workout: workout)
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
                    Text("Workouts")
                    Spacer()
                    if !vm.workoutRecords.isEmpty {
                        Text("\(vm.totalLoaded) loaded")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
        .navigationTitle("Workouts")
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

struct WorkoutRow: View {
    let workout: WorkoutRecord

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text(workout.activityType)
                    .font(.subheadline.weight(.semibold))
                Spacer()
                Text(workout.durationFormatted)
                    .font(.subheadline.monospacedDigit())
                    .foregroundStyle(.secondary)
            }

            HStack(spacing: 16) {
                if let kcal = workout.energyKcal {
                    statBadge(value: "\(Int(kcal)) kcal", icon: "flame.fill", color: .orange)
                }
                if let dist = workout.distanceMeters {
                    statBadge(value: distanceLabel(dist), icon: "location.fill", color: .blue)
                }
            }

            Text(workout.startDate, style: .date)
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .padding(.vertical, 2)
    }

    private func distanceLabel(_ meters: Double) -> String {
        if meters >= 1000 {
            return String(format: "%.2f km", meters / 1000)
        }
        return String(format: "%.0f m", meters)
    }

    private func statBadge(value: String, icon: String, color: Color) -> some View {
        HStack(spacing: 3) {
            Image(systemName: icon)
                .font(.caption2)
                .foregroundStyle(color)
            Text(value)
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }
}
