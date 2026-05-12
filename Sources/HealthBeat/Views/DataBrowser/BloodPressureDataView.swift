import SwiftUI

struct BloodPressureDataView: View {
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
                if vm.isLoading && vm.bloodPressureRecords.isEmpty {
                    HStack { ProgressView(); Text("Loading…").foregroundStyle(.secondary) }
                } else if let err = vm.errorMessage {
                    Label(err, systemImage: "exclamationmark.triangle")
                        .foregroundStyle(.red).font(.caption)
                } else if vm.bloodPressureRecords.isEmpty {
                    Text("No blood pressure readings found").foregroundStyle(.secondary)
                } else {
                    ForEach(vm.bloodPressureRecords) { record in
                        BloodPressureRow(record: record)
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
                    Text("Readings")
                    Spacer()
                    if !vm.bloodPressureRecords.isEmpty {
                        Text("\(vm.totalLoaded) loaded").font(.caption).foregroundStyle(.secondary)
                    }
                }
            }
        }
        .navigationTitle("Blood Pressure")
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

struct BloodPressureRow: View {
    let record: BloodPressureRecord

    var category: (label: String, color: Color) {
        // AHA categories
        switch (record.systolic, record.diastolic) {
        case let (s, d) where s >= 180 || d >= 120: return ("Crisis", .red)
        case let (s, d) where s >= 140 || d >= 90:  return ("Stage 2", .red)
        case let (s, d) where s >= 130 || d >= 80:  return ("Stage 1", .orange)
        case let (s, _) where s >= 120:             return ("Elevated", .yellow)
        default:                                    return ("Normal", .green)
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack(spacing: 8) {
                Text("\(Int(record.systolic))/\(Int(record.diastolic))")
                    .font(.title3.weight(.semibold).monospacedDigit())
                Text("mmHg")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Spacer()
                Text(category.label)
                    .font(.caption.weight(.medium))
                    .padding(.horizontal, 6).padding(.vertical, 2)
                    .background(category.color.opacity(0.15))
                    .foregroundStyle(category.color)
                    .clipShape(RoundedRectangle(cornerRadius: 4))
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
