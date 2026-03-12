import SwiftUI

struct MedicationDataView: View {
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
                if vm.isLoading && vm.medicationRecords.isEmpty {
                    HStack { ProgressView(); Text("Loading…").foregroundStyle(.secondary) }
                } else if let err = vm.errorMessage {
                    Label(err, systemImage: "exclamationmark.triangle")
                        .foregroundStyle(.red).font(.caption)
                } else if vm.medicationRecords.isEmpty {
                    Text("No medication records found").foregroundStyle(.secondary)
                } else {
                    ForEach(vm.medicationRecords) { record in
                        MedicationRecordRow(record: record)
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
                    if !vm.medicationRecords.isEmpty {
                        Text("\(vm.totalLoaded) loaded")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
        .navigationTitle("Medications")
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

struct MedicationRecordRow: View {
    let record: MedicationRecord

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(record.medicationName ?? "Unknown Medication")
                .font(.subheadline.weight(.medium))
            if let dosage = record.dosage, !dosage.isEmpty {
                Text(dosage)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            HStack {
                Text(record.startDate, style: .date) + Text(" ") + Text(record.startDate, style: .time)
                Spacer()
                if let src = record.sourceName {
                    Text(src).foregroundStyle(.secondary)
                }
            }
            .font(.caption)
            .foregroundStyle(.secondary)
        }
        .padding(.vertical, 2)
    }
}
