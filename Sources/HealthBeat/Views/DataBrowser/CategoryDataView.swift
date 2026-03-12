import SwiftUI
import HealthKit

struct CategoryDataView: View {
    @ObservedObject var vm: DataBrowserViewModel
    let typeDesc: CategoryTypeDescriptor
    let config: MySQLConfig

    var body: some View {
        Group {
            // Use sleep-specific view for sleep analysis
            if typeDesc.id == HKCategoryTypeIdentifier.sleepAnalysis.rawValue {
                SleepDataView(vm: vm, config: config)
            } else {
                GenericCategoryListView(vm: vm, typeDesc: typeDesc, config: config)
            }
        }
        .navigationTitle(typeDesc.displayName)
    }
}

struct GenericCategoryListView: View {
    @ObservedObject var vm: DataBrowserViewModel
    let typeDesc: CategoryTypeDescriptor
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
                if vm.isLoading && vm.records.isEmpty {
                    HStack { ProgressView(); Text("Loading…").foregroundStyle(.secondary) }
                } else if let err = vm.errorMessage {
                    Label(err, systemImage: "exclamationmark.triangle")
                        .foregroundStyle(.red).font(.caption)
                } else if vm.records.isEmpty {
                    Text("No records found").foregroundStyle(.secondary)
                } else {
                    ForEach(vm.records) { record in
                        CategoryRecordRow(record: record, valueLabels: typeDesc.valueLabels)
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
                    if !vm.records.isEmpty {
                        Text("\(vm.totalLoaded) loaded")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
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

struct CategoryRecordRow: View {
    let record: HealthRecord
    let valueLabels: [Int: String]

    var label: String {
        if let lbl = record.valueLabel { return lbl }
        if let v = record.value, let lbl = valueLabels[Int(v)] { return lbl }
        return record.value.map { "\(Int($0))" } ?? "–"
    }

    var duration: String? {
        let secs = record.endDate.timeIntervalSince(record.startDate)
        if secs < 60 { return nil }
        let mins = Int(secs / 60)
        if mins >= 60 { return "\(mins / 60)h \(mins % 60)m" }
        return "\(mins)m"
    }

    var body: some View {
        HStack {
            VStack(alignment: .leading, spacing: 2) {
                Text(record.startDate, style: .date) + Text(" ") + Text(record.startDate, style: .time)
                if let src = record.sourceName {
                    Text(src).font(.caption).foregroundStyle(.secondary)
                }
            }
            Spacer()
            VStack(alignment: .trailing, spacing: 2) {
                Text(label)
                    .font(.subheadline.weight(.medium))
                if let dur = duration {
                    Text(dur).font(.caption).foregroundStyle(.secondary)
                }
            }
        }
    }
}
