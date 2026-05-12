import SwiftUI

struct StateOfMindDataView: View {
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
                if vm.isLoading && vm.stateOfMindRecords.isEmpty {
                    HStack { ProgressView(); Text("Loading…").foregroundStyle(.secondary) }
                } else if let err = vm.errorMessage {
                    Label(err, systemImage: "exclamationmark.triangle")
                        .foregroundStyle(.red).font(.caption)
                } else if vm.stateOfMindRecords.isEmpty {
                    Text("No State of Mind entries found").foregroundStyle(.secondary)
                } else {
                    ForEach(vm.stateOfMindRecords) { record in
                        StateOfMindRow(record: record)
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
                    Text("Entries")
                    Spacer()
                    if !vm.stateOfMindRecords.isEmpty {
                        Text("\(vm.totalLoaded) loaded").font(.caption).foregroundStyle(.secondary)
                    }
                }
            }
        }
        .navigationTitle("State of Mind")
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

struct StateOfMindRow: View {
    let record: StateOfMindRecord

    var valenceColor: Color {
        switch record.valenceClassification {
        case 1, 2: return .red
        case 3:    return .orange
        case 4:    return .gray
        case 5:    return .yellow
        case 6, 7: return .green
        default:
            if record.valence < -0.3 { return .red }
            if record.valence > 0.3  { return .green }
            return .gray
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack(spacing: 8) {
                Image(systemName: "brain.head.profile")
                    .foregroundStyle(.pink)
                Text(record.valenceLabel)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(valenceColor)
                Spacer()
                Text(record.kindLabel)
                    .font(.caption.weight(.medium))
                    .padding(.horizontal, 6).padding(.vertical, 2)
                    .background(Color.pink.opacity(0.15))
                    .foregroundStyle(.pink)
                    .clipShape(RoundedRectangle(cornerRadius: 4))
            }
            if !record.labels.isEmpty {
                Text("Labels: \(record.labels.map(String.init).joined(separator: ", "))")
                    .font(.caption)
                    .foregroundStyle(.secondary)
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
