import SwiftUI

struct VisionPrescriptionDataView: View {
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
                if vm.isLoading && vm.visionPrescriptionRecords.isEmpty {
                    HStack { ProgressView(); Text("Loading…").foregroundStyle(.secondary) }
                } else if let err = vm.errorMessage {
                    Label(err, systemImage: "exclamationmark.triangle")
                        .foregroundStyle(.red).font(.caption)
                } else if vm.visionPrescriptionRecords.isEmpty {
                    Text("No vision prescriptions found").foregroundStyle(.secondary)
                } else {
                    ForEach(vm.visionPrescriptionRecords) { record in
                        VisionPrescriptionRow(record: record)
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
                    Text("Prescriptions")
                    Spacer()
                    if !vm.visionPrescriptionRecords.isEmpty {
                        Text("\(vm.totalLoaded) loaded").font(.caption).foregroundStyle(.secondary)
                    }
                }
            }
        }
        .navigationTitle("Vision Prescriptions")
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

struct VisionPrescriptionRow: View {
    let record: VisionPrescriptionRecord

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Image(systemName: "eyeglasses").foregroundStyle(.indigo)
                Text(record.prescriptionTypeLabel)
                    .font(.subheadline.weight(.semibold))
                Spacer()
                Text(record.startDate, style: .date)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            // Per-eye summary: Sphere / Cylinder × Axis
            HStack(spacing: 12) {
                eyeLabel(side: "R",
                         sphere: record.rightEyeSphere,
                         cylinder: record.rightEyeCylinder,
                         axis: record.rightEyeAxis)
                eyeLabel(side: "L",
                         sphere: record.leftEyeSphere,
                         cylinder: record.leftEyeCylinder,
                         axis: record.leftEyeAxis)
            }
            HStack {
                if let exp = record.expirationDate {
                    Label("Expires \(exp.formatted(date: .abbreviated, time: .omitted))",
                          systemImage: "calendar.badge.clock")
                }
                Spacer()
                if let src = record.sourceName, !src.isEmpty {
                    Text(src)
                }
            }
            .font(.caption)
            .foregroundStyle(.secondary)
        }
        .padding(.vertical, 2)
    }

    private func eyeLabel(side: String, sphere: Double?, cylinder: Double?, axis: Double?) -> some View {
        HStack(spacing: 4) {
            Text(side)
                .font(.caption.weight(.bold))
                .foregroundStyle(.secondary)
            if let sph = sphere {
                Text(String(format: "%+.2f", sph))
                    .font(.caption.monospacedDigit())
            } else {
                Text("-").font(.caption)
            }
            if let cyl = cylinder, let ax = axis {
                Text(String(format: "/ %+.2f × %.0f°", cyl, ax))
                    .font(.caption2.monospacedDigit())
                    .foregroundStyle(.secondary)
            }
        }
    }
}
