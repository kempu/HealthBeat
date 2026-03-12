import SwiftUI
import Charts

struct QuantityDataView: View {
    @ObservedObject var vm: DataBrowserViewModel
    let typeID: String
    let displayName: String
    let unit: String
    let config: MySQLConfig

    @State private var chartData: [(Date, Double)] = []
    @State private var loadingChart = true
    @State private var chartRange: ChartTimeRange = .month
    @State private var showFilters = false

    var body: some View {
        List {
            // Chart
            chartSection

            // Filters
            filterSection

            // Records
            Section {
                if vm.isLoading && vm.records.isEmpty {
                    HStack {
                        ProgressView()
                        Text("Loading…").foregroundStyle(.secondary)
                    }
                } else if let err = vm.errorMessage {
                    Label(err, systemImage: "exclamationmark.triangle")
                        .foregroundStyle(.red)
                        .font(.caption)
                } else if vm.records.isEmpty {
                    Text("No records found")
                        .foregroundStyle(.secondary)
                } else {
                    ForEach(vm.records) { record in
                        QuantityRecordRow(record: record, unit: unit)
                    }
                    paginationControls
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
        .navigationTitle(displayName)
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button {
                    withAnimation { showFilters.toggle() }
                } label: {
                    Image(systemName: showFilters ? "line.3.horizontal.decrease.circle.fill" : "line.3.horizontal.decrease.circle")
                }
            }
        }
        .task {
            chartData = await vm.loadChartData(config: config, typeID: typeID, range: chartRange)
            loadingChart = false
        }
        .onAppear {
            vm.loadSources(config: config)
        }
    }

    // MARK: - Chart section

    @ViewBuilder
    private var chartSection: some View {
        Section {
            // Time range picker
            Picker("Range", selection: $chartRange) {
                ForEach(ChartTimeRange.allCases) { range in
                    Text(range.rawValue).tag(range)
                }
            }
            .pickerStyle(.segmented)
            .onChange(of: chartRange) { _, newRange in
                loadingChart = true
                Task {
                    chartData = await vm.loadChartData(config: config, typeID: typeID, range: newRange)
                    loadingChart = false
                }
            }

            if !chartData.isEmpty {
                Chart(chartData, id: \.0) { item in
                    LineMark(
                        x: .value("Date", item.0),
                        y: .value(unit, item.1)
                    )
                    .foregroundStyle(.blue)
                    AreaMark(
                        x: .value("Date", item.0),
                        y: .value(unit, item.1)
                    )
                    .foregroundStyle(.blue.opacity(0.1))
                }
                .frame(height: 180)
                .chartXAxis {
                    AxisMarks(values: .stride(by: chartRange.strideComponent, count: chartRange.strideCount)) { _ in
                        AxisGridLine()
                        AxisValueLabel(format: .dateTime.month().day())
                    }
                }
                .chartYAxisLabel(unit)
            } else if loadingChart {
                HStack {
                    ProgressView()
                    Text("Loading chart…").foregroundStyle(.secondary)
                }
            } else {
                Text("No chart data available")
                    .foregroundStyle(.secondary)
            }
        } header: {
            Text("Chart")
        }
    }

    // MARK: - Filter section

    @ViewBuilder
    private var filterSection: some View {
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
    }

    // MARK: - Pagination

    @ViewBuilder
    private var paginationControls: some View {
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
}

// MARK: - Reusable date filter row

struct DateFilterRow: View {
    let label: String
    @Binding var date: Date?
    @State private var isEnabled = false
    @State private var pickerDate = Date()

    var body: some View {
        HStack {
            Toggle(label, isOn: $isEnabled)
                .onChange(of: isEnabled) { _, on in
                    date = on ? pickerDate : nil
                }
            if isEnabled {
                DatePicker("", selection: $pickerDate, displayedComponents: .date)
                    .labelsHidden()
                    .onChange(of: pickerDate) { _, d in
                        date = d
                    }
            }
        }
        .onAppear {
            if let d = date {
                isEnabled = true
                pickerDate = d
            }
        }
    }
}

struct QuantityRecordRow: View {
    let record: HealthRecord
    let unit: String

    var body: some View {
        HStack {
            VStack(alignment: .leading, spacing: 2) {
                Text(record.startDate, style: .date) + Text(" ") + Text(record.startDate, style: .time)
                if let src = record.sourceName {
                    Text(src)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            Spacer()
            if let value = record.value {
                Text("\(value, specifier: "%.2f") \(unit)")
                    .font(.subheadline.monospacedDigit())
                    .foregroundStyle(.primary)
            }
        }
    }
}
