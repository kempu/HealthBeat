import SwiftUI

struct DataBrowserView: View {
    @StateObject private var vm = DataBrowserViewModel()
    @State private var config = MySQLConfig.load()
    @State private var searchText = ""

    private var filteredQuantityTypes: [QuantityTypeDescriptor] {
        if searchText.isEmpty { return HealthDataTypes.allQuantityTypes }
        return HealthDataTypes.allQuantityTypes.filter {
            $0.displayName.localizedCaseInsensitiveContains(searchText)
        }
    }

    private var filteredCategoryTypes: [CategoryTypeDescriptor] {
        if searchText.isEmpty { return HealthDataTypes.allCategoryTypes }
        return HealthDataTypes.allCategoryTypes.filter {
            $0.displayName.localizedCaseInsensitiveContains(searchText)
        }
    }

    private var showWorkouts: Bool {
        searchText.isEmpty || "Workouts".localizedCaseInsensitiveContains(searchText)
    }

    private var showMedications: Bool {
        searchText.isEmpty || "Medications".localizedCaseInsensitiveContains(searchText)
    }

    private var showLocationTracks: Bool {
        searchText.isEmpty || "Location Tracks".localizedCaseInsensitiveContains(searchText)
    }

    private var showCheckIns: Bool {
        searchText.isEmpty || "Check-ins".localizedCaseInsensitiveContains(searchText)
    }

    private func matchesSearch(_ label: String) -> Bool {
        searchText.isEmpty || label.localizedCaseInsensitiveContains(searchText)
    }

    // Special record families that live in dedicated MySQL tables (one row per family).
    // Each entry is (selectedTypeID, displayName, systemImage, sectionTitle).
    private struct SpecialRecordType {
        let typeID: String
        let title: String
        let icon: String
        let sectionTitle: String
    }

    private let specialRecordTypes: [SpecialRecordType] = [
        .init(typeID: "blood_pressure",       title: "Blood Pressure",        icon: "drop.fill",                          sectionTitle: "Blood Pressure"),
        .init(typeID: "ecg",                  title: "ECG Recordings",        icon: "waveform.path.ecg.rectangle.fill",   sectionTitle: "ECG"),
        .init(typeID: "audiograms",           title: "Audiograms",            icon: "ear.badge.waveform",                 sectionTitle: "Audiograms"),
        .init(typeID: "activity_summaries",   title: "Activity Summaries",    icon: "chart.bar.fill",                     sectionTitle: "Activity Summaries"),
        .init(typeID: "workout_routes",       title: "Workout Routes",        icon: "map.fill",                           sectionTitle: "Workout Routes"),
        .init(typeID: "vision_prescriptions", title: "Vision Prescriptions",  icon: "eyeglasses",                         sectionTitle: "Vision Prescriptions"),
        .init(typeID: "state_of_mind",        title: "State of Mind",         icon: "brain.head.profile",                 sectionTitle: "State of Mind"),
    ]

    var body: some View {
        NavigationStack {
            List {
                // Workouts
                if showWorkouts {
                    Section("Workouts") {
                        NavigationLink {
                            WorkoutDataView(vm: vm, config: config)
                                .onAppear {
                                    vm.resetFilters()
                                    vm.selectedTypeID = "workout"
                                    vm.loadData(config: config)
                                }
                        } label: {
                            Label("Workouts", systemImage: "dumbbell.fill")
                        }
                    }
                }

                // Medications
                if showMedications {
                    Section("Medications") {
                        NavigationLink {
                            MedicationDataView(vm: vm, config: config)
                                .onAppear {
                                    vm.resetFilters()
                                    vm.selectedTypeID = "medications"
                                    vm.loadData(config: config)
                                }
                        } label: {
                            Label("Medications", systemImage: "pills.fill")
                        }
                    }
                }

                // Location
                if showLocationTracks || showCheckIns {
                    Section("Location") {
                        if showLocationTracks {
                            NavigationLink {
                                LocationTrackDataView(vm: vm, config: config)
                                    .onAppear {
                                        vm.resetFilters()
                                        vm.selectedTypeID = "location_tracks"
                                        vm.loadData(config: config)
                                    }
                            } label: {
                                Label("Location Tracks", systemImage: "location.fill")
                            }
                        }
                        if showCheckIns {
                            NavigationLink {
                                CheckInDataView(vm: vm, config: config)
                                    .onAppear {
                                        vm.resetFilters()
                                        vm.selectedTypeID = "location_geofence_events"
                                        vm.loadData(config: config)
                                    }
                            } label: {
                                Label("Check-ins", systemImage: "mappin.circle.fill")
                            }
                        }
                    }
                }

                // Dedicated-table record families (Blood Pressure, ECG, Audiograms, etc.)
                ForEach(specialRecordTypes, id: \.typeID) { entry in
                    if matchesSearch(entry.title) {
                        Section(entry.sectionTitle) {
                            NavigationLink {
                                specialRecordDestination(for: entry.typeID)
                                    .onAppear {
                                        vm.resetFilters()
                                        vm.selectedTypeID = entry.typeID
                                        vm.loadData(config: config)
                                    }
                            } label: {
                                Label(entry.title, systemImage: entry.icon)
                            }
                        }
                    }
                }

                // Category types grouped
                ForEach(HealthCategory.allCases) { cat in
                    let qtypes = filteredQuantityTypes.filter { $0.category == cat }
                    let ctypes = filteredCategoryTypes.filter { $0.category == cat }
                    if !qtypes.isEmpty || !ctypes.isEmpty {
                        Section(cat.rawValue) {
                            ForEach(qtypes) { t in
                                NavigationLink {
                                    QuantityDataView(vm: vm, typeID: t.id, displayName: t.displayName, unit: t.unitString, config: config)
                                        .onAppear {
                                            vm.resetFilters()
                                            vm.selectedTypeID = t.id
                                            vm.loadData(config: config)
                                        }
                                } label: {
                                    Label(t.displayName, systemImage: cat.systemImage)
                                }
                            }
                            ForEach(ctypes) { t in
                                NavigationLink {
                                    CategoryDataView(vm: vm, typeDesc: t, config: config)
                                        .onAppear {
                                            vm.resetFilters()
                                            vm.selectedTypeID = t.id
                                            vm.loadData(config: config)
                                        }
                                } label: {
                                    Label(t.displayName, systemImage: cat.systemImage)
                                }
                            }
                        }
                    }
                }

                BrandFooter()
            }
            .searchable(text: $searchText, prompt: "Search health data types")
            .navigationTitle("Data Browser")
            .onAppear { config = MySQLConfig.load() }
        }
    }

    @ViewBuilder
    private func specialRecordDestination(for typeID: String) -> some View {
        switch typeID {
        case "blood_pressure":       BloodPressureDataView(vm: vm, config: config)
        case "ecg":                  ECGDataView(vm: vm, config: config)
        case "audiograms":           AudiogramDataView(vm: vm, config: config)
        case "activity_summaries":   ActivitySummaryDataView(vm: vm, config: config)
        case "workout_routes":       WorkoutRouteDataView(vm: vm, config: config)
        case "vision_prescriptions": VisionPrescriptionDataView(vm: vm, config: config)
        case "state_of_mind":        StateOfMindDataView(vm: vm, config: config)
        default:                     EmptyView()
        }
    }
}
