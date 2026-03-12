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
}
