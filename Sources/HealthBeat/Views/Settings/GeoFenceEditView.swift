import CoreLocation
import MapKit
import SwiftUI

enum GeoFenceEditMode {
    case add
    case edit(GeoFence)
}

struct GeoFenceEditView: View {
    let mode: GeoFenceEditMode
    let vm: LocationViewModel

    @Environment(\.dismiss) private var dismiss

    @State private var name: String = ""
    @State private var selectedCategoryId: UUID?
    @State private var latitudeText: String = ""
    @State private var longitudeText: String = ""
    @State private var radiusText: String = "100"
    @State private var showMapPicker = false
    @State private var categories: [PlaceCategory] = PlaceCategory.loadAll()

    var body: some View {
        Form {
            Section {
                TextField("Name (e.g. Home, Work)", text: $name)

                Picker("Type", selection: $selectedCategoryId) {
                    Text("None").tag(UUID?.none)
                    ForEach(categories) { cat in
                        HStack {
                            Image(systemName: cat.systemImage)
                            Text(cat.name)
                        }
                        .tag(Optional(cat.id))
                    }
                }

                TextField("Latitude", text: $latitudeText)
                    .keyboardType(.decimalPad)

                TextField("Longitude", text: $longitudeText)
                    .keyboardType(.decimalPad)

                TextField("Radius (meters)", text: $radiusText)
                    .keyboardType(.numberPad)

                Button {
                    showMapPicker = true
                } label: {
                    Label("Pick on Map", systemImage: "map.fill")
                }
            } header: {
                Text("Place Details")
            } footer: {
                Text("Tap \"Pick on Map\" to visually select a location and radius, or enter coordinates manually.")
                    .font(.caption)
            }

            // Inline map preview when coordinates are valid
            if let lat = Double(latitudeText), let lon = Double(longitudeText),
               let radius = Double(radiusText), radius > 0 {
                Section("Preview") {
                    GeoFenceMapPreview(
                        latitude: lat,
                        longitude: lon,
                        radius: radius
                    )
                    .frame(height: 200)
                    .listRowInsets(.init())
                }
            }
        }
        .navigationTitle(navigationTitle)
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .cancellationAction) {
                Button("Cancel") { dismiss() }
            }
            ToolbarItem(placement: .confirmationAction) {
                Button("Save") { save() }
                    .disabled(!isValid)
            }
        }
        .onAppear { loadExisting() }
        .sheet(isPresented: $showMapPicker) {
            GeoFenceMapPicker(
                latitude: Double(latitudeText) ?? 0,
                longitude: Double(longitudeText) ?? 0,
                radius: Double(radiusText) ?? 100,
                currentLocation: LocationService.shared.lastKnownLocation
            ) { lat, lon, radius in
                latitudeText = String(format: "%.6f", lat)
                longitudeText = String(format: "%.6f", lon)
                radiusText = String(format: "%.0f", radius)
            }
        }
    }

    private var navigationTitle: String {
        if case .edit = mode { return "Edit Place" }
        return "Add Place"
    }

    private var isValid: Bool {
        !name.trimmingCharacters(in: .whitespaces).isEmpty &&
        Double(latitudeText) != nil &&
        Double(longitudeText) != nil &&
        Double(radiusText) != nil
    }

    private func loadExisting() {
        guard case .edit(let fence) = mode else { return }
        name = fence.name
        selectedCategoryId = fence.placeCategoryId
        latitudeText = "\(fence.latitude)"
        longitudeText = "\(fence.longitude)"
        radiusText = "\(fence.radius)"
    }

    private func save() {
        guard let lat = Double(latitudeText),
              let lon = Double(longitudeText),
              let radius = Double(radiusText) else { return }

        switch mode {
        case .add:
            let fence = GeoFence(
                name: name.trimmingCharacters(in: .whitespaces),
                latitude: lat,
                longitude: lon,
                radius: radius,
                placeCategoryId: selectedCategoryId
            )
            vm.addGeofence(fence)
        case .edit(let existing):
            var updated = existing
            updated.name = name.trimmingCharacters(in: .whitespaces)
            updated.latitude = lat
            updated.longitude = lon
            updated.radius = radius
            updated.placeCategoryId = selectedCategoryId
            vm.updateGeofence(updated)
        }
        dismiss()
    }
}

// MARK: - Map Picker (full-screen sheet)

struct GeoFenceMapPicker: View {
    @Environment(\.dismiss) private var dismiss

    @State private var region: MKCoordinateRegion
    @State private var position: MapCameraPosition
    @State private var pinCoordinate: CLLocationCoordinate2D
    @State private var radius: Double
    @State private var isDragging = false

    @State private var searchText = ""
    @State private var searchResults: [MKMapItem] = []
    @State private var isSearching = false

    let onSave: (Double, Double, Double) -> Void

    init(latitude: Double, longitude: Double, radius: Double, currentLocation: CLLocationCoordinate2D? = nil, onSave: @escaping (Double, Double, Double) -> Void) {
        let center: CLLocationCoordinate2D
        if latitude == 0 && longitude == 0 {
            center = currentLocation ?? CLLocationCoordinate2D(latitude: 37.7749, longitude: -122.4194)
        } else {
            center = CLLocationCoordinate2D(latitude: latitude, longitude: longitude)
        }
        let span = MKCoordinateSpan(
            latitudeDelta: max(radius / 50000, 0.005),
            longitudeDelta: max(radius / 50000, 0.005)
        )
        let initialRegion = MKCoordinateRegion(center: center, span: span)
        _region = State(initialValue: initialRegion)
        _position = State(initialValue: .region(initialRegion))
        _pinCoordinate = State(initialValue: center)
        _radius = State(initialValue: radius)
        self.onSave = onSave
    }

    var body: some View {
        NavigationStack {
            ZStack(alignment: .top) {
                Map(position: $position) {
                    Annotation("", coordinate: pinCoordinate) {
                        VStack(spacing: 0) {
                            Image(systemName: "mappin.circle.fill")
                                .font(.system(size: 30))
                                .foregroundStyle(.red)
                            Circle()
                                .fill(Color.blue.opacity(0.15))
                                .stroke(Color.blue.opacity(0.4), lineWidth: 1)
                                .frame(
                                    width: radiusInPoints,
                                    height: radiusInPoints
                                )
                        }
                    }
                }
                .ignoresSafeArea(edges: .bottom)
                .onMapCameraChange(frequency: .continuous) { context in
                    region = context.region
                    isDragging = true
                    Task {
                        try? await Task.sleep(nanoseconds: 500_000_000)
                        isDragging = false
                    }
                }
                .overlay(alignment: .center) {
                    if isDragging {
                        Image(systemName: "plus")
                            .font(.title2)
                            .foregroundStyle(.blue.opacity(0.6))
                    }
                }

                // Bottom controls
                VStack {
                    Spacer()

                    VStack(spacing: 12) {
                        Button {
                            pinCoordinate = region.center
                        } label: {
                            Label("Drop Pin at Center", systemImage: "mappin.and.ellipse")
                                .frame(maxWidth: .infinity)
                                .padding(.vertical, 10)
                                .background(Color.blue, in: RoundedRectangle(cornerRadius: 10))
                                .foregroundStyle(.white)
                                .font(.subheadline.weight(.semibold))
                        }
                        .buttonStyle(.plain)

                        VStack(alignment: .leading, spacing: 4) {
                            Text("Radius: \(Int(radius)) meters")
                                .font(.caption.weight(.semibold))
                            Slider(value: $radius, in: 50...2000, step: 25)
                                .tint(.blue)
                        }

                        Text("Coordinates: \(String(format: "%.6f", pinCoordinate.latitude)), \(String(format: "%.6f", pinCoordinate.longitude))")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    .padding()
                    .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 16))
                    .padding()
                }

                // Search bar + results overlay
                VStack(spacing: 0) {
                    HStack(spacing: 8) {
                        Image(systemName: "magnifyingglass")
                            .foregroundStyle(.secondary)
                        TextField("Search address or place", text: $searchText)
                            .submitLabel(.search)
                            .onSubmit { performSearch() }
                        if !searchText.isEmpty {
                            Button {
                                searchText = ""
                                searchResults = []
                            } label: {
                                Image(systemName: "xmark.circle.fill")
                                    .foregroundStyle(.secondary)
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .padding(10)
                    .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 12))
                    .padding(.horizontal)
                    .padding(.top, 8)

                    if !searchResults.isEmpty {
                        ScrollView {
                            VStack(spacing: 0) {
                                ForEach(Array(searchResults.enumerated()), id: \.offset) { index, item in
                                    Button {
                                        selectSearchResult(item)
                                    } label: {
                                        VStack(alignment: .leading, spacing: 2) {
                                            Text(item.name ?? "Unknown")
                                                .font(.subheadline.weight(.medium))
                                                .foregroundStyle(.primary)
                                            if let address = item.placemark.formattedAddress {
                                                Text(address)
                                                    .font(.caption)
                                                    .foregroundStyle(.secondary)
                                            }
                                        }
                                        .frame(maxWidth: .infinity, alignment: .leading)
                                        .padding(.horizontal, 14)
                                        .padding(.vertical, 10)
                                    }
                                    .buttonStyle(.plain)

                                    if index < searchResults.count - 1 {
                                        Divider().padding(.leading, 14)
                                    }
                                }
                            }
                            .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 12))
                        }
                        .frame(maxHeight: 280)
                        .padding(.horizontal)
                        .padding(.top, 4)
                    }
                }
            }
            .navigationTitle("Select Location")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Use This Location") {
                        onSave(pinCoordinate.latitude, pinCoordinate.longitude, radius)
                        dismiss()
                    }
                }
            }
        }
    }

    private func performSearch() {
        guard !searchText.trimmingCharacters(in: .whitespaces).isEmpty else { return }
        isSearching = true
        let request = MKLocalSearch.Request()
        request.naturalLanguageQuery = searchText
        request.region = region
        MKLocalSearch(request: request).start { response, _ in
            isSearching = false
            searchResults = response?.mapItems ?? []
        }
    }

    private func selectSearchResult(_ item: MKMapItem) {
        let coord = item.placemark.coordinate
        let span = MKCoordinateSpan(
            latitudeDelta: max(radius / 50000, 0.005),
            longitudeDelta: max(radius / 50000, 0.005)
        )
        let newRegion = MKCoordinateRegion(center: coord, span: span)
        region = newRegion
        position = .region(newRegion)
        pinCoordinate = coord
        searchResults = []
        searchText = item.name ?? ""
    }

    private var radiusInPoints: CGFloat {
        let metersPerDegree = 111_320.0
        let degreesForRadius = radius / metersPerDegree
        let screenWidth = UIScreen.main.bounds.width
        let degreesVisible = region.span.longitudeDelta
        guard degreesVisible > 0 else { return 50 }
        let pointsPerDegree = screenWidth / degreesVisible
        return CGFloat(degreesForRadius * pointsPerDegree * 2)
    }
}

private extension CLPlacemark {
    var formattedAddress: String? {
        [subThoroughfare, thoroughfare, locality, administrativeArea, country]
            .compactMap { $0 }
            .filter { !$0.isEmpty }
            .joined(separator: ", ")
            .nilIfEmpty
    }
}

private extension String {
    var nilIfEmpty: String? { isEmpty ? nil : self }
}


// MARK: - Map Preview (inline in form)

struct GeoFenceMapPreview: View {
    let latitude: Double
    let longitude: Double
    let radius: Double

    var body: some View {
        let center = CLLocationCoordinate2D(latitude: latitude, longitude: longitude)
        let span = MKCoordinateSpan(
            latitudeDelta: max(radius / 50000, 0.005),
            longitudeDelta: max(radius / 50000, 0.005)
        )
        Map(initialPosition: .region(MKCoordinateRegion(center: center, span: span))) {
            Annotation("", coordinate: center) {
                ZStack {
                    Circle()
                        .fill(Color.blue.opacity(0.1))
                        .stroke(Color.blue.opacity(0.4), lineWidth: 1)
                        .frame(width: 80, height: 80)
                    Image(systemName: "mappin.circle.fill")
                        .font(.title2)
                        .foregroundStyle(.red)
                }
            }
        }
        .disabled(true) // Preview only, not interactive
        .clipShape(RoundedRectangle(cornerRadius: 10))
    }
}
