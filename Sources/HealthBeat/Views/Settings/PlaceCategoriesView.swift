import SwiftUI

struct PlaceCategoriesView: View {
    @State private var categories: [PlaceCategory] = PlaceCategory.loadAll()
    @State private var showAddSheet = false
    @State private var editingCategory: PlaceCategory?

    var body: some View {
        List {
            Section {
                ForEach(categories) { cat in
                    Button {
                        editingCategory = cat
                    } label: {
                        Label(cat.name, systemImage: cat.systemImage)
                            .foregroundStyle(.primary)
                    }
                }
                .onDelete(perform: deleteCategories)
                .onMove(perform: moveCategories)

                Button {
                    showAddSheet = true
                } label: {
                    Label("Add Category", systemImage: "plus.circle.fill")
                        .foregroundStyle(.blue)
                }
            } header: {
                Text("Place Categories")
            } footer: {
                Text("Categories describe the type of each geofenced place. They are logged with check-in events to the database.")
                    .font(.caption)
            }

            Section {
                Button("Reset to Defaults") {
                    // Soft-delete all existing, then add defaults
                    var allCategories = PlaceCategory.loadAllIncludingDeleted()
                    let now = Date()
                    for i in allCategories.indices where !allCategories[i].isDeleted {
                        allCategories[i].isDeleted = true
                        allCategories[i].updatedAt = now
                    }
                    allCategories.append(contentsOf: PlaceCategory.defaults)
                    PlaceCategory.saveAll(allCategories)
                    categories = PlaceCategory.loadAll()
                }
                .foregroundStyle(.red)
            }
        }
        .navigationTitle("Place Categories")
        .navigationBarTitleDisplayMode(.inline)
        .onReceive(NotificationCenter.default.publisher(for: .iCloudSettingsDidChange)) { _ in
            categories = PlaceCategory.loadAll()
        }
        .onReceive(NotificationCenter.default.publisher(for: .geofencesDidSync)) { _ in
            categories = PlaceCategory.loadAll()
        }
        .toolbar {
            ToolbarItem(placement: .topBarTrailing) {
                EditButton()
            }
        }
        .sheet(isPresented: $showAddSheet) {
            PlaceCategoryEditSheet(mode: .add) { newCat in
                var allCategories = PlaceCategory.loadAllIncludingDeleted()
                allCategories.append(newCat)
                PlaceCategory.saveAll(allCategories)
                categories = PlaceCategory.loadAll()
            }
        }
        .sheet(item: $editingCategory) { cat in
            PlaceCategoryEditSheet(mode: .edit(cat)) { updated in
                var allCategories = PlaceCategory.loadAllIncludingDeleted()
                if let idx = allCategories.firstIndex(where: { $0.id == updated.id }) {
                    var stamped = updated
                    stamped.updatedAt = Date()
                    allCategories[idx] = stamped
                    PlaceCategory.saveAll(allCategories)
                    categories = PlaceCategory.loadAll()
                }
            }
        }
    }

    private func deleteCategories(at offsets: IndexSet) {
        // Soft-delete: mark as deleted and stamp updatedAt
        var allCategories = PlaceCategory.loadAllIncludingDeleted()
        for offset in offsets {
            let catToDelete = categories[offset]
            if let idx = allCategories.firstIndex(where: { $0.id == catToDelete.id }) {
                allCategories[idx].isDeleted = true
                allCategories[idx].updatedAt = Date()
            }
        }
        PlaceCategory.saveAll(allCategories)
        categories = PlaceCategory.loadAll()
    }

    private func moveCategories(from source: IndexSet, to destination: Int) {
        categories.move(fromOffsets: source, toOffset: destination)
        // Rebuild full list: replace non-deleted entries with reordered visible ones, stamp updatedAt
        var allCategories = PlaceCategory.loadAllIncludingDeleted().filter { $0.isDeleted }
        let now = Date()
        var reordered = categories
        for i in reordered.indices {
            reordered[i].updatedAt = now
        }
        allCategories.append(contentsOf: reordered)
        PlaceCategory.saveAll(allCategories)
    }
}

// MARK: - Add / Edit sheet

private enum PlaceCategoryEditMode: Identifiable {
    case add
    case edit(PlaceCategory)

    var id: String {
        switch self {
        case .add: return "add"
        case .edit(let cat): return cat.id.uuidString
        }
    }
}

private struct PlaceCategoryEditSheet: View {
    let mode: PlaceCategoryEditMode
    let onSave: (PlaceCategory) -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var name: String = ""
    @State private var selectedIcon: String = "mappin.circle.fill"

    private static let iconOptions: [(label: String, icon: String)] = [
        ("House", "house.fill"),
        ("Building", "building.2.fill"),
        ("Cart", "cart.fill"),
        ("Dumbbell", "dumbbell.fill"),
        ("Graduation Cap", "graduationcap.fill"),
        ("Fork & Knife", "fork.knife"),
        ("Medical", "cross.case.fill"),
        ("Leaf", "leaf.fill"),
        ("Airplane", "airplane"),
        ("Car", "car.fill"),
        ("Train", "tram.fill"),
        ("Coffee", "cup.and.saucer.fill"),
        ("Book", "book.fill"),
        ("Music", "music.note.house.fill"),
        ("Heart", "heart.fill"),
        ("Star", "star.fill"),
        ("Pin", "mappin.circle.fill"),
        ("Flag", "flag.fill"),
    ]

    var body: some View {
        NavigationStack {
            Form {
                Section("Name") {
                    TextField("Category name", text: $name)
                }

                Section("Icon") {
                    LazyVGrid(columns: Array(repeating: GridItem(.flexible()), count: 6), spacing: 16) {
                        ForEach(Self.iconOptions, id: \.icon) { option in
                            Button {
                                selectedIcon = option.icon
                            } label: {
                                Image(systemName: option.icon)
                                    .font(.title3)
                                    .frame(width: 44, height: 44)
                                    .background(
                                        selectedIcon == option.icon
                                            ? Color.blue.opacity(0.15)
                                            : Color.clear,
                                        in: RoundedRectangle(cornerRadius: 10)
                                    )
                                    .foregroundStyle(selectedIcon == option.icon ? .blue : .primary)
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .padding(.vertical, 4)
                }
            }
            .navigationTitle(isEditing ? "Edit Category" : "New Category")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") { save() }
                        .disabled(name.trimmingCharacters(in: .whitespaces).isEmpty)
                }
            }
            .onAppear { loadExisting() }
        }
    }

    private var isEditing: Bool {
        if case .edit = mode { return true }
        return false
    }

    private func loadExisting() {
        guard case .edit(let cat) = mode else { return }
        name = cat.name
        selectedIcon = cat.systemImage
    }

    private func save() {
        let trimmed = name.trimmingCharacters(in: .whitespaces)
        guard !trimmed.isEmpty else { return }

        switch mode {
        case .add:
            let cat = PlaceCategory(name: trimmed, systemImage: selectedIcon)
            onSave(cat)
        case .edit(let existing):
            var updated = existing
            updated.name = trimmed
            updated.systemImage = selectedIcon
            onSave(updated)
        }
        dismiss()
    }
}
