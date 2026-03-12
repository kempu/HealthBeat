import SwiftUI

struct BrandFooter: View {
    var body: some View {
        Section {
            Link(destination: URL(string: "https://klemens.ee/healthbeat")!) {
                Text("by Klemens.ee")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, alignment: .center)
            }
            .listRowBackground(Color.clear)
        }
    }
}
