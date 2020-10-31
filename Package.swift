// swift-tools-version:5.1
// The swift-tools-version declares the minimum version of Swift required to build this package.
import PackageDescription

let package = Package(
    name: "ZenPostgres",
    platforms: [
        .macOS(.v10_15)
    ],
    products: [
        .library(name: "ZenPostgres", targets: ["ZenPostgres"])
    ],
    dependencies: [
        .package(url: "https://github.com/vapor/postgres-kit.git", .branch("master"))
    ],
    targets: [
        .target(name: "ZenPostgres", dependencies: ["PostgresKit"]),
        .testTarget(name: "ZenPostgresTests", dependencies: ["ZenPostgres"]),
    ]
)

