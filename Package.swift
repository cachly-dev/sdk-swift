// swift-tools-version: 5.10
import PackageDescription

let package = Package(
    // "swift" matches the directory name so SPM can resolve products via package: "swift"
    name: "Cachly",
    platforms: [
        .macOS(.v14),
        .iOS(.v17),
    ],
    products: [
        .library(name: "Cachly", targets: ["Cachly"]),
    ],
    dependencies: [
        // RediStack – official Swift NIO Redis client (Swift Server Workgroup)
        .package(url: "https://github.com/swift-server/RediStack.git", from: "1.4.0"),
    ],
    targets: [
        .target(
            name: "Cachly",
            dependencies: [
                .product(name: "RediStack", package: "RediStack"),
            ]
        ),
        .testTarget(
            name: "CachlyTests",
            dependencies: ["Cachly"]
        ),
    ]
)

