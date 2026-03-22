# raft

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Zig](https://img.shields.io/badge/Zig-0.15.1+-orange.svg)](https://ziglang.org/)

A Zig library for raft

## Features

- **Minimal Dependencies**: Depends only on [spice](https://gitlab.com/devnw/zig/spice) for fork/join parallelism
- **Memory Safe**: Proper allocation patterns with no leaks
- **Cross-Platform**: Linux, macOS, Windows support
- **Well Tested**: Comprehensive unit and integration tests

## Installation

### Using Zig Package Manager

Add to your `build.zig.zon`:

```zig
.dependencies = .{
    .raft = .{
        .url = "https://gitlab.com/devnw/zig/raft/-/archive/v0.1.0/raft-v0.1.0.tar.gz",
        .hash = "...",
    },
},
```

Then in your `build.zig`:

```zig
const raft = b.dependency("raft", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("raft", raft.module("raft"));
```

## Quick Start

```zig
const std = @import("std");
const raft = @import("raft");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Create storage and transport backends.
    var store = try raft.InMemoryStorage.init(allocator);
    defer store.deinit();
    // (Use raft.FileStorage for crash-safe persistence.)

    // Configure and initialize the node.
    const config = raft.Config{ .id = 1, .peers = &.{ 2, 3 } };
    var node = try raft.RaftNode.init(allocator, config, store.storage());
    defer node.deinit();

    // Consumer loop: tick → ready → persist/send/apply → advance.
    node.tick(null);
    var rd = node.ready();
    // ... persist rd.hard_state, send rd.messages, apply rd.committed_entries ...
    node.advance(&rd);
}
```

## Requirements

- Zig 0.15.1 or later

## Building

```bash
# Build the library
make build

# Run tests
make test

# Quick development iteration
make dev

# Full CI pipeline
make ci
```

## Documentation

See the [docs](docs/) directory for detailed documentation.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Security

See [SECURITY.md](SECURITY.md) for security policy and vulnerability reporting.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
