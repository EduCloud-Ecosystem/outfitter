# Linux/amd64 broker SPDX inventory

This pilot starts at main `bfc3ac48acbae7fafb7893ee35a9332f848d5c15` and extends
the existing broker workflow. CI builds `bin/outfitter` once, captures its hashes
and provenance, and runs the existing Docker, recovery and consumer acceptance
scripts against that binary. Each suite checks the binary hash before and after
execution and retains a receipt binding the successful report to those bytes.

The verifier requires Linux/amd64 ELF, embedded Go module and toolchain metadata,
the expected Git revision with a clean build, and agreement with the module graph
and Go environment. It reads ELF headers and dynamic entries, resolves native
libraries on the CI runner, checks the transitive dependency closure, and records
the owning installed Debian packages and exact library file hashes. CGO behavior
continues to follow the current builder.

After all three acceptance receipts pass, CI retains the exercised executable,
an SPDX 2.3 document, provenance, receipts, and SHA-256 checksums. The SPDX binary
package identifies the exact executable hash and source commit. Its inventory
includes the compiled Go standard library, versioned external Go modules when
present, and linked native packages. Package licensing is unasserted.

Acceptance requires regression controls for changed binaries or reports, failed
or absent suites, foreign source/platform/toolchain/module metadata, unresolved
native linkage, and unowned libraries. The CI artifact must pass SPDX format and
relationship validation and checksum verification after download. Runtime Docker
services, workload images, cloud resources, and deployment have separate records.
