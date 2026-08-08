# 08 — Isolation tiers for untrusted student code

*New 2026-08-08, from a sandbox/isolation scan (sources:
`outfitter-waypoint-landscape-2026-08-08.md`, Cowork project). Outfitter provisions
the compute that Cairn's autograder and Belay's runner execute student code on, so
the isolation boundary is a provisioning-time property and belongs in this
repository's specs — with Cairn and Belay as consumers of the tier, not as
independent implementers of their own boundaries.*

---

## 1. The premise this spec exists to correct

**A plain OCI container is not a security boundary against hostile code.** All
containers share one host kernel and the full ~450-call syscall surface, and the
runtime is itself attack surface. This is not theoretical: `runc` CVE-2024-21626
("Leaky Vessels") let a container's working directory resolve to the host root via
a leaked `/proc/self/fd` handle, and the November 2025 trio (CVE-2025-31133,
-52565, -52881) defeated masked-paths and, in some configurations, AppArmor/SELinux
mitigations through procfs write redirection. Add cgroup `release_agent` escapes
(CVE-2022-0492) and kernel LPEs like Dirty Pipe.

Container hardening flags — `--cap-drop ALL`, `--security-opt no-new-privileges`,
`--read-only`, `--pids-limit`, `--network none` — raise the cost of an escape
substantially and are necessary. They are not sufficient, and a platform running a
stranger's code on a shared host at classroom scale should not pretend otherwise.

## 2. The tiers

The isolation tier is a **per-assignment policy field**, not a build-time
architecture decision. This is the central design property of this spec: because
both `runsc` (gVisor) and Kata are OCI/CRI-compatible and RuntimeClass-selectable,
moving an assignment between tiers is a config change, not a rebuild — which is
the same "config diff, not code" contract the rest of the platform holds itself to.

| Tier | Mechanism | Boundary | Startup | Overhead | Use |
|---|---|---|---|---|---|
| `T-fast` | WASM/Pyodide, in-browser | none server-side | ~ms | none | Optional instant feedback. **Never authoritative** — always regraded server-side. |
| `T-standard` | **gVisor** (`runsc`), Apache-2.0 | userspace kernel; Sentry confined by seccomp to ~53 host syscalls | tens of ms | 10–50 MB; ~native CPU, 2–10× syscall-heavy | **Default** for autograde runs and tutor snippets |
| `T-strong` | **Kata Containers** (Apache-2.0), Cloud Hypervisor VMM | hardware VM boundary (EPT/VT-x) | 150–300 ms | ~100 MB+/VM | Long-lived student workspaces, instructor-supplied arbitrary Dockerfiles, GPU passthrough |
| `T-batch` | Slurm / Jetstream2 node-exclusive scheduling | site policy + scheduler | minutes | n/a | GPU coursework, burst overflow |

**`T-standard` is the default and gVisor is the recommendation**, on cost/benefit:
it removes the direct kernel attack surface without a VM, costs roughly nothing on
CPU-bound work (the common case for autograding), and is a drop-in
containerd/Kubernetes RuntimeClass. Its costs are honest and bounded: syscall- and
I/O-heavy workloads run 2–10× slower, and it does not defend against Sentry bugs or
microarchitectural side channels.

**GPU is the one place to route away rather than isolate harder.** gVisor's
`nvproxy` forwards NVIDIA ioctls to the host driver — a large, weakly-validated
attack surface that gVisor's own issue tracker treats as unfinished work. Kata with
VFIO passthrough is stronger but coarse (a whole GPU or MIG slice per VM). The
pragmatic posture for EduCloud: **keep the local sandbox tier CPU-only and route
GPU coursework to `T-batch`**, where node-exclusive scheduling and site policy are
the boundary.

## 3. Defense in depth — what layers on what

A tier is not a single mechanism. The full stack, outermost first:

- **T0 Policy.** No untrusted-code path exists without a resource ceiling, a
  wall-clock deadline, and a teardown owner. Fail closed on any missing limit.
  (This is `specs/03`'s invariant, applied to execution rather than spend.)
- **T1 Host.** Dedicated runner nodes. No co-tenancy with the control plane, no
  cloud credentials on the node, cloud metadata endpoint (169.254.169.254) blocked,
  cgroup v2, current kernel, periodic reimage.
- **T2 Kernel boundary.** §2's tier — gVisor or Kata, RuntimeClass-selected.
- **T3 Container.** Read-only rootfs, tmpfs `/tmp` with a size cap, drop ALL caps,
  `no_new_privs`, deny-by-default seccomp, cgroup v2 `memory.max` / `pids.max` /
  `cpu.max`, empty netns.
- **T4 In-sandbox runner.** Per-submission unprivileged uid, per-run reset, CPU
  **and** wall-clock limits. `nsjail` (Apache-2.0) is preferred over `isolate`
  (GPL-2.0+) for a permissively-licensed platform; if `isolate` is used, keep it
  out-of-process to avoid license entanglement.
- **T5 Data.** Reference solutions and hidden tests never enter the sandbox
  filesystem (see §4).
- **T6 Control plane.** This repository's reserve-then-reconcile ceilings and
  dead-man's switch, extended with per-student/per-class quotas and fair queueing.

## 4. The autograding-specific hard problems

These have established answers; adopting them is cheaper than rediscovering them.

**Solution and fixture leakage.** *If student code and test code share a process,
the student wins* — `inspect.getsource`, `__file__`, `sys.modules`, and tracebacks
all leak. This is nbgrader's default failure mode (it executes student notebooks
in-process on the grader host; its own issue #483, "run autograder in isolation,"
has been open since 2016) and Otter-Grader's weak point. **Requirement:** the
harness runs *outside* the sandbox and communicates over a single fd/vsock; only
student files and public fixtures are bind-mounted read-only; hidden tests run in a
**separate invocation** after student code has exited; results cross the boundary
as a schema-validated structured blob, never an echoed stdout stream, with a size
cap.

**Network exfiltration.** Default-deny at the **netns level**, not the policy
level — a namespace with loopback only cannot be misconfigured away, whereas a
policy can. In Kubernetes, a default-deny egress NetworkPolicy *plus* verification
the CNI actually enforces it: Zero-to-JupyterHub's own docs warn that NetworkPolicy
is **silently ignored** without a policy-capable CNI controller. Dependencies are
installed at image build time, never at run time.

**Fork bombs.** cgroup v2 `pids.max`. `RLIMIT_NPROC` alone is per-uid and racy.
Tear down with `cgroup.kill` for atomicity.

**Infinite loops.** CPU time **and** wall time — a `sleep()` or a blocked read
evades a CPU limit entirely. Three layers: in-sandbox rlimit, supervisor timer, and
the provider-side dead-man's switch this repository already requires.

**Attacking the grader or other students.** Unique uid per sandbox, one-shot
sandboxes, no shared tmpfs. And the Judge0 lesson, which is worth stating as a
rule: **the control plane must not be reachable from the sandbox network segment.**
Judge0's 2024 escape chain (CVE-2024-28185 symlink → overwrite the `isolate` binary
itself; CVE-2024-29021 SSRF from inside the sandbox → default Postgres credentials
→ host takeover) succeeded because sandbox-adjacent services were reachable and
credentialed.

**Reproducibility.** Pin the image **by digest, never by tag**; lockfile-pinned
dependencies; fixed seeds; no network. Record image digest, runner version, and CPU
model in the grade record. Modal's published finding that memory snapshots fragment
by CPU feature flag (e.g. `pclmulqdq`) is direct evidence that CPU features leak
into numeric behaviour — pin the instance type for numerics-sensitive courses.

**Deadline bursts.** Per-student fair queueing, not FIFO: one student's 200
resubmissions must not starve a cohort. Idempotent job keys on submission hash.
Backpressure with an honest "queued, position N" UX rather than a silent stall. And
**predictive** warm-pool scaling — Cairn knows every deadline in advance, so
pre-warming is a calendar lookup, not a reactive autoscaler.

## 5. Cold start — the UX problem, and the published techniques

"Student clicks Run, waits for a container" is the tier's real usability
constraint. Four techniques, in order of leverage:

1. **Warm pool per language image**, sized off the assignment calendar.
2. **Snapshot after interpreter and library import, restore copy-on-write.** Modal
   publishes the strongest numbers here using gVisor's own checkpoint/restore:
   `import torch` falls from ~5 s to **1.05 s p50 / 0.69 s p0**, converting ~26,000
   syscalls into roughly one lazily-paged file load. Fly Machines' suspend/resume
   (memory snapshotted to local NVMe) is the same idea for persistent workspaces.
3. **Lazy, FUSE-backed image filesystem** so image pull is off the critical path.
4. **Dedupe identical submission hashes** — at a deadline, many resubmissions are
   byte-identical to a previous attempt.

## 6. What to adopt, and what to only read

- **Adopt as a component:** **Piston** (MIT, `isolate`-in-Docker, 70+ languages,
  network off by default, 3 s CPU+wall defaults) for the stateless multi-language
  snippet path. Permissive license, small surface.
- **Adopt the format, never the execution model:** **nbgrader** (BSD-3) — cell
  metadata and hidden-test conventions are good; in-process execution is not.
- **Adopt the workflow, harden the runtime:** **Otter-Grader** (BSD-3).
- **Reference only, on licence grounds:** **Judge0** (GPL-3.0, ships as a
  *privileged* container), **DMOJ judge-server** (AGPL-3.0, technically excellent),
  **CMS** (AGPL-3.0).
- **Skip:** **Daytona** — went closed-source June 2026, archived at v0.190.0 with
  no further security patches. A direct cautionary case for this platform's
  no-proprietary-default rule.

## 7. Where each consumer stands today

- **Cairn** already implements T3 well — its container runner sets `--cap-drop
  ALL`, `--security-opt no-new-privileges`, `--read-only`, `--pids-limit`,
  `--memory`/`--memory-swap`, `--cpus`, and fails safe to `--network none`. It is
  missing **T2 only**, and because the runner shells out to a runtime CLI and
  already carries an `ExtraArgs` seam, adding gVisor is close to a flag plus a
  `cairn doctor` check — not a rewrite.
- **Belay** is the real gap, and its own docs already say so ("the containerized
  runner is the roadmap convergence point"). Its current runner is an out-of-process
  child with `setrlimit` and socket monkeypatching, which is bypassable in one line
  via `ctypes`, `subprocess`, or a raw `socket()` call. It needs T2+T3+T4, and
  moving it from "no boundary" to "two boundaries" is the single highest-value
  security change available anywhere in the platform.
