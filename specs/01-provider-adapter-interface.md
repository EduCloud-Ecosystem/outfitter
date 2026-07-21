# 01 — Provider adapter interface

The plugin seam. Every compute backend implements one interface; the broker knows
only this interface.

## Contract (Go, illustrative — refine in code)
```go
type Workload struct {
    Image     string            // or a provider-native spec ref
    Resources ResourceRequest   // cpu, mem, gpu, disk
    Env       map[string]string
    Labels    Attribution       // course, assignment, student (opaque IDs)
}

type Placement struct {
    ID       string
    Provider string
    Handle   string            // provider-native id
    Endpoint string            // how the requester reaches the workload
    State    State             // Pending|Running|Succeeded|Failed|Terminated
}

type Adapter interface {
    Name() string
    Estimate(ctx, Workload, Lifetime) (CostEstimate, error) // no side effects
    Provision(ctx, Workload, Lifetime) (Placement, error)
    Status(ctx, handle string) (Placement, error)
    Usage(ctx, handle string) (UsageSample, error)          // for cost + idle
    Teardown(ctx, handle string) error                       // idempotent
    Capabilities() Capabilities                               // gpu?, persistence?, max lifetime
}
```

## Rules
- **Idempotent teardown.** Calling `Teardown` on an already-gone handle is success.
- **No hidden spend.** `Provision` must accept a `Lifetime`; a provider that can't
  self-expire must be reapable via `Teardown` by the reaper.
- **`Estimate` is pure.** No provisioning, no charges.
- **Capabilities are honest.** The broker filters providers by declared caps.

## Conformance harness
A table-driven suite every adapter runs against: provision → status reaches
Running → usage sample returns → teardown → status reaches Terminated → teardown
again succeeds. `docker` is the reference implementation and the baseline the suite
is validated on.
