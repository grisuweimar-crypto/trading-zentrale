# QM-B Current Investability Gap Integration

This package integrates the durable project-membership snapshot with the separately versioned internal project-restriction policy at an explicit `as_of` time.

The policy is not back-projected. Its evidence becomes usable only from the policy Git observation time onward.

For the current 207 stable membership instruments, the internal project-restriction dimension can be resolved to `CLEAR`. The remaining unresolved dimensions are therefore:

- listing state
- market tradability
- execution channel

All 207 instruments remain `project_investability_status = UNKNOWN` until those independent evidence dimensions are resolved. No strict-universe promotion is performed.
