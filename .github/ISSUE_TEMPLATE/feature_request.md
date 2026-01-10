---
name: Feature Request
about: Suggest a new feature or enhancement
title: '[FEATURE] '
labels: enhancement
assignees: ''
---

## Problem Statement

A clear and concise description of the problem you're trying to solve.

Example: "I need to be able to [...] because [...]"

## Proposed Solution

Describe the solution you'd like to see implemented.

### API Changes (if applicable)

```yaml
# If this requires CRD changes, show the proposed API
apiVersion: postgres.example.com/v1alpha1
kind: PostgresCluster
spec:
  newFeature:
    enabled: true
    # ...
```

### Behavior

Describe how the feature should behave:
- What happens when enabled?
- What are the defaults?
- How does it interact with existing features?

## Alternatives Considered

Describe any alternative solutions or features you've considered.

## Use Case

Describe your use case in detail:
- What are you trying to accomplish?
- How does this fit into your workflow?
- How often do you need this?

## Additional Context

Add any other context, screenshots, or references here.

- Links to related projects/implementations
- Relevant documentation or specifications
- Prior art in other operators

## Checklist

- [ ] I have searched existing issues and discussions for similar requests
- [ ] I have clearly described the problem and proposed solution
- [ ] I have considered alternatives
- [ ] I am willing to contribute to implementing this feature (optional)
