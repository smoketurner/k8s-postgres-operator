---
name: Bug Report
about: Report a bug to help us improve
title: '[BUG] '
labels: bug
assignees: ''
---

## Describe the Bug

A clear and concise description of what the bug is.

## To Reproduce

Steps to reproduce the behavior:

1. Create a PostgresCluster with '...'
2. Apply configuration '...'
3. Wait for '...'
4. See error

## Expected Behavior

A clear and concise description of what you expected to happen.

## Actual Behavior

What actually happened, including any error messages.

## PostgresCluster Manifest

```yaml
# Paste your PostgresCluster manifest here (remove sensitive data)
apiVersion: postgres.example.com/v1alpha1
kind: PostgresCluster
metadata:
  name: my-cluster
spec:
  version: "16"
  replicas: 3
  storage:
    size: 10Gi
```

## Operator Logs

```
# Paste relevant operator logs here
kubectl logs -n postgres-operator-system deploy/postgres-operator
```

## Environment

- **Kubernetes version**: (output of `kubectl version`)
- **Operator version**: (e.g., v0.1.0 or commit SHA)
- **Cloud provider/Platform**: (e.g., AWS EKS, GKE, on-premise)
- **Kubernetes distribution**: (e.g., EKS, GKE, AKS, OpenShift, k3s)

## Additional Context

Add any other context about the problem here, such as:
- Screenshots
- Related issues
- Workarounds attempted

## Checklist

- [ ] I have searched existing issues to ensure this is not a duplicate
- [ ] I have included the PostgresCluster manifest (with sensitive data removed)
- [ ] I have included relevant operator logs
- [ ] I have provided environment details
