# Cosmos Live-Test Infrastructure

This resource-group Bicep module provisions the Azure resources used by protected integration,
load, and managed-identity release validation. The managed-identity proof runs as a manual Azure
Container Apps Job; GitHub-hosted Actions runners only orchestrate it through workload federation.

## Resources

- One Azure Cosmos DB for NoSQL account with Session consistency, TLS 1.2 minimum, continuous
  seven-day backup, no network ACL bypass, and local key authentication disabled.
- One SQL database and separate live/load containers with partition key `/sessionId`, schema-v2
  index paths, autoscale throughput, and TTL enabled.
- One user-assigned identity attached to the proof job, with Cosmos DB Built-in Data Contributor
  and `AcrPull` access.
- One Basic Azure Container Registry with local admin access disabled and legacy registry RBAC
  permissions enabled explicitly.
- One Log Analytics workspace and Container Apps managed environment.
- One manual Container Apps Job configured to pull through the user-assigned identity.
- Optional orchestration assignments for GitHub OIDC principals: `AcrPull`, Container Registry
  Tasks Contributor, Container Apps Jobs Operator, and Log Analytics Data Reader.
- Optional data-plane assignments for GitHub OIDC or other Entra principals.
- Optional account logs and request metrics sent to an existing Log Analytics workspace.

The account defaults to public network access because GitHub-hosted runners do not have private
network reachability. Entra authentication is still mandatory. Supply firewall or virtual-network
rules where the runner topology permits them. A production application account should normally
disable public access and use a private endpoint supplied by its platform-specific infrastructure.

## Deploy

Install Azure CLI and Bicep, authenticate to the intended subscription, and confirm the active
subscription before creating resources. Use the service-principal object ID, not its application
client ID, for role-assignment parameters:

```powershell
$resourceGroup = "rg-openai-cosmos-session-live"
$accountName = "<globally-unique-account-name>"
$workflowPrincipalObjectId = "<github-oidc-service-principal-object-id>"

az account show --query "{subscription:name, subscriptionId:id, tenantId:tenantId}"

az deployment group validate `
    --resource-group $resourceGroup `
    --template-file infra/live-test/main.bicep `
  --parameters `
    cosmosAccountName=$accountName `
    managedIdentityWorkflowPrincipalIds="['$workflowPrincipalObjectId']" `
    additionalDataContributorPrincipalIds="['$workflowPrincipalObjectId']"

az deployment group create `
    --name openai-cosmos-session-live `
    --resource-group $resourceGroup `
    --template-file infra/live-test/main.bicep `
  --parameters `
    cosmosAccountName=$accountName `
    managedIdentityWorkflowPrincipalIds="['$workflowPrincipalObjectId']" `
    additionalDataContributorPrincipalIds="['$workflowPrincipalObjectId']"
```

Omit `additionalDataContributorPrincipalIds` when the OIDC principal will orchestrate only the
managed-identity proof. Pass an existing workspace resource ID through
`logAnalyticsWorkspaceResourceId` only when Cosmos account diagnostics should also be exported;
the Container Apps proof always uses the workspace created by this module.

Map deployment outputs to protected GitHub variables as follows:

| Protected variable | Deployment output or operator value |
| --- | --- |
| `AZURE_CLIENT_ID` | GitHub OIDC application client ID |
| `AZURE_TENANT_ID` | Entra tenant ID |
| `AZURE_SUBSCRIPTION_ID` | Azure subscription ID |
| `AZURE_RESOURCE_GROUP` | Resource group containing this deployment |
| `AZURE_CONTAINER_REGISTRY` | `containerRegistry` |
| `AZURE_CONTAINER_APP_JOB` | `managedIdentityJob` |
| `AZURE_LOG_ANALYTICS_WORKSPACE_ID` | `runtimeLogAnalyticsWorkspaceCustomerId` |
| `AZURE_MANAGED_IDENTITY_CLIENT_ID` | `runtimeIdentityClientId` |
| `COSMOS_ENDPOINT` | `cosmosEndpoint` |
| `COSMOS_DATABASE` | `database` |
| `COSMOS_CONTAINER` | `liveContainer` |

The `runtimeIdentityResourceId` and `runtimeIdentityPrincipalId` outputs support attachment and
RBAC auditing. Never configure `COSMOS_CONNECTION_STRING` for this workflow.

## Identity Boundary

There are two independent identity boundaries:

1. The GitHub-hosted runner exchanges its OIDC token through `azure/login`. That principal can
  build in ACR, resolve the image digest, start and inspect the preprovisioned job, and query the
  proof workspace. It does not establish managed-identity Cosmos access.
2. The Container Apps Job runs with the user-assigned identity. `AZURE_CLIENT_ID` selects that
  identity inside the container, and `COSMOS_AUTH_MODE=managed-identity` forces
  `ManagedIdentityCredential`. The job identity can pull from ACR and access Cosmos but cannot
  orchestrate the workflow.

The protected workflow builds `${GITHUB_SHA}`, resolves the ACR manifest digest, and executes
`<login-server>/openai-cosmosdb-session-mi@sha256:<digest>`. It verifies the execution image, then
queries durable Log Analytics records filtered by a per-run nonce. The test runner emits JUnit as
`COSMOS_MI_JUNIT|nonce|index|total|sha256|base64` chunks. Recovery rejects missing, conflicting,
malformed, checksum-invalid, image-mismatched, or provenance-mismatched evidence before uploading
the artifact.

Protect the `managed-identity-live` environment with required reviewers, prevent self-review and
administrator bypass, and restrict deployment branches. Do not set
`MANAGED_IDENTITY_RELEASE_ENABLED=true` based on an OIDC live run, a successful deployment, or a
mutable image tag.

## Local Validation

Use the pinned Bicep `0.46.1` binary with SHA-256
`441d3d6094513acaa8a9be0b96b754d174bc0c19064c7be04e35b87e67b66250`. Compilation must emit no
warnings or errors:

```powershell
bicep build infra/live-test/main.bicep
```

The first authoritative container check is the protected ACR build because the local development
environment does not need Docker. A successful release proof additionally requires the exact
execution to finish with `Succeeded` and checksum-valid JUnit to be recovered from Log Analytics.