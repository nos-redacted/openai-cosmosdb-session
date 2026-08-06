targetScope = 'resourceGroup'

@description('Azure region for the Cosmos DB account and runtime identity.')
param location string = resourceGroup().location

@description('Globally unique Azure Cosmos DB account name.')
@minLength(3)
@maxLength(44)
param cosmosAccountName string

@description('Cosmos DB SQL database name.')
param databaseName string = 'agents'

@description('Dedicated container used by live integration tests.')
param liveContainerName string = 'agent_sessions'

@description('Dedicated container used by load validation.')
param loadContainerName string = 'agent_sessions_load'

@description('Default TTL for the live container. -1 enables per-item TTL without expiring every item.')
param liveContainerDefaultTtl int = -1

@description('Default TTL for the load container, in seconds.')
@minValue(1)
param loadContainerDefaultTtl int = 86400

@description('Maximum autoscale throughput for the live container.')
@minValue(1000)
param liveContainerMaxThroughput int = 4000

@description('Maximum autoscale throughput for the load container.')
@minValue(1000)
param loadContainerMaxThroughput int = 10000

@description('Name of the user-assigned identity attached to the managed-identity proof job.')
param runtimeIdentityName string = 'openai-cosmos-session-runtime'

@description('Globally unique Azure Container Registry name used for exact-commit test images.')
@minLength(5)
@maxLength(50)
param containerRegistryName string = 'ocsession${uniqueString(subscription().id, resourceGroup().id)}'

@description('Name of the Log Analytics workspace that stores Container Apps Job evidence.')
param runtimeLogAnalyticsWorkspaceName string = 'openai-cosmos-session-runtime-logs'

@description('Name of the Container Apps managed environment for managed-identity proof.')
param containerAppsEnvironmentName string = 'openai-cosmos-session-env'

@description('Name of the preprovisioned manual Container Apps Job.')
param managedIdentityJobName string = 'openai-cosmos-session-mi'

@description('Entra object IDs allowed to build proof images, start the job, and recover its logs, such as the GitHub OIDC application service principal.')
param managedIdentityWorkflowPrincipalIds string[] = []

@description('Additional Entra object IDs that require Cosmos DB Built-in Data Contributor access, such as the GitHub OIDC application service principal.')
param additionalDataContributorPrincipalIds string[] = []

@description('Whether the Cosmos DB data endpoint accepts public-network traffic.')
@allowed([
  'Enabled'
  'Disabled'
])
param publicNetworkAccess string = 'Enabled'

@description('Optional public IPv4 addresses or CIDR ranges allowed by the Cosmos DB firewall.')
param allowedIpRanges string[] = []

@description('Optional subnet resource IDs with Microsoft.AzureCosmosDB service endpoints.')
param virtualNetworkSubnetIds string[] = []

@description('Optional Log Analytics workspace resource ID. Leave empty to omit diagnostic settings.')
param logAnalyticsWorkspaceResourceId string = ''

@description('Tags applied to the provisioned resources.')
param tags object = {}

var dataContributorRoleDefinitionName = '00000000-0000-0000-0000-000000000002'
var acrPullRoleDefinitionId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '7f951dda-4ed3-4680-a7ca-43fe172d538d'
)
var acrTasksContributorRoleDefinitionId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'fb382eab-e894-4461-af04-94435c366c3f'
)
var containerAppsJobsOperatorRoleDefinitionId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'b9a307c4-5aa3-4b52-ba60-2b17c136cd7b'
)
var logAnalyticsDataReaderRoleDefinitionId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '3b03c2da-16b3-4a49-8834-0f8130efdd3b'
)
var requiredIndexingPolicy = {
  automatic: true
  indexingMode: 'consistent'
  includedPaths: [
    {
      path: '/seq/?'
    }
    {
      path: '/type/?'
    }
    {
      path: '/incarnation/?'
    }
  ]
  excludedPaths: [
    {
      path: '/*'
    }
  ]
}

resource cosmosAccount 'Microsoft.DocumentDB/databaseAccounts@2024-11-15' = {
  name: cosmosAccountName
  location: location
  kind: 'GlobalDocumentDB'
  tags: tags
  properties: {
    databaseAccountOfferType: 'Standard'
    locations: [
      {
        locationName: location
        failoverPriority: 0
        isZoneRedundant: false
      }
    ]
    consistencyPolicy: {
      defaultConsistencyLevel: 'Session'
    }
    backupPolicy: {
      type: 'Continuous'
      continuousModeProperties: {
        tier: 'Continuous7Days'
      }
    }
    disableLocalAuth: true
    disableKeyBasedMetadataWriteAccess: true
    enableAutomaticFailover: false
    enableMultipleWriteLocations: false
    minimalTlsVersion: 'Tls12'
    publicNetworkAccess: publicNetworkAccess
    ipRules: [for ipRange in allowedIpRanges: {
      ipAddressOrRange: ipRange
    }]
    isVirtualNetworkFilterEnabled: length(virtualNetworkSubnetIds) > 0
    virtualNetworkRules: [for subnetId in virtualNetworkSubnetIds: {
      id: subnetId
      ignoreMissingVNetServiceEndpoint: false
    }]
    networkAclBypass: 'None'
    networkAclBypassResourceIds: []
  }
}

resource sqlDatabase 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2024-11-15' = {
  name: databaseName
  parent: cosmosAccount
  properties: {
    resource: {
      id: databaseName
    }
    options: {}
  }
}

resource liveContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-11-15' = {
  name: liveContainerName
  parent: sqlDatabase
  properties: {
    resource: {
      id: liveContainerName
      partitionKey: {
        paths: [
          '/sessionId'
        ]
        kind: 'Hash'
        version: 2
      }
      indexingPolicy: requiredIndexingPolicy
      defaultTtl: liveContainerDefaultTtl
    }
    options: {
      autoscaleSettings: {
        maxThroughput: liveContainerMaxThroughput
      }
    }
  }
}

resource loadContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-11-15' = {
  name: loadContainerName
  parent: sqlDatabase
  properties: {
    resource: {
      id: loadContainerName
      partitionKey: {
        paths: [
          '/sessionId'
        ]
        kind: 'Hash'
        version: 2
      }
      indexingPolicy: requiredIndexingPolicy
      defaultTtl: loadContainerDefaultTtl
    }
    options: {
      autoscaleSettings: {
        maxThroughput: loadContainerMaxThroughput
      }
    }
  }
}

resource runtimeIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: runtimeIdentityName
  location: location
  tags: tags
}

resource containerRegistry 'Microsoft.ContainerRegistry/registries@2025-11-01' = {
  name: containerRegistryName
  location: location
  tags: tags
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false
    anonymousPullEnabled: false
    dataEndpointEnabled: false
    networkRuleBypassOptions: 'AzureServices'
    publicNetworkAccess: 'Enabled'
    roleAssignmentMode: 'LegacyRegistryPermissions'
    zoneRedundancy: 'Disabled'
  }
}

resource runtimeAcrPullAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(containerRegistry.id, runtimeIdentity.id, acrPullRoleDefinitionId)
  scope: containerRegistry
  properties: {
    principalId: runtimeIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: acrPullRoleDefinitionId
  }
}

resource runtimeLogs 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: runtimeLogAnalyticsWorkspaceName
  location: location
  tags: tags
  properties: {
    features: {
      enableLogAccessUsingOnlyResourcePermissions: true
    }
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
    retentionInDays: 30
    sku: {
      name: 'PerGB2018'
    }
  }
}

resource containerAppsEnvironment 'Microsoft.App/managedEnvironments@2025-01-01' = {
  name: containerAppsEnvironmentName
  location: location
  tags: tags
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: runtimeLogs.properties.customerId
        sharedKey: runtimeLogs.listKeys().primarySharedKey
      }
    }
    zoneRedundant: false
  }
}

resource managedIdentityJob 'Microsoft.App/jobs@2025-01-01' = {
  name: managedIdentityJobName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${runtimeIdentity.id}': {}
    }
  }
  properties: {
    environmentId: containerAppsEnvironment.id
    configuration: {
      identitySettings: [
        {
          identity: runtimeIdentity.id
          lifecycle: 'All'
        }
      ]
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: [
        {
          identity: runtimeIdentity.id
          server: containerRegistry.properties.loginServer
        }
      ]
      replicaRetryLimit: 0
      replicaTimeout: 1800
      triggerType: 'Manual'
    }
    template: {
      containers: [
        {
          name: 'managed-identity-tests'
          image: 'mcr.microsoft.com/azurelinux/base/core:3.0'
          command: [
            'python'
          ]
          args: [
            '-m'
            'scripts.run_managed_identity_tests'
          ]
          env: []
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
        }
      ]
    }
  }
  dependsOn: [
    runtimeAcrPullAssignment
  ]
}

resource workflowAcrPullAssignments 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for principalId in managedIdentityWorkflowPrincipalIds: {
  name: guid(containerRegistry.id, principalId, acrPullRoleDefinitionId)
  scope: containerRegistry
  properties: {
    principalId: principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: acrPullRoleDefinitionId
  }
}]

resource workflowAcrTaskAssignments 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for principalId in managedIdentityWorkflowPrincipalIds: {
  name: guid(containerRegistry.id, principalId, acrTasksContributorRoleDefinitionId)
  scope: containerRegistry
  properties: {
    principalId: principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: acrTasksContributorRoleDefinitionId
  }
}]

resource workflowJobOperatorAssignments 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for principalId in managedIdentityWorkflowPrincipalIds: {
  name: guid(managedIdentityJob.id, principalId, containerAppsJobsOperatorRoleDefinitionId)
  scope: managedIdentityJob
  properties: {
    principalId: principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: containerAppsJobsOperatorRoleDefinitionId
  }
}]

resource workflowLogReaderAssignments 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for principalId in managedIdentityWorkflowPrincipalIds: {
  name: guid(runtimeLogs.id, principalId, logAnalyticsDataReaderRoleDefinitionId)
  scope: runtimeLogs
  properties: {
    principalId: principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: logAnalyticsDataReaderRoleDefinitionId
  }
}]

resource dataContributorRoleDefinition 'Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions@2024-11-15' existing = {
  name: dataContributorRoleDefinitionName
  parent: cosmosAccount
}

resource runtimeDataContributorAssignment 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-11-15' = {
  name: guid(cosmosAccount.id, runtimeIdentity.id, dataContributorRoleDefinition.id)
  parent: cosmosAccount
  properties: {
    principalId: runtimeIdentity.properties.principalId
    roleDefinitionId: dataContributorRoleDefinition.id
    scope: cosmosAccount.id
  }
}

resource additionalDataContributorAssignments 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-11-15' = [for principalId in additionalDataContributorPrincipalIds: {
  name: guid(cosmosAccount.id, principalId, dataContributorRoleDefinition.id)
  parent: cosmosAccount
  properties: {
    principalId: principalId
    roleDefinitionId: dataContributorRoleDefinition.id
    scope: cosmosAccount.id
  }
}]

resource cosmosDiagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (!empty(logAnalyticsWorkspaceResourceId)) {
  name: 'cosmos-session-diagnostics'
  scope: cosmosAccount
  properties: {
    workspaceId: logAnalyticsWorkspaceResourceId
    logAnalyticsDestinationType: 'Dedicated'
    logs: [
      {
        categoryGroup: 'allLogs'
        enabled: true
      }
    ]
    metrics: [
      {
        category: 'Requests'
        enabled: true
      }
    ]
  }
}

output cosmosEndpoint string = cosmosAccount.properties.documentEndpoint
output database string = sqlDatabase.name
output liveContainer string = liveContainer.name
output loadContainer string = loadContainer.name
output runtimeIdentityResourceId string = runtimeIdentity.id
output runtimeIdentityClientId string = runtimeIdentity.properties.clientId
output runtimeIdentityPrincipalId string = runtimeIdentity.properties.principalId
output containerRegistry string = containerRegistry.name
output containerRegistryLoginServer string = containerRegistry.properties.loginServer
output containerAppsEnvironment string = containerAppsEnvironment.name
output managedIdentityJob string = managedIdentityJob.name
output runtimeLogAnalyticsWorkspace string = runtimeLogs.name
output runtimeLogAnalyticsWorkspaceCustomerId string = runtimeLogs.properties.customerId
output diagnosticsEnabled bool = !empty(logAnalyticsWorkspaceResourceId)