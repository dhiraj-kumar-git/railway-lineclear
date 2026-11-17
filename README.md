# Railway LineClear

Railway LineClear manages train movements for a railway division, modelled on Indian Railways. Trains report their GPS positions, staff report track incidents, and trains are diverted around blocked sections using two-phase commit, coordinated by an elected controller.

## Infrastructure

Terraform lives in infra/ and is applied in this order:

1. infra/account: S3 bucket for Terraform state, monthly cost budget
2. infra/platform: VPC, EKS, databases, Kafka and the rest of the AWS resources
3. infra/argocd: ArgoCD, which deploys everything in gitops/