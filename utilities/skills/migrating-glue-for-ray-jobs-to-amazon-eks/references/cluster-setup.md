# Preparing the cluster (Step 7)

Every `kubectl` command here takes `--context glue-ray-<C>` and every `helm` command
`--kube-context glue-ray-<C>` (`SKILL.md` Step 7); they are left out below for brevity.

## New cluster (only after confirmation; this is billable)

1. Fill in `templates/cluster/eksctl-automode-cluster.yaml`, then
   `eksctl create cluster -f cluster.yaml --set-kubeconfig-context=false` (so the user's current
   kubeconfig context doesn't change; `SKILL.md` Step 7 creates the job's own context).
   Ask the user for the CIDRs allowed to reach the Kubernetes API (`[OPERATOR_CIDR]`); include this
   machine's public IP (`curl -s https://checkip.amazonaws.com`), or the next steps can't reach the
   cluster. Check it several times: behind a corporate proxy or NAT pool it can change between
   requests, and then the user must give the network's whole egress range. Traffic to AWS endpoints
   can also leave through another egress than the one `checkip` sees; the `sourceIPAddress` of your
   own recent API calls in CloudTrail shows what AWS sees (`aws cloudtrail lookup-events
   --lookup-attributes AttributeKey=EventName,AttributeValue=DescribeCluster --max-results 5`). If
   `eksctl` or `kubectl` then times out reaching the API (`dial tcp <ip>:443: i/o timeout`), see
   troubleshooting `API-UNREACHABLE`. Then add an **S3 gateway endpoint** to the VPC's private route tables (recommended). Without
   it, all S3 traffic (job I/O, `working_dir`, spilling) and every ECR image layer (ECR serves layers from
   S3) goes through the NAT gateway and is billed as NAT data processing; gateway endpoints have no
   charge.
   ```bash
   bash -c '   # bash, not zsh: $RTS must split into one argument per route table
   VPC=$(aws eks describe-cluster --region <REGION> --name <C> --query cluster.resourcesVpcConfig.vpcId --output text)
   RTS=$(aws ec2 describe-route-tables --region <REGION> --filters Name=vpc-id,Values=$VPC "Name=route.nat-gateway-id,Values=nat-*" --query "RouteTables[].RouteTableId" --output text)
   aws ec2 create-vpc-endpoint --region <REGION> --vpc-id $VPC --vpc-endpoint-type Gateway --service-name com.amazonaws.<REGION>.s3 --route-table-ids $RTS'
   ```
   For an existing VPC, check that it already has one.
2. Apply `templates/cluster/ray-nodepool.yaml`:
   - reuse the built-in node role (otherwise create an EC2 access entry with `AmazonEKSAutoNodePolicy`);
   - use the cluster security group ID and private subnets;
   - set `[MAX_VCPU]` = (largest job's `NumberOfWorkers`) × 8 × (jobs that may run at once; a must-ask
     concurrency decision).
3. Apply `templates/cluster/enable-network-policy-automode.yaml`. Auto Mode doesn't enforce
   NetworkPolicies until this ConfigMap is applied.
4. **Required:** a log pipeline. The cluster template installs the CloudWatch Observability add-on, with
   an EKS Pod Identity association for its agent (`CloudWatchAgentServerPolicy`); remove that block from
   the template only if the customer ships logs with a pipeline of their own. Pod logs are deleted with
   the pods and their nodes shortly after each run, so without a log pipeline the job's output is lost.
   The driver output lands in `/aws/containerinsights/<CLUSTER>/application`, in the `ray-job-submitter`
   container stream of the submitter pod. Stream names start with the node's instance ID, so find it by
   substring:
   `aws logs describe-log-streams --log-group-name <GROUP> --query "logStreams[?contains(logStreamName,'<RAYJOB>') && contains(logStreamName,'ray-job-submitter')].logStreamName"`.
   It includes every task's and actor's output (not deduplicated), which Glue shipped per worker to
   `/aws-glue/ray/jobs/ray-worker-out-logs` and `ray-worker-err-logs`. Ray's own per-worker files under
   `/tmp/ray` are not shipped.

## Existing cluster

Check that it has:
- a supported Kubernetes version and a log pipeline (item 4 above; install the add-on the same way if no
  log pipeline runs);
- capacity with the right architecture (arm64) and size: an Auto Mode NodePool, or a managed node group;
- an IAM OIDC provider, which IRSA needs;
- on node groups, no fallback from pods to the node's IAM role: IMDSv2 required with a hop limit of 1
  (in the launch template). Auto Mode nodes need nothing.

Use `--capacity nodegroup --nodegroup <name>` for a managed node group (its nodes carry the
`eks.amazonaws.com/nodegroup` label), plus `--toleration key=value:Effect` for each taint on them. For
self-managed nodes, the nodes need the same label (`eks.amazonaws.com/nodegroup=<name>`, set through the
kubelet's `--node-labels`), or the pods stay `Pending`. If KubeRay already runs there, pass its namespace
as `--operator-namespace`; the NetworkPolicy only admits the operator from that namespace, and without
that access RayJob status never updates.

## KubeRay, pinned

First check for an existing operator, which other teams may share:

```bash
kubectl get deploy -A -l app.kubernetes.io/name=kuberay-operator \
  -o 'custom-columns=NS:.metadata.namespace,IMAGE:.spec.template.spec.containers[0].image'
```

The operator is shared by everything on the cluster, so if one exists, don't install or upgrade over it
without the user's explicit decision (interaction). Use it as is (pass its namespace as
`--operator-namespace`) only if its version is at least 1.7 and below 1.9, and tell the user when it
differs from `[KUBERAY_VERSION]`:
- 1.9 or later no longer serves `ray.io/v1alpha1`, so the Ray 2.4 autoscaler can't work with it;
- the manifests clean up with RayJob `deletionStrategy` rules (including a `jobDeploymentStatus`
  condition) as in KubeRay 1.7, with the `RayJobDeletionPolicy` feature gate on (the 1.7 Helm chart's
  default). Older operators may not support them.

If the existing operator doesn't qualify, tell the user and stop (a separate cluster would be needed).
On a cluster with no operator:

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/ && helm repo update
helm install kuberay-operator kuberay/kuberay-operator --version [KUBERAY_VERSION] \
  -n kuberay-system --create-namespace --wait
```

## IAM for the Ray pods (IRSA)

Head and worker pods get the same permissions as the Glue job role. The Glue-parity boto3 (1.26.x)
predates EKS Pod Identity, so the pods use IRSA.
- Create the role with the name the renderer printed, so it matches the ARN in the manifests, with the
  Glue role's boundary and tags: `aws iam create-role --role-name <ROLE> --assume-role-policy-document
  file://manifests/iam/trust.json --tags file://manifests/iam/role-tags.json`, plus `--permissions-boundary <ARN>` when
  `manifests/iam/permissions-boundary.txt` isn't empty. Attach each policy in
  `manifests/iam/attach-policy-arns.txt` (`aws iam attach-role-policy`), and add each other
  `manifests/iam/<name>.json` (not `trust.json` or `role-tags.json`) as an inline policy
  (`aws iam put-role-policy`).
- If the test-run locations (Step 5) are outside what the Glue role allows, add
  `test-run/manifests/s3-policy.json` as an extra inline policy for the test run, and remove it at
  cutover. To check, simulate the Glue role, one resource ARN per call (several in one call return
  placeholder results): `aws iam simulate-principal-policy --policy-source-arn <GLUE_ROLE_ARN>
  --action-names s3:PutObject s3:GetObject --resource-arns <one test-run object ARN>`. (The renderer writes that file only when the plan lists S3 locations.)
- Trust: fill in `templates/cluster/ray-pod-trust-policy-irsa.json` as `manifests/iam/trust.json`,
  listing the exact head and worker ServiceAccount names the renderer printed
  (`system:serviceaccount:<NS>:<SA>`). Find the cluster's OIDC
  provider with `aws eks describe-cluster --name <C> --query cluster.identity.oidc.issuer --output text`
  (drop the `https://`).
- The submitter ServiceAccount gets no AWS role.
