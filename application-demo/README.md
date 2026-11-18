# LineClear Control Office: application demo

A small web console that shows Railway LineClear working, in particular its two-phase commit (2PC) diversions, in a way a first-time viewer can follow. It runs on a laptop and talks to the live system on AWS through the same public interfaces any client uses:

- the four REST APIs on the public load balancer (`/api/trains`, `/api/positions`, `/api/incidents`, `/api/schedule`)
- the Passenger Notification Lambda's HTTPS function URL
- `kubectl get hpa` for the two autoscalers

It is not a ninth microservice and is not deployed to the cluster. The eight services, the Terraform and the GitOps configuration are exactly as tagged `v1.0.0` on `main`.

## Who uses which view

| View | Actor | What it does | Access |
| --- | --- | --- | --- |
| 1 Field staff | Track inspector or loco pilot | Reports an incident, with ready-made scenarios for COMMIT and ABORT | Public: `POST /api/incidents` |
| 2 Control office | Station master or section controller | Track diagram, duty controller and term, the coordinator's log with each transaction's two phases, incidents | Reads are public; resolve, the fault switch and freeing the track are staff actions |
| 3 Passenger | Passenger | The train's diversion notice and the station bulletin | Public: the Lambda function URL over HTTPS |
| 4 Trains and scaling | Operations | Latest GPS positions and the two autoscalers | Public, plus `kubectl get hpa` |

Staff actions go through `control-office.py`, which reads the staff key from AWS Secrets Manager into memory and adds it to the staff calls. The key is never sent to the browser or printed, and a key sent by the browser is not passed on.

## The two-phase commit timeline

Every diversion is drawn as one timeline, read from `GET /api/schedule/diversions/{txn}` and `GET /api/incidents/{id}`:

1. Incident saved by Incident Manager (REST, RDS)
2. Announced to the duty controller (Kafka `incident.events`)
3. The duty controller starts the transaction in its term, after looking up the route in Train Registry (REST)
4. Phase 1: PREPARING logged, then `Prepare` to Track Availability and Incident Manager (gRPC), with each vote and reason
5. Decision COMMIT or ABORT, logged with the revised times, then published (Kafka `coordination.events`)
6. Phase 2: `Commit` or `Abort` delivered and acknowledged (gRPC); a long gap here shows a hand-over and recovery
7. Passengers told (SQS, Lambda, DynamoDB, S3), or, after an ABORT, nothing changed anywhere

Each step carries the requirement it shows: (b) communication and serverless, (c) autoscaling, (f) two-phase commit, (g) storage.

## Run it

Requirements: Python 3.10 or later (standard library only). For the live system: `kubectl` pointed at the `lineclear` cluster and the AWS CLI signed in to the project account. The Grafana and ArgoCD links open through the usual port-forwards on 3000 and 8443.

Against the live system on AWS, from the repository root:

```bash
python3 application-demo/control-office.py remote
```

Without AWS, everything simulated in the same process:

```bash
python3 application-demo/control-office.py stub
```

Then open http://localhost:8090. The terminal prints every call the page makes and the service that answered. Stop with Ctrl+C.

Options: `--port`, `--target http://<load balancer>` if kubectl cannot find it, `--passenger-url https://<function URL>` if the AWS CLI cannot.

## Demo walkthrough

Each action in the page is followed by its proof outside the page.

| # | In the page | What it shows | Requirement | Proof outside the page |
| --- | --- | --- | --- | --- |
| 1 | Field staff: scenario A | Both participants vote YES; COMMIT; revised times; passengers told | (f) | Grafana Railway operations; Loki filtered by the transaction ID |
| 2 | Field staff: scenario B | 12002 reserves L2, then 12951 asks for it; Track Availability votes NO; ABORT; nothing changes | (f) | Railway operations votes panel with a NO; Loki for both transactions |
| 3 | Control office: freeze, then scenario A | The leader logs COMMIT and freezes; the other replica wins a higher term, recovers the transaction and finishes phase 2 | (f) | Term change on Railway operations; the `NoDutyController` alert if the gap passes 10 s |
| 4 | Any timeline: the transaction ID | One diversion over REST, Kafka, gRPC and SQS | (b) | Loki `{app=~".+"} \| json \| txn_id="T-..."` |
| 5 | Passenger | Notice from DynamoDB and bulletin from S3, served by the Lambda | (b), (g) | AWS console: DynamoDB `lineclear-passenger-notices`, S3 `lineclear-bulletins-...`, the SQS queue |
| 6 | Trains and scaling, while k6 runs in the terminal | Pods added by both autoscalers, then removed | (c) | `k6 run` output; Grafana Cluster health |
| 7 | Control office: free the track | Holds released and incidents resolved before the next run | | The trail in this terminal |
