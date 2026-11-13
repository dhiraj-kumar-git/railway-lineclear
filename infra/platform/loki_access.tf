# While EMR runs, an internal Network Load Balancer carries the Flink logs from the EMR nodes
# to Loki: port 3100 is forwarded to Loki's fixed node port on the EKS nodes (the loki-node-port
# Service in gitops/cluster-config). It has no public address and exists only with enable_emr.
locals {
  loki_node_port = 31000
}

resource "aws_lb" "loki" {
  count              = var.enable_emr ? 1 : 0
  name               = "${var.project}-loki-internal"
  internal           = true
  load_balancer_type = "network"
  subnets            = module.vpc.private_subnets
}

resource "aws_lb_target_group" "loki" {
  count       = var.enable_emr ? 1 : 0
  name        = "${var.project}-loki"
  port        = local.loki_node_port
  protocol    = "TCP"
  target_type = "instance"
  vpc_id      = module.vpc.vpc_id

  health_check {
    protocol = "TCP"
  }
}

resource "aws_lb_listener" "loki" {
  count             = var.enable_emr ? 1 : 0
  load_balancer_arn = aws_lb.loki[0].arn
  port              = 3100
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.loki[0].arn
  }
}

# Every EKS node is a target, so the load balancer follows nodes as they are replaced
resource "aws_autoscaling_attachment" "loki" {
  count                  = var.enable_emr ? 1 : 0
  autoscaling_group_name = module.eks.eks_managed_node_groups_autoscaling_group_names[0]
  lb_target_group_arn    = aws_lb_target_group.loki[0].arn
}

# The load balancer keeps the sender's address, so the nodes accept the node port from inside the VPC
resource "aws_vpc_security_group_ingress_rule" "loki_node_port" {
  count             = var.enable_emr ? 1 : 0
  security_group_id = module.eks.node_security_group_id
  cidr_ipv4         = module.vpc.vpc_cidr_block
  ip_protocol       = "tcp"
  from_port         = local.loki_node_port
  to_port           = local.loki_node_port
  description       = "Loki node port, for Flink logs from EMR"
}
