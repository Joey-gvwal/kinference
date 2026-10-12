#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# launch-ec2.sh  –  Launch a c5.9xlarge (Amazon Linux 2023) in us-west-2
#                   and print a ready-to-use SSH command.
#
# Prerequisites: AWS CLI v2 configured (aws configure / env vars / IAM role)
# Usage:         bash launch-ec2.sh [--name my-build-box]
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Tunables ─────────────────────────────────────────────────────────────────
REGION="us-west-2"
INSTANCE_TYPE="c5.9xlarge"
KEY_NAME="build-box-key"
KEY_FILE="${HOME}/.ssh/${KEY_NAME}.pem"
SG_NAME="build-box-ssh-sg"
INSTANCE_NAME="${1:-build-box}"   # override with  --name <tag>  if you like
# ─────────────────────────────────────────────────────────────────────────────

echo "==> Region:        $REGION"
echo "==> Instance type: $INSTANCE_TYPE"
echo "==> Key pair:      $KEY_NAME  (saved to $KEY_FILE)"
echo "==> Security group: $SG_NAME"
echo ""

# ── 1. Resolve the latest Amazon Linux 2023 AMI ──────────────────────────────
echo "==> Looking up the latest Amazon Linux 2023 AMI …"
AMI_ID=$(aws ec2 describe-images \
  --region "$REGION" \
  --owners amazon \
  --filters \
    "Name=name,Values=al2023-ami-2023.*-x86_64" \
    "Name=architecture,Values=x86_64" \
    "Name=state,Values=available" \
  --query "sort_by(Images, &CreationDate)[-1].ImageId" \
  --output text)

echo "    AMI: $AMI_ID"

# ── 2. Create key pair (skip if the .pem file already exists) ─────────────────
if [[ -f "$KEY_FILE" ]]; then
  echo "==> Key file $KEY_FILE already exists – skipping key-pair creation."
else
  echo "==> Creating EC2 key pair '$KEY_NAME' …"

  # Delete any stale key pair in AWS with the same name (won't error if absent)
  aws ec2 delete-key-pair \
    --region "$REGION" \
    --key-name "$KEY_NAME" 2>/dev/null || true

  aws ec2 create-key-pair \
    --region "$REGION" \
    --key-name "$KEY_NAME" \
    --query "KeyMaterial" \
    --output text > "$KEY_FILE"

  chmod 400 "$KEY_FILE"
  echo "    Saved to $KEY_FILE (chmod 400)"
fi

# ── 3. Create (or reuse) a security group that allows SSH from anywhere ────────
echo "==> Setting up security group '$SG_NAME' …"

# Get the default VPC
VPC_ID=$(aws ec2 describe-vpcs \
  --region "$REGION" \
  --filters "Name=isDefault,Values=true" \
  --query "Vpcs[0].VpcId" \
  --output text)

# Check whether the SG already exists
SG_ID=$(aws ec2 describe-security-groups \
  --region "$REGION" \
  --filters \
    "Name=group-name,Values=$SG_NAME" \
    "Name=vpc-id,Values=$VPC_ID" \
  --query "SecurityGroups[0].GroupId" \
  --output text 2>/dev/null || echo "None")

if [[ "$SG_ID" == "None" || -z "$SG_ID" ]]; then
  SG_ID=$(aws ec2 create-security-group \
    --region "$REGION" \
    --group-name "$SG_NAME" \
    --description "SSH access for build box" \
    --vpc-id "$VPC_ID" \
    --query "GroupId" \
    --output text)
  echo "    Created security group $SG_ID"

  aws ec2 authorize-security-group-ingress \
    --region "$REGION" \
    --group-id "$SG_ID" \
    --protocol tcp \
    --port 22 \
    --cidr 0.0.0.0/0
  echo "    Allowed inbound SSH (port 22) from 0.0.0.0/0"
else
  echo "    Reusing existing security group $SG_ID"
fi

# ── 4. Launch the instance ────────────────────────────────────────────────────
echo ""
echo "==> Launching $INSTANCE_TYPE instance …"

INSTANCE_ID=$(aws ec2 run-instances \
  --region "$REGION" \
  --image-id "$AMI_ID" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name "$KEY_NAME" \
  --security-group-ids "$SG_ID" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${INSTANCE_NAME}}]" \
  --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":100,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
  --query "Instances[0].InstanceId" \
  --output text)

echo "    Instance ID: $INSTANCE_ID"

# ── 5. Wait for the instance to reach 'running' state ────────────────────────
echo ""
echo "==> Waiting for instance to reach 'running' state (this usually takes ~30s) …"
aws ec2 wait instance-running \
  --region "$REGION" \
  --instance-ids "$INSTANCE_ID"

# ── 6. Get the public DNS / IP ────────────────────────────────────────────────
PUBLIC_DNS=$(aws ec2 describe-instances \
  --region "$REGION" \
  --instance-ids "$INSTANCE_ID" \
  --query "Reservations[0].Instances[0].PublicDnsName" \
  --output text)

PUBLIC_IP=$(aws ec2 describe-instances \
  --region "$REGION" \
  --instance-ids "$INSTANCE_ID" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

# ── 7. Print summary ──────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo "  Instance is RUNNING"
echo "  ID:         $INSTANCE_ID"
echo "  Public IP:  $PUBLIC_IP"
echo "  Public DNS: $PUBLIC_DNS"
echo ""
echo "  SSH command:"
echo ""
echo "    ssh -i \"$KEY_FILE\" ec2-user@${PUBLIC_DNS}"
echo ""
echo "  (it may take another ~15s for sshd to start)"
echo "══════════════════════════════════════════════════════"
echo ""
echo "  To terminate the instance when you're done:"
echo ""
echo "    aws ec2 terminate-instances --region $REGION --instance-ids $INSTANCE_ID"
echo ""
