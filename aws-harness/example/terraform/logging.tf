# logging.tf — Linky transcript storage
#
# Linky writes each conversation's transcript directly to S3 (or local disk —
# see the runtime contract below).  Stateless pods overwrite the same key on
# every turn, so the latest PutObject is always the fullest transcript.  S3
# versioning retains prior turns for 7 days in case you ever need to audit.
#
# S3 layout (flat, one object per conversation):
#
#   s3://471112539178-us-west-2/transcripts/<conversation_id>.json.gz
#
#   Current version → fullest transcript (gzipped JSON).
#   Noncurrent versions → prior turns, auto-expired after 7 days.
#   Current version lifecycle → Standard → Standard-IA (30d) → Glacier (90d).
#
# -----------------------------------------------------------------------------
# Linky runtime contract (implemented in the Linky source, out of this repo):
#
#   Env var                    Meaning
#   -------                    -------
#   TRANSCRIPTS_LOCAL_DIR      If non-empty, write transcripts to
#                              $TRANSCRIPTS_LOCAL_DIR/<conversation_id>.json.gz
#                              instead of S3.  Format is identical to the
#                              S3 payload (gzipped JSON).
#   S3_TRANSCRIPTS_BUCKET      If TRANSCRIPTS_LOCAL_DIR is empty and this is
#                              set, write to
#                              s3://$S3_TRANSCRIPTS_BUCKET/transcripts/<id>.json.gz
#   (neither set)              Transcripts disabled — log a warning and no-op.
#
#   On every turn, the Linky handler builds the full running transcript and
#   does a single PutObject / file write.  No appending, no part-numbering.
#
# -----------------------------------------------------------------------------
# This file defines:
#   1. IAM policy — PutObject to transcripts/ prefix only
#   2. IRSA role bound to the `linky:linky` service account
#   3. Bucket versioning (latest object = fullest; prior versions retained)
#   4. Lifecycle rules:
#        a. transcripts/ current → Standard-IA at 30d → Glacier at 90d
#        b. transcripts/ noncurrent → expire at 7d
#        c. weights/ noncurrent → expire at 1d (weights are immutable mirrors
#           of upstream HF content — any prior version is garbage)

# ── IAM policy: full access to transcripts/ prefix for `aws s3 sync` ─────────
# The uploader sidecar runs `aws s3 sync` which under the hood needs:
#   • s3:ListBucket on the bucket (to enumerate existing keys)
#   • s3:GetObject on transcripts/*     (HeadObject — compare size/mtime)
#   • s3:PutObject on transcripts/*     (upload new/changed)
# ListBucket is bucket-scoped (not prefix-scoped) so we use the s3:prefix
# condition to keep the blast radius limited to the transcripts/ prefix.
resource "aws_iam_policy" "linky_s3_transcripts" {
  name = "${var.cluster_name}-linky-s3-transcripts"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListTranscriptsPrefix"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = "arn:aws:s3:::471112539178-us-west-2"
        Condition = {
          StringLike = {
            "s3:prefix" = ["transcripts/*", "transcripts/"]
          }
        }
      },
      {
        Sid      = "ReadWriteTranscriptObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "arn:aws:s3:::471112539178-us-west-2/transcripts/*"
      }
    ]
  })

  tags = local.tags
}

# ── IRSA role for the Linky pod service account ──────────────────────────────
# Mirrors the pattern used by aws_lbc_irsa / ebs_csi_irsa in eks.tf.  The SA
# name below must match whatever `serviceAccount.name` the Linky helm chart
# creates (annotate it with this role's ARN).
module "linky_s3_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name = "${var.cluster_name}-linky-s3-transcripts"

  role_policy_arns = {
    transcripts = aws_iam_policy.linky_s3_transcripts.arn
  }

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["linky:linky"]
    }
  }

  tags = local.tags
}

# ── Bucket versioning ─────────────────────────────────────────────────────────
# Enabled at the bucket level (S3 versioning is not per-prefix).  Downstream
# effect on the weights/ prefix is mitigated by the weights-noncurrent
# lifecycle rule below — weights overwrites don't accumulate old versions.
resource "aws_s3_bucket_versioning" "shared" {
  bucket = "471112539178-us-west-2"

  versioning_configuration {
    status = "Enabled"
  }
}

# ── Lifecycle rules (one configuration, multiple rules scoped by prefix) ─────
resource "aws_s3_bucket_lifecycle_configuration" "shared" {
  depends_on = [aws_s3_bucket_versioning.shared]
  bucket     = "471112539178-us-west-2"

  # Current transcripts age out into cheaper storage classes.  Transcripts are
  # write-once-per-turn, read-rarely — IA/Glacier tiering keeps the archive
  # bill near-zero.
  rule {
    id     = "transcripts-current-archive"
    status = "Enabled"

    filter {
      prefix = "transcripts/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  # Prior-turn versions retained for 7 days for audit / debugging only.  Not
  # part of the normal retrieval path.
  rule {
    id     = "transcripts-noncurrent-expire"
    status = "Enabled"

    filter {
      prefix = "transcripts/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  # Model-weight prefixes should not accumulate noncurrent versions — the
  # content is immutable upstream, so any prior version is just dead storage.
  rule {
    id     = "weights-noncurrent-expire"
    status = "Enabled"

    filter {
      prefix = "weights/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 1
    }
  }
}
