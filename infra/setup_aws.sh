#!/usr/bin/env bash
# One-time AWS setup. Run with an ADMIN IAM user/SSO profile (never the root user):
#   AWS_PROFILE=admin ./infra/setup_aws.sh <bucket-name> [region]
# Creates:
#   * a private, encrypted S3 bucket
#   * IAM user  taxi-pipeline           -> used by ingest, Spark and Polaris (raw/ + warehouse/ only)
#   * IAM role  taxi-snowflake-read     -> assumed by Snowflake (read warehouse/ only)
# The script is safe to re-run: "already exists" errors are ignored.
set -euo pipefail

BUCKET="${1:?usage: setup_aws.sh <bucket-name> [region]}"
REGION="${2:-us-east-1}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
HERE="$(cd "$(dirname "$0")" && pwd)"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT

fill() { sed -e "s/YOUR_BUCKET/$BUCKET/g" -e "s/YOUR_REGION/$REGION/g" -e "s/YOUR_ACCOUNT_ID/$ACCOUNT_ID/g" "$1" > "$2"; }

echo "== bucket s3://$BUCKET ($REGION)"
if [ "$REGION" = "us-east-1" ]; then
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null || echo "   (exists)"
else
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION" 2>/dev/null || echo "   (exists)"
fi
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket "$BUCKET" --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

echo "== IAM user taxi-pipeline"
aws iam create-user --user-name taxi-pipeline >/dev/null 2>&1 || echo "   (exists)"
fill "$HERE/iam/taxi-pipeline-policy.json" "$TMP/p.json"
aws iam put-user-policy --user-name taxi-pipeline --policy-name taxi-pipeline-s3 --policy-document "file://$TMP/p.json"
fill "$HERE/iam/taxi-pipeline-glue-policy.json" "$TMP/g.json"
aws iam put-user-policy --user-name taxi-pipeline --policy-name taxi-pipeline-glue --policy-document "file://$TMP/g.json"

echo "== IAM role taxi-snowflake-read (trust policy is a placeholder until Snowflake gives you its ARN)"
cat > "$TMP/trust.json" <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::$ACCOUNT_ID:root"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"placeholder"}}}]}
EOF
aws iam create-role --role-name taxi-snowflake-read --assume-role-policy-document "file://$TMP/trust.json" >/dev/null 2>&1 || echo "   (exists)"
fill "$HERE/iam/snowflake-read-policy.json" "$TMP/s.json"
aws iam put-role-policy --role-name taxi-snowflake-read --policy-name snowflake-read-warehouse --policy-document "file://$TMP/s.json"

cat <<EOF

Done.
  Snowflake role ARN : arn:aws:iam::$ACCOUNT_ID:role/taxi-snowflake-read
Next:
  1. Create an access key for the pipeline user (store it in .env, never in git):
       aws iam create-access-key --user-name taxi-pipeline
  2. Put the role ARN above in .env as POLARIS_S3_ROLE_ARN and in snowflake/01_setup.sql.
EOF
