#!/bin/zsh
# One-time AWS setup for nightly bot DB backups (Nov 1 2026).
# Creates a private, encrypted, versioned bucket with 30-day expiry, and lets the
# existing cougconnect-ses-bot IAM user PUT into it (no read/list/delete).
# Undo: aws iam delete-user-policy --user-name cougconnect-ses-bot --policy-name s3-backup-put
#       aws s3 rb s3://cougconnect-bot-backups --force   (after deleting object versions)
set -e
export AWS_PROFILE=cougconnect AWS_REGION=us-east-2
B=cougconnect-bot-backups

aws s3api create-bucket --bucket $B --region us-east-2 \
  --create-bucket-configuration LocationConstraint=us-east-2
aws s3api put-public-access-block --bucket $B --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket $B --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
aws s3api put-bucket-versioning --bucket $B --versioning-configuration Status=Enabled
aws s3api put-bucket-lifecycle-configuration --bucket $B --lifecycle-configuration \
  '{"Rules":[{"ID":"expire-30d","Filter":{"Prefix":""},"Status":"Enabled","Expiration":{"Days":30},"NoncurrentVersionExpiration":{"NoncurrentDays":30},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}'
aws iam put-user-policy --user-name cougconnect-ses-bot --policy-name s3-backup-put \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Sid":"PutBackupsOnly","Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::cougconnect-bot-backups/*"}]}'

echo "--- verify"
aws s3api get-public-access-block --bucket $B --query 'PublicAccessBlockConfiguration' --output text
aws s3api get-bucket-versioning --bucket $B --output text
aws s3api get-bucket-lifecycle-configuration --bucket $B --query 'Rules[].[ID,Status,Expiration.Days]' --output text
aws iam get-user-policy --user-name cougconnect-ses-bot --policy-name s3-backup-put --query 'PolicyDocument.Statement[].Action' --output text
echo "DONE"
