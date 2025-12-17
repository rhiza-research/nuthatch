#!/bin/bash
#
# Teardown AWS infrastructure for nuthatch integration tests.
#
# This script removes:
#   - IAM user access keys
#   - IAM user policy
#   - IAM user: nuthatch-test
#   - S3 bucket: nuthatch-test (must be empty)

set -e

BUCKET_NAME="nuthatch-test"
USER_NAME="nuthatch-test"

echo "Deleting access keys for user: $USER_NAME"
for key_id in $(aws iam list-access-keys --user-name "$USER_NAME" --query 'AccessKeyMetadata[].AccessKeyId' --output text); do
    echo "  Deleting key: $key_id"
    aws iam delete-access-key --user-name "$USER_NAME" --access-key-id "$key_id"
done

echo "Deleting user policy"
aws iam delete-user-policy --user-name "$USER_NAME" --policy-name "${USER_NAME}-s3" || true

echo "Deleting IAM user: $USER_NAME"
aws iam delete-user --user-name "$USER_NAME"

echo "Emptying and deleting S3 bucket: $BUCKET_NAME"
aws s3 rm "s3://$BUCKET_NAME" --recursive || true
aws s3 rb "s3://$BUCKET_NAME"

echo "Done!"
