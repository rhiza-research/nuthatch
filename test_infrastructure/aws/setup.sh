#!/bin/bash
#
# Setup AWS infrastructure for nuthatch integration tests.
#
# Prerequisites:
#   - AWS CLI configured with admin credentials
#
# This script creates:
#   - S3 bucket: nuthatch-test
#   - IAM user: nuthatch-test (with scoped S3 permissions)
#   - Access keys for the IAM user
#
# After running, update your .envrc with the output credentials.

set -e

BUCKET_NAME="nuthatch-test"
USER_NAME="nuthatch-test"
REGION="us-east-1"

echo "Creating S3 bucket: $BUCKET_NAME"
aws s3 mb "s3://$BUCKET_NAME" --region "$REGION"

echo "Creating IAM user: $USER_NAME"
aws iam create-user --user-name "$USER_NAME"

echo "Attaching S3 policy to user"
aws iam put-user-policy \
    --user-name "$USER_NAME" \
    --policy-name "${USER_NAME}-s3" \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::'"$BUCKET_NAME"'",
                    "arn:aws:s3:::'"$BUCKET_NAME"'/*"
                ]
            }
        ]
    }'

echo "Creating access keys"
aws iam create-access-key --user-name "$USER_NAME"

echo ""
echo "Done! Add the following to your .envrc:"
echo ""
echo "export AWS_TEST_BUCKET=\"$BUCKET_NAME\""
echo "export AWS_ACCESS_KEY_ID=\"<AccessKeyId from above>\""
echo "export AWS_SECRET_ACCESS_KEY=\"<SecretAccessKey from above>\""
