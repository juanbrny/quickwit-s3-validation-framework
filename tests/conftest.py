"""
Shared fixtures for the framework's own test suite.

These tests validate that the framework's *code* does what it claims --
e.g. that compat_checks.py's five checks actually pass against a
correctly-behaving S3 implementation, that put_split() really falls back to
single-PutObject when multipart is disabled, etc. They run against `moto`
(an in-memory S3 emulator), not a real bucket, so they're fast, free, and
need no credentials.

They are NOT a substitute for running run_certification.py against a real
vendor endpoint -- moto emulates AWS S3's behavior closely, but a vendor's
actual implementation is exactly what's in question when you run this
framework for real. Think of these tests as "does our checker work
correctly", not "is vendor X compliant".
"""
import os
import sys
from pathlib import Path

# Repo root on sys.path so `from src...` imports work from any test module.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import boto3
import pytest
from moto import mock_aws

TEST_BUCKET = "qw-cert-test"
TEST_REGION = "us-east-1"


@pytest.fixture(scope="function")
def aws_credentials():
    """Dummy credentials -- moto intercepts calls before they hit the network."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = TEST_REGION


@pytest.fixture(scope="function")
def moto_s3(aws_credentials):
    """Starts a fresh in-memory S3 backend with one bucket, per test."""
    with mock_aws():
        client = boto3.client("s3", region_name=TEST_REGION)
        client.create_bucket(Bucket=TEST_BUCKET)
        yield client
