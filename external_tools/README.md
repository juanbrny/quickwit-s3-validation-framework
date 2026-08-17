# External tools (Layer 1 and raw-throughput cross-check)

This framework does not reimplement generic S3 (Simple Storage Service) compliance checks.
It also does not reimplement raw-throughput benchmarking.
Mature open-source tools already do both well.

Run these tools first.
If they fail, fix the underlying issue before you run anything in `src/`.

## Ceph `s3-tests` (functional compliance)

```bash
git clone https://github.com/ceph/s3-tests.git
cd s3-tests
./bootstrap

cat > s3tests.conf <<EOF
[DEFAULT]
host = s3.vendor.example.com
port = 443
is_secure = true

[fixtures]
bucket prefix = qwcert-{random}-

[s3 main]
access_key = ${VENDOR_ACCESS_KEY}
secret_key = ${VENDOR_SECRET_KEY}
EOF

S3TEST_CONF=s3tests.conf tox -- \
  s3tests_boto3/functional/test_s3.py -k "multipart or range or delete_multi or list_objects"
```

Focus the `-k` filter on the operation families that Quickwit actually uses.
See `docs/01_s3_interaction_analysis.md` §7 for the list.
The full suite includes many ACL (Access Control List), versioning, and lifecycle tests.
Quickwit never uses these operations, so a vendor should not need to pass them for this certification.

## MinIO `mint` (containerized compliance, easier one-shot run)

```bash
docker run --rm \
  -e SERVER_ENDPOINT=s3.vendor.example.com:443 \
  -e ACCESS_KEY=$VENDOR_ACCESS_KEY \
  -e SECRET_KEY=$VENDOR_SECRET_KEY \
  -e ENABLE_HTTPS=1 \
  minio/mint:latest
```

## MinIO `warp` (raw throughput/latency baseline, no Quickwit shape)

Use this test to tell apart two cases:
- The endpoint is generally slow.
- The endpoint is slow only under Quickwit's specific mix of operations.

To do this, run the test against both the vendor and AWS S3, using identical parameters.

```bash
# Mixed GET/PUT, object sizes similar to an immature split at your target tier
warp mixed \
  --host=s3.vendor.example.com --access-key=$VENDOR_ACCESS_KEY --secret-key=$VENDOR_SECRET_KEY \
  --bucket=qw-cert-warp --obj.size=64MiB --duration=10m \
  --get-distrib=45 --put-distrib=45 --delete-distrib=5 --stat-distrib=5

# Repeat identically against AWS S3 for the baseline comparison
warp mixed \
  --host=s3.us-east-1.amazonaws.com --access-key=$AWS_AK --secret-key=$AWS_SK \
  --bucket=qw-cert-warp-baseline --obj.size=64MiB --duration=10m \
  --get-distrib=45 --put-distrib=45 --delete-distrib=5 --stat-distrib=5
```

`warp`'s own report already gives p50, p90, and p99 latency and throughput.
Keep both `warp` reports alongside the `src/report.py` output when you hand a
certification report to the vendor or to Quickwit's maintainers.
Together, the two reports show whether a gap is a generic-throughput issue or specific to Quickwit's operation mix.

## Multipart minimum-part-size probe (edge case worth calling out separately)

Real AWS S3 requires multipart parts to be at least 5 MiB, except for the last part.
It also supports objects up to 5 TiB in size.
Some appliances enforce different minimum or maximum sizes.

Use this manual check if `compat_checks.py`'s multipart check passes, but you want to find the exact boundaries:

```bash
aws s3api create-multipart-upload --endpoint-url https://s3.vendor.example.com \
  --bucket qw-cert --key boundary-test.split
# then try upload-part with a 1MiB part (expect it to be rejected on non-last parts
# if the vendor matches AWS's minimum), and again with a part >5GiB (expect rejection
# per AWS's max single-part size), to map out where the vendor's limits actually sit.
```
