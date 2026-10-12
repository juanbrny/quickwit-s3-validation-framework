#!/bin/sh
# Inside the image: route a command to the right program.
set -e
case "$1" in
  print-wrapper)
    # The wrapper, with this image's name filled in.
    sed "s|^DEFAULT_IMAGE=.*|DEFAULT_IMAGE=\"$BYOC_IMAGE\"|" /app/bin/byoc-s3
    ;;
  sample-report)
    shift
    exec python /app/examples/make_sample_report.py "$@"
    ;;
  *)
    exec python /app/run_validation.py "$@"
    ;;
esac
