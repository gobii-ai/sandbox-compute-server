# Sandbox Compute Server

Minimal HTTP supervisor for Gobii sandbox compute pods.

## Running

```sh
pip install -r requirements.txt

# Local run (for testing)
python -c "import sandbox_compute_server as s; print(s.application)"
```

## Production

The container image is built from the Gobii infra repo using the
`infra/platform/sandbox-compute-image/Dockerfile` with this repo checked out
as `sandbox_compute_server/` in the build context.
