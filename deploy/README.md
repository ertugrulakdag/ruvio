# Deployment files

| File | Use |
| --- | --- |
| `ruvio.toml` | Example configuration; every key is documented in [docs/CONFIGURATION.md](../docs/CONFIGURATION.md) |
| `ruvio.service` | systemd unit that runs `/usr/local/bin/ruvio --config /etc/ruvio/ruvio.toml` as the `ruvio` user |
| `kubernetes.yaml` | Example Kubernetes manifest |

The Docker image and its environment variables are described in the [main README](../README.md#start-the-server) and in [docs/SETUP.md](../docs/SETUP.md). Review the files before use; they are starting points, not a hardened production setup.

## Local Ruvio, Redis, and UI in Docker

`local.sh` starts the published Ruvio, Redis, and Ruvio UI images on this machine, using `compose.stack.yml`:

```bash
deploy/local.sh up      # first run asks: Mac or Windows, and ARM or AMD container CPU
deploy/local.sh status  # containers, plus PING, SET, GET on Ruvio and Redis
deploy/local.sh down    # stop and remove the containers; the Ruvio data volume is kept
```

- Ruvio listens on `127.0.0.1:6379`. It has no username or password, and it is reachable from this machine only.
- Redis listens on `127.0.0.1:6380`, also without a password and localhost only.
- The UI is at `http://127.0.0.1:8080`.
- The answers are saved in `deploy/.local.env` (git-ignored). Delete that file to be asked again. To skip the questions, set `LOCAL_OS=mac|windows` and `LOCAL_ARCH=arm64|amd64` before `up`.
- Ruvio runs in memory durability mode, so its data is lost when the container is removed.
