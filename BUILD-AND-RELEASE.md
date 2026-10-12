## Checks and image build

The `justfile` at the repo root is the entry point for everything CI runs
(and will run — the workflows are thin shims over these recipes):

```bash
just lint                       # cargo fmt --check + clippy -D warnings
just test-unit                  # cargo test
just IMAGE_TAG=<tag> image      # docker build, tag baked in as KINFERENCE_VERSION
just cli-bin [TARGET]           # kinfctl for one rust target, laid out as a release
                                # asset: target/cli/kinfctl-<version>-<os>-<arch> (+ .sha256)
```

`just` with no arguments lists all recipes.

## Releasing

Releases run through the `release` GitHub Actions workflow
(`.github/workflows/release.yml`), dispatched manually:

- **Dry-run**: dispatch with `publish` unchecked (any branch). Builds the
  release-named image, installs the charts on kind, runs the smoke test, and
  builds `kinfctl` for every release target (downloadable from the run as
  artifacts). Pushes nothing.
- **Publish**: dispatch from `main` with `publish` checked and a semver
  `version` (no `v` prefix, e.g. `0.1.0`). On a green smoke test and green
  CLI builds it pushes `ghcr.io/buoyantio/kinference:<version>` (the exact
  tested bytes) and both charts (`kinference-crds`, `kinference` — versions
  in lockstep) to `oci://ghcr.io/buoyantio/charts`, creates the `v<version>`
  release in the public [BuoyantIO/kinference-releases](https://github.com/BuoyantIO/kinference-releases)
  repo, and finally tags `v<version>` here. A failed run leaves no tag, so
  the version can be retried.

This repo is private, so its own GitHub releases can't be fetched with plain
`curl`; that is why the release lives in `kinference-releases`, a public repo
that holds no source. It carries the `kinfctl` binaries, one per target, a
`.sha256` next to each, and notes that list the artifacts and install steps
only (no commit history):

```
kinfctl-<version>-darwin-arm64    # Apple Silicon
kinfctl-<version>-linux-amd64
```

Each binary is built natively on a runner of its kind (`just cli-bin <target>`),
checked to report the release version from `--version`, and run against a
sample policy before it is attached. The workflow writes to the public repo
with the `KINFERENCE_RELEASES_TOKEN` repository secret: a fine-grained PAT
whose only repository access is `kinference-releases`, with `Contents: read
and write`. Rotate it by creating a new token with the same scope and
`gh secret set KINFERENCE_RELEASES_TOKEN --repo BuoyantIO/llm-monitor`.

## Build and deploy

Do this:

```bash
export KINFERENCE_VERSION=dev-$(date "+%y-%m-%d-%H-%M")-$(git rev-parse --short HEAD)

echo Building version $KINFERENCE_VERSION
cargo build --release

echo Confirming:
./target/release/kinference --version

cd k8s/kinference

make build push deploy TAG=$KINFERENCE_VERSION
```

Then deploy the models, the gateway, etc

To expose gateway to the public internet:

```bash
make install-lbc
make expose-gateway
make gateway-endpoint
```

