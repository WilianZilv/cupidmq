# Releasing CupidMQ

Branch: **`main`**. Consumption map: [README — Use in your project](README.md#use-in-your-project).

## What each release ships

| Artifact | Install from |
|----------|----------------|
| `cupidmq-headless` / `cupidmq-headless.exe` | GitHub **Release** — master, metrics API only (`:9752/health`, `/metrics`) |
| `cupidmq` / `cupidmq.exe` | Same Release — master **with embedded dashboard** (`:9752/`) |
| `cupidmq.conf.example` | Same Release (once, from Linux job) |
| `cupidmq_client-*.whl` | GitHub **Release** URL (`pip` / `uv`) |
| Rust library | **Git repo** — `tag`, `branch`, or `rev` (not a Release URL) |
| Python (rolling) | **Git repo** — `@main` + `subdirectory=python-client` |
| Master Docker | **Git repo** — `docker build …#main` or `#v0.1.0` |
| Load tool `cupidmq-producer` | **Not** in Release — `make build` / git examples |

## First publish (repo not on GitHub yet)

```bash
git init -b main
git add .
git commit -m "chore: initial cupidmq"
git remote add origin https://github.com/WilianZilv/cupidmq.git
git push -u origin main
```

Replace the remote URL with your org/repo.

## Cut a release

1. Bump **both** versions (must match the tag without `v`):
   - `master/Cargo.toml` → `version = "0.1.0"`
   - `python-client/pyproject.toml` → `version = "0.1.0"`

2. Commit on `main`, push.

3. Validate locally:

   ```bash
   make check-release TAG=v0.1.0
   # or: bash scripts/check-release-version.sh v0.1.0
   ```

4. Tag and push (tag can point to any commit on `main`; usually the version bump commit):

   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```

5. [`.github/workflows/release.yml`](.github/workflows/release.yml) builds the Vite dashboard and uploads **two** master binaries per OS:
   - `cupidmq-headless` — no UI (smaller)
   - `cupidmq` — dashboard embedded (`:9752/`)
   - `cupidmq.conf.example` (Linux job)
   - `cupidmq_client-0.1.0-py3-none-any.whl`

6. Verify on GitHub → **Releases** → assets list.

## Multiple tags on `main`

Normal. `v0.1.0`, `v0.1.1`, `v0.2.0` can all live on the same branch history. Consumers pin the tag they need; `branch = "main"` always tracks HEAD.

## Local dry-run (no GitHub)

```bash
make publish          # dashboard build + embedded master → dist/
make publish-packages # wheel → dist/
```

`make publish` builds `cupidmq-headless` first, then `npm run build` + `cupidmq` with `embed-dashboard` (two separate `cargo` invocations so headless stays slim).
