# Releasing

Releases are automated via GitHub Actions. Pushing a semver tag triggers the pipeline.

## Steps

1. Update `CHANGELOG.md` with the new version and date.

2. Commit the changelog:
   ```bash
   git add CHANGELOG.md
   git commit -m "Add CHANGELOG.md for vX.Y.Z release."
   ```

3. Push, tag, and release:
   ```bash
   git push origin main
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

## What the pipeline does

The `release.yml` workflow runs on any `v*.*.*` tag push:

1. **Verify** — runs `go test ./...`, `make architecture`, `go vet ./...`, and
   `make vulncheck`.
2. **Build** — cross-compiles for linux/amd64, linux/arm64, darwin/amd64, darwin/arm64 with version/commit/date baked into the binary.
3. **Publish** — creates a GitHub Release with tarballs, checksums, and auto-generated release notes.

## Versioning

This project follows [Semantic Versioning](https://semver.org/):

- **Patch** (v0.1.1) — bug fixes, no behavior changes.
- **Minor** (v0.2.0) — new features, backward compatible.
- **Major** (v1.0.0) — breaking changes.

During pre-v1 development, minor versions may include breaking changes.

## Security maintenance

Go modules and GitHub Actions are checked weekly by Dependabot. The Go version
in `go.mod` is the minimum patched toolchain used by CI and release builds.
Reachable vulnerability fixes take priority over the regular release cadence:
merge patched dependencies after the full test and vulnerability suites pass,
and publish a patch release when a published binary is affected.
