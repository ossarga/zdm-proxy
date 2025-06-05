# Release Process

All published container images can be found at [https://hub.docker.com/r/datastax/zdm-proxy/tags?page=1&ordering=last_updated](https://hub.docker.com/r/datastax/zdm-proxy/tags?page=1&ordering=last_updated).

## Official/Stable Releases

### Before publishing an official release

Before triggering the build and publish process for an official/stable release, two files need to be updated: the `RELEASE_NOTES` and `CHANGELOG`.

**Note**: Version information is automatically managed through Git tags, the `VERSION` file, or environment variables with build-time injection into the `proxy/pkg/version` package.

**Version Sources (in priority order):**
1. Build-time injection via `-ldflags` (primary method for builds)
2. `ZDM_VERSION` environment variable (fallback)
3. Git tags (for tagged releases)
4. `VERSION` file in repository root

The [RELEASE_NOTES.md](RELEASE_NOTES.md) file should be updated so that it contains a section for the new release.

The `CHANGELOG.md` file associated with the release (in the [CHANGELOG](CHANGELOG) folder) should be updated so that tickets in the `UNRELEASED` section are moved to a new section (in the same file) that is specific to the new release.

These changes should be done on a branch so that a PR can be opened for review prior to merging them.

### Building and publishing the docker image

Periodically, "official"/"stable" releases with standard semantic versioning applied are released to capture milestones for the project.  This process happens in automated fashion through the use of the GH Action workflow found in [release.yml](.github/workflows/release.yml).

The Dockerfile uses a multi-stage build process that supports both deployment and build-only use cases:

**Deployment Build (default):**
```bash
# Builds complete runtime image
docker build -t zdm-proxy .
```

**Build-Only Usage:**
```bash
# Build and extract versioned binary
docker build --target builder -t zdm-proxy-builder .
docker run -d --name build-container zdm-proxy-builder
docker cp build-container:/dist/zdm-proxy-v2.6.4-release-abc123-linux-amd64 ./
```

**Custom Version Build:**
```bash
docker build \
  --build-arg VERSION=2.6.4 \
  --build-arg VERSION_TYPE=release \
  --build-arg GIT_HASH=abc123 \
  -t zdm-proxy .
```

If a semantically valid release tag is pushed then the `release.yml` workflow will consider it an official/stable release and tag the docker image with a series of tags as shown in the following example.

As an example, let's say we want to publish a `v2.0.0` official release. To do that, push a `v2.0.0` tag:

```
git tag v2.0.0
git push origin v2.0.0
```

The result of that workflow is the creation and publishing of a Docker image with a series of tags applied to it as mentioned before, for example:

* `v2.0.0` -- A tag marking the exact version published
* `v2.x` -- A moving tag at all times marking the most recent `major` version compatible image (e.g `2.0.0`, `2.0.1`, `2.1.0`, `2.2.0`)
* `v2.0.x` -- A moving tag at all times marking the most recent `minor` version compatible image (e.g `2.0.0`, `2.0.1`, `2.0.2`, `2.0.3`)
* `latest` -- A moving tag at all times marking the most recent stable image

### Binary Naming Convention

Starting with version 2.5.2, ZDM Proxy binaries follow a standardized naming convention:

```
zdm-proxy-v<major>.<minor>.<build>-<type>-<git_hash>-<os>-<architecture>
```

**Examples:**
- `zdm-proxy-v2.6.4-release-f2b188c-linux-amd64`
- `zdm-proxy-v2.6.4-custom-f2b188c-darwin-arm64`
- `zdm-proxy-v2.6.5-dev-a1b2c3d-windows-amd64`

**Components:**
- **version**: Semantic version from Git tag (e.g., `v2.6.4`)
- **type**: Build type (`release`, `custom`, `dev`, `rc`)
- **git_hash**: Short Git commit hash (7 characters)
- **os**: Target operating system (`linux`, `darwin`, `windows`)
- **architecture**: Target architecture (`amd64`, `arm64`)

### Version Management

Version information is managed through multiple sources with the following priority:

1. **Environment Variables** (highest priority):
   ```bash
   export ZDM_VERSION="2.6.4"
   export ZDM_VERSION_TYPE="custom"
   ```

2. **Git Tags** (for releases):
   ```bash
   git tag v2.6.4
   git push origin v2.6.4
   ```

3. **VERSION File** (fallback):
   ```bash
   echo "2.6.4" > VERSION
   ```

**Build Examples:**
```bash
# Local binary build using VERSION file
make build-binary

# Local build with environment variables
ZDM_VERSION="2.6.4" ZDM_VERSION_TYPE="custom" make build-binary

# Cross-platform build
GOOS=linux GOARCH=amd64 make build-binary-cross

# Docker build for deployment
docker build -t zdm-proxy .

# Docker build-only for binary extraction
docker build --target builder -t zdm-proxy-builder .

# Docker build with custom version
docker build \
  --build-arg VERSION=2.6.4 \
  --build-arg VERSION_TYPE=release \
  --target builder \
  -t zdm-proxy-builder .
```

### Create an official `Release` in GitHub

Once the tag has been pushed to the repository and the build process completed, a new `Release` will be created automatically within GitHub matching the tag. A manual step is required to update the release notes.

1. Navigate in a browser to [https://github.com/datastax/zdm-proxy/releases](https://github.com/datastax/zdm-proxy/releases).
2. Search for the release matching the new tag name and edit it.
3. Paste the contents of the `RELEASE_NOTES` relevant to this release into the text-area for `Describe the release`.
4. Click the `Update release` button.

## Per-Merge Releases

To support easier testing workflows, every commit that is pushed to the primary `main` branch will trigger a build and publish of a non official release.

This happens automatically and requires no manual steps.  This process is provided for through the use of the same GH Action workflow that is used for official releases, i.e., [release.yml](.github/workflows/release.yml).

The result of this workflow is the creation and publishing of a Docker image with only the `main` tag applied to it (no `latest`).

## Manually triggered test releases on any branch

To support easier testing workflows without requiring a merge to `main`, you can manually trigger the [push-release.yml](.github/workflows/push-release.yml) workflow (selecting which branch you want to use in the Github UI) which will result in the creation and publishing of a Docker image with a tag specific to the SHA of the commit from which it was built (e.g. `sha-3758gd2b`).
