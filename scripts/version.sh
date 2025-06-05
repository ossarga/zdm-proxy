#!/bin/bash

# Version detection and formatting script for ZDM Proxy
# Generates version information for build-time injection

set -e

# Function to get version from Git tag
get_git_version() {
    if git describe --tags --exact-match HEAD 2>/dev/null; then
        git describe --tags --exact-match HEAD | sed 's/^v//'
    elif [ -n "${GITHUB_REF_NAME:-}" ] && [[ "${GITHUB_REF_NAME}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+ ]]; then
        echo "${GITHUB_REF_NAME}" | sed 's/^v//'
    else
        echo ""
    fi
}

# Function to get Git hash (short form)
get_git_hash() {
    local hash
    hash=$(git rev-parse --short=7 HEAD 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$hash" ]; then
        echo "$hash"
    else
        echo "unknown"
    fi
}

# Function to determine build type
get_build_type() {
    if [ -n "${ZDM_VERSION_TYPE:-}" ]; then
        echo "${ZDM_VERSION_TYPE}"
    elif [ -n "${GITHUB_REF_TYPE:-}" ] && [ "${GITHUB_REF_TYPE}" = "tag" ]; then
        echo "release"
    elif [ -n "${GITHUB_REF_NAME:-}" ] && [[ "${GITHUB_REF_NAME}" =~ -custom$ ]]; then
        echo "custom"
    elif [ -n "${GITHUB_REF_NAME:-}" ] && [[ "${GITHUB_REF_NAME}" =~ -rc ]]; then
        echo "rc"
    else
        echo "dev"
    fi
}

# Function to get OS
get_os() {
    if [ -n "${GOOS:-}" ]; then
        echo "${GOOS}"
    else
        case "$(uname -s)" in
            Linux*)     echo "linux";;
            Darwin*)    echo "darwin";;
            CYGWIN*|MINGW*|MSYS*) echo "windows";;
            *)          echo "unknown";;
        esac
    fi
}

# Function to get architecture
get_arch() {
    if [ -n "${GOARCH:-}" ]; then
        echo "${GOARCH}"
    else
        case "$(uname -m)" in
            x86_64|amd64)   echo "amd64";;
            arm64|aarch64)  echo "arm64";;
            armv7l)         echo "arm";;
            i386|i686)      echo "386";;
            *)              echo "unknown";;
        esac
    fi
}

# Function to parse version components
parse_version() {
    local version="$1"
    if [[ "$version" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+) ]]; then
        echo "${BASH_REMATCH[1]} ${BASH_REMATCH[2]} ${BASH_REMATCH[3]}"
    else
        echo "0 0 0"
    fi
}

# Main logic
main() {
    local action="${1:-version}"
    
    # Get version from various sources
    local git_version
    git_version=$(get_git_version)
    
    local version
    if [ -n "${ZDM_VERSION:-}" ]; then
        version="${ZDM_VERSION}"
        # Remove 'v' prefix if present
        version="${version#v}"
    elif [ -n "$git_version" ]; then
        version="$git_version"
    elif [ -f "VERSION" ]; then
        version=$(cat VERSION | tr -d '\n\r')
    else
        echo "Error: No version information available. Set ZDM_VERSION environment variable or create VERSION file." >&2
        exit 1
    fi

    # Get other components
    local build_type
    build_type=$(get_build_type)
    
    local git_hash
    git_hash=$(get_git_hash)
    
    local os
    os=$(get_os)
    
    local arch
    arch=$(get_arch)
    
    # Generate outputs based on action
    case "$action" in
        "version")
            echo "${version}"
            ;;
        "binary-name")
            echo "zdm-proxy-v${version}-${build_type}-${git_hash}-${os}-${arch}"
            ;;
        "ldflags")
            echo "-X github.com/datastax/zdm-proxy/proxy/pkg/version.ZdmVersion=${version} \
                  -X github.com/datastax/zdm-proxy/proxy/pkg/version.ZdmVersionType=${build_type} \
                  -X github.com/datastax/zdm-proxy/proxy/pkg/version.ZdmGitHash=${git_hash}"
            ;;
        "all")
            echo "VERSION=${version}"
            echo "BINARY_NAME=zdm-proxy-v${version}-${build_type}-${git_hash}-${os}-${arch}"
            echo "GIT_HASH=${git_hash}"
            echo "BUILD_TYPE=${build_type}"
            echo "OS=${os}"
            echo "ARCH=${arch}"
            ;;
        *)
            echo "Usage: $0 [version|binary-name|ldflags|all]"
            exit 1
            ;;
    esac
}

main "$@"
