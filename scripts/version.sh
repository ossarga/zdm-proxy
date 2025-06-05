#!/bin/bash

# Version detection and formatting script for ZDM Proxy
# Generates version information for build-time injection using the following sources, in order of precedence:
# 1. GitHub Actions environment variables
# 2. Git tags
# 3. VERSION file
# Supports multiple output formats for different build scenarios


set -e


get_github_actions_tag_ref_name() {
    if [ -n "${GITHUB_REF_TYPE:-}" ] && \
        [ "${GITHUB_REF_TYPE}" = "tag" ] && \
        [ -n "${GITHUB_REF_NAME:-}" ] && \
        [[ "${GITHUB_REF_NAME}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+ ]]
    then
        echo "${GITHUB_REF_NAME}"
    else
        echo ""
    fi
}

get_git_desc_tags_exact_match() {
    local git_desc_tags=""
    git_desc_tags=$(git describe --tags --exact-match HEAD 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$git_desc_tags" ]
    then
        echo "$git_desc_tags"
    else
        echo ""
    fi
}

get_version_and_build_type() {
    local gh_actions_ref=""
    local git_desc_tags=""
    local git_version_info=""

    gh_actions_ref=$(get_github_actions_tag_ref_name)
    git_desc_tags=$(get_git_desc_tags_exact_match)
    git_version_info="${gh_actions_ref:-$git_desc_tags}"

    local version_rtn=""
    local build_type_rtn=""

    # If Git tag information is available, then generate versioning for an official release.
    if [ -n "$git_version_info" ]
    then
        if [[ "${git_version_info}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+\-custom$ ]]
        then
            version_rtn=$(cut -d'-' -f1 <<< "${git_version_info/v/}")
            build_type_rtn="custom"
        elif [[ "${git_version_info}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+\-rc[0-9]+$ ]]
        then
            version_rtn=$(cut -d'-' -f1 <<< "${git_version_info/v/}")
            build_type_rtn=$(cut -d'-' -f2 <<< "${git_version_info}")
        elif [[ "${git_version_info}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]
        then
            version_rtn="${git_version_info/v/}"
            # For official releases, typically no build type is specified.
            # However, allow build type override in this specific case.
            if [ -n "${BUILD_TYPE:-}" ]
            then
                build_type_rtn="${BUILD_TYPE}"
            else
                build_type_rtn=""
            fi
        fi
    # Otherwise, generate versioning for a development build.
    else
        build_type_rtn="dev"
        if [ -f "VERSION" ]
        then
            file_version=$(cat VERSION | tr -d '\n\r')
            if [ -n "$file_version" ]
            then
                version_rtn="$file_version"
            fi
        fi

        # Fallback to the current tag if VERSION file is unavailable or invalid.
        if [[ ! "${version_rtn}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
        then
            git_desc_tags=$(git describe --tags)
            version_rtn=$(cut -d'-' -f1 <<< "${git_desc_tags/v/}")
        fi

        local hash
        hash=$(git rev-parse --short=7 HEAD 2>/dev/null)
        if [ $? -eq 0 ] && [ -n "$hash" ]
        then
            build_type_rtn="dev-${hash}"
        fi

         # Allow build type override for development builds
         if [ -n "${BUILD_TYPE:-}" ]
         then
             build_type_rtn="${BUILD_TYPE}"
         fi
    fi

    echo "$version_rtn|$build_type_rtn"
}


# Function to get OS
get_os() {
    if [ -n "${GOOS:-}" ]
    then
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
    if [ -n "${GOARCH:-}" ]
    then
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

# Main logic
main() {
    local action="${1:-version}"

    local version
    local build_type
    local version_and_build_type

    version_and_build_type=$(get_version_and_build_type)
    version=${version_and_build_type/|*/}
    build_type=${version_and_build_type/*|/}
    
    local os
    os=$(get_os)
    
    local arch
    arch=$(get_arch)
    
    # Generate outputs based on action
    case "$action" in
        "version")
            echo "${version}"
            ;;
        "version-tag")
            local version_tag="v${version}"
            if [ -n "${build_type}" ]
            then
              version_tag="${version_tag}-${build_type}"
            fi
            echo "${version_tag}"
            ;;
        "binary-name")
            local binary_name="zdm-proxy-v${version}"
            if [ -n "${build_type}" ]
            then
              binary_name="${binary_name}-${build_type}"
            fi
            echo "${binary_name}-${os}-${arch}"
            ;;
        "ldflags")
            ldflags_out=(
                "-X github.com/datastax/zdm-proxy/proxy/pkg/version.ZdmVersion=${version}"
            )

            if [ -n "${build_type}" ]
            then
              ldflags_out+=("-X github.com/datastax/zdm-proxy/proxy/pkg/version.ZdmVersionType=${build_type}")
            fi

            ldflags_out+=(
                "-X github.com/datastax/zdm-proxy/proxy/pkg/version.ZdmBuildOs=${os}"
                "-X github.com/datastax/zdm-proxy/proxy/pkg/version.ZdmBuildArch=${arch}"
            )

            echo "${ldflags_out[@]}"
            ;;
        "vars")
            binary_name_version="zdm-proxy-v${version}"
            echo "VERSION=${version}"
            if [ -n "${build_type}" ]
            then
              echo "BUILD_TYPE=${build_type}"
              binary_name_version="${binary_name_version}-${build_type}"
            fi
            echo "OS=${os}"
            echo "ARCH=${arch}"
            echo "BINARY_NAME=${binary_name_version}-${os}-${arch}"
            ;;
        *)
            echo "Usage: $0 [version|version-tag|binary-name|ldflags|vars]"
            exit 1
            ;;
    esac
}

main "$@"
