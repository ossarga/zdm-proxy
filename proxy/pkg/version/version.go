package version

import (
	"fmt"
	"os"
)

var (
	ZdmVersion     = ""
	ZdmVersionType = "dev"
	ZdmGitHash     = ""
	ZdmBuildOs     = ""
	ZdmBuildArch   = ""
)

func ZdmVersionString() string {
	if ZdmVersion != "" && ZdmVersionType != "" {
		versionString := fmt.Sprintf(
			"v%s-%s",
			ZdmVersion,
			ZdmVersionType,
		)

		if ZdmGitHash != "" {
			versionString = fmt.Sprintf("%s-%s", versionString, ZdmGitHash)
		}

		if ZdmBuildOs != "" {
			versionString = fmt.Sprintf("%s-%s", versionString, ZdmBuildOs)
		}

		if ZdmBuildArch != "" {
			versionString = fmt.Sprintf("%s-%s", versionString, ZdmBuildArch)
		}

		return versionString
	}

	if envVersion := os.Getenv("ZDM_VERSION"); envVersion != "" {
		return envVersion
	}

	return "unknown"
}
