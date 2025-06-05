package version

import "fmt"

var (
	ZdmVersion     = ""
	ZdmBuildOs     = ""
	ZdmBuildArch   = ""
	ZdmVersionType = ""
)

func ZdmVersionString() string {
	if ZdmVersion != "" && ZdmBuildOs != "" && ZdmBuildArch != "" {
		versionString := fmt.Sprintf("v%s", ZdmVersion)

		if ZdmVersionType != "" {
			versionString = fmt.Sprintf("%s-%s", versionString, ZdmVersionType)
		}

		versionString = fmt.Sprintf("%s-%s", versionString, ZdmBuildOs)
		versionString = fmt.Sprintf("%s-%s", versionString, ZdmBuildArch)

		return versionString
	}

	return "unknown"
}
