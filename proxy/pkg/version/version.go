package version

import "fmt"

var ReleaseVersion = "0.0.0"
var ReleaseType = "development"
var ReleaseHash = ""
var ReleaseOS = ""
var ReleaseArch = ""

func ZdmVersionString() string {
	versionString := fmt.Sprintf("v%s-%s", ReleaseVersion, ReleaseType)
	if ReleaseHash != "" {
		versionString = fmt.Sprintf("%s-%s", versionString, ReleaseHash)
	}

	if ReleaseOS != "" {
		versionString = fmt.Sprintf("%s-%s", versionString, ReleaseOS)
	}

	if ReleaseArch != "" {
		versionString = fmt.Sprintf("%s-%s", versionString, ReleaseArch)
	}

	return versionString
}
