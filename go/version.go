package zerobus

// sdkVersion must match the version in the next go/vX.Y.Z release tag.
const sdkVersion = "1.4.0"

const sdkIdentifierPrefix = "zerobus-sdk-go"

func sdkIdentifier() string {
	return sdkIdentifierPrefix + "/" + sdkVersion
}
