package zerobus

// sdkVersion must be bumped in sync with the `## Release vX.Y.Z` header in
// NEXT_CHANGELOG.md at each release.
const sdkVersion = "1.3.0"

const sdkIdentifierPrefix = "zerobus-sdk-go"

func sdkIdentifier() string {
	return sdkIdentifierPrefix + "/" + sdkVersion
}
