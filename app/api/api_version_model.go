package api

type apiVersionRequest struct {
	MessageSize uint32
	Header      *CommonAPIRequestHeader
	Body        *apiVersionRequestBody
}

type apiVersionRequestBody struct {
	*apiVersionRequestBodyClientID
	*apiVersionClientSoftwareVersion
}

type apiVersionRequestBodyClientID struct {
	Length   uint8
	Contents []byte
}

type apiVersionClientSoftwareVersion struct {
	Length   uint8
	Contents []byte
}

type apiVersionResponseBody struct {
	ErrorCode    uint16
	APIVersions  *apiVersionsArray
	ThrottleTime uint32
}

type apiVersionsArray struct {
	ArrayLength uint8
	VersionList []*apiVersion
}

type apiVersion struct {
	APIKey                 uint16
	MinSupportedAPIVersion uint16
	MaxSupportedAPIVersion uint16
}

type apiVersionResponse struct {
	MessageSize uint32
	Header      *ResponseHeader
	Body        *apiVersionResponseBody
}
