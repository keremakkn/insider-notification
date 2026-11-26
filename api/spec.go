// Package apispec embeds the OpenAPI document, so the service can serve it
// and tests can check it against the router.
package apispec

import _ "embed"

//go:embed openapi.yaml
var OpenAPI []byte
