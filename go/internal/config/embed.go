package config

import _ "embed"

// defaultYAML is a copy of config/default.yaml, the defaults every
// implementation shares. go generate refreshes it, and a test fails if it
// differs from the original.
//
//go:generate cp ../../../config/default.yaml default.yaml
//go:embed default.yaml
var defaultYAML []byte
