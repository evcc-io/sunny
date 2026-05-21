//go:build tools

// Package sunny references build-time tool dependencies so that
// `go mod tidy` keeps them in go.mod / go.sum. They are not compiled
// into the library binary because of the `tools` build tag.
package sunny

import (
	_ "github.com/dmarkham/enumer"
)
