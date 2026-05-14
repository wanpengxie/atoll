package worker

import "os"

// parentEnv returns os.Environ() as a copy. Lifted into its own helper
// so tests can shadow the function via a build-tagged file if they
// ever need to. Currently the production behaviour is "inherit
// everything verbatim".
func parentEnv() []string {
	return os.Environ()
}
