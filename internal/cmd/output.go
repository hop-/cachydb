package cmd

import (
	"fmt"
	"io"
)

// output writes command output and keeps the first write error, so a
// command can print several lines and check once.
type output struct {
	w   io.Writer
	err error
}

func (o *output) printf(format string, args ...any) {
	if o.err != nil {
		return
	}
	_, o.err = fmt.Fprintf(o.w, format, args...)
}
