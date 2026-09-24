package user

import (
	"fmt"
	"strings"
)

var validDisableNames = []string{"chat", "whiteboard", "notepad", "reactions", "hands"}

// DisableSet implements flag.Value with append semantics: comma-separated
// values and repeated flag instances both accumulate.
type DisableSet []string

func (d *DisableSet) String() string { return strings.Join(*d, ",") }

func (d *DisableSet) Set(s string) error {
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		*d = append(*d, part)
	}
	return nil
}

// Validate checks every accumulated name.
func (d *DisableSet) Validate() error {
	for _, n := range *d {
		ok := false
		for _, v := range validDisableNames {
			if n == v {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("unknown --disable feature: %q (valid names: %s)", n, strings.Join(validDisableNames, ", "))
		}
	}
	return nil
}

// Apply marks the listed features false (subtractive) and returns the
// disabled names sorted for banner display.
func (d *DisableSet) Apply(f *Features) []string {
	var disabled []string
	for _, n := range *d {
		switch n {
		case "chat":
			f.Chat = false
		case "whiteboard":
			f.Whiteboard = false
		case "notepad":
			f.Notepad = false
		case "reactions":
			f.Reactions = false
		case "hands":
			f.Hands = false
		}
	}
	if len(*d) == 0 {
		return nil
	}
	for _, n := range validDisableNames {
		for _, un := range *d {
			if n == un {
				disabled = append(disabled, n)
				break
			}
		}
	}
	return disabled
}
