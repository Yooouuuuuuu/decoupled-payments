package config

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// kind is the type of a setting. It is the type of the setting's value in
// default.yaml, and every other source is parsed as that type.
type kind int

const (
	kindInt kind = iota // 64-bit integer
	kindFloat
	kindBool
	kindString
)

func (k kind) String() string {
	switch k {
	case kindInt:
		return "an integer"
	case kindFloat:
		return "a number"
	case kindBool:
		return "true or false"
	default:
		return "a string"
	}
}

// setting is the current value of one key and where it came from.
type setting struct {
	kind   kind
	value  any    // int64, float64, bool or string, per kind
	text   string // the value as its source wrote it
	source string // e.g. "environment variable DP_SITE_ID", for error messages
}

// Settings holds a value for every dotted key of default.yaml
// ("aggregator.blockSize"). Those keys are the only valid ones.
type Settings struct {
	keys   []string // in default.yaml order
	values map[string]*setting
	read   map[string]bool // keys read by the typed getters, see config_test.go
}

// Flag is one command-line flag: --name=value, or --name alone.
type Flag struct {
	Name     string
	Value    string
	HasValue bool // false for a bare --name, which sets a boolean to true
}

// parseDefaults reads default.yaml, which defines every key and its type.
func parseDefaults(data []byte) (*Settings, error) {
	leaves, err := readYAML(data, "default.yaml", nil)
	if err != nil {
		return nil, err
	}
	s := &Settings{values: make(map[string]*setting, len(leaves)), read: map[string]bool{}}
	for _, l := range leaves {
		k, err := kindOfTag(l.tag)
		if err != nil {
			return nil, fmt.Errorf("default.yaml: %s: %w", l.key, err)
		}
		s.keys = append(s.keys, l.key)
		s.values[l.key] = &setting{kind: k}
		if err := s.set(l.key, l.text, "default.yaml"); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func kindOfTag(tag string) (kind, error) {
	switch tag {
	case "!!int":
		return kindInt, nil
	case "!!float":
		return kindFloat, nil
	case "!!bool":
		return kindBool, nil
	case "!!str":
		return kindString, nil
	}
	return 0, fmt.Errorf("unsupported value type %s", tag)
}

// applyYAML overrides settings with the values of a YAML file. The file must
// have the shape of default.yaml: the same sections, and only its keys.
func (s *Settings) applyYAML(data []byte, file string) error {
	leaves, err := readYAML(data, "config file "+file, s.shape)
	if err != nil {
		return err
	}
	for _, l := range leaves {
		if err := s.set(l.key, l.text, l.source); err != nil {
			return err
		}
	}
	return nil
}

// shape tells readYAML whether a dotted path is a setting or a section of
// settings in default.yaml.
func (s *Settings) shape(path string) pathShape {
	if _, ok := s.values[path]; ok {
		return isSetting
	}
	for _, key := range s.keys {
		if strings.HasPrefix(key, path+".") {
			return isSection
		}
	}
	return isUnknown
}

// applyEnv overrides settings with the environment variables named by EnvName.
func (s *Settings) applyEnv(lookupEnv func(string) (string, bool)) error {
	for _, key := range s.keys {
		name := EnvName(key)
		if raw, ok := lookupEnv(name); ok {
			if err := s.set(key, raw, "environment variable "+name); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyFlags overrides settings with command-line flags. A flag that isn't a
// full key is looked up in the role's section: for the aggregator,
// --blockSize means --aggregator.blockSize.
func (s *Settings) applyFlags(role string, flags []Flag) error {
	for _, f := range flags {
		key, ok := s.resolve(role, f.Name)
		if !ok {
			return fmt.Errorf("unknown flag --%s: default.yaml has no setting %s or %s.%s", f.Name, f.Name, role, f.Name)
		}
		raw := f.Value
		if !f.HasValue {
			if s.values[key].kind != kindBool {
				return fmt.Errorf("flag --%s needs a value: --%s=<value>", f.Name, f.Name)
			}
			raw = "true"
		}
		if err := s.set(key, raw, "flag --"+f.Name); err != nil {
			return err
		}
	}
	return nil
}

func (s *Settings) resolve(role, name string) (string, bool) {
	if _, ok := s.values[name]; ok {
		return name, true
	}
	if key := role + "." + name; s.values[key] != nil {
		return key, true
	}
	return "", false
}

// set parses raw as the key's type and records where the value came from.
func (s *Settings) set(key, raw, source string) error {
	st, ok := s.values[key]
	if !ok {
		return fmt.Errorf("unknown setting %s (from %s)", key, source)
	}
	v, err := parse(st.kind, raw)
	if err != nil {
		return fmt.Errorf("invalid value %q for %s (from %s): want %s", raw, key, source, st.kind)
	}
	st.value, st.text, st.source = v, raw, source
	return nil
}

// The text every source must use, whatever the source: the YAML file, an
// environment variable or a flag. Java parses with the same rules, so a value
// means the same in both implementations (YAML 1.1 would read 010 as 8 and yes
// as true; Go's own parsers accept 0x10, 1_000 and NaN).
var (
	integerText = regexp.MustCompile(`^-?[0-9]+$`)                              // leading zeros are still decimal
	numberText  = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$`) // no hex, underscores, NaN or Inf
	errSyntax   = errors.New("invalid syntax")
)

func parse(k kind, raw string) (any, error) {
	switch k {
	case kindInt:
		if !integerText.MatchString(raw) {
			return nil, errSyntax
		}
		return strconv.ParseInt(raw, 10, 64)
	case kindFloat:
		if !numberText.MatchString(raw) {
			return nil, errSyntax
		}
		return strconv.ParseFloat(raw, 64)
	case kindBool:
		switch raw {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return nil, errSyntax
	default:
		return raw, nil
	}
}

// EnvName is the environment variable that overrides a key: DP_, then the key
// in upper snake case ("aggregator.blockSize" → DP_AGGREGATOR_BLOCK_SIZE).
func EnvName(key string) string {
	var b strings.Builder
	b.WriteString("DP_")
	var prev rune
	for _, r := range key {
		switch {
		case r == '.':
			b.WriteByte('_')
		case unicode.IsUpper(r) && (unicode.IsLower(prev) || unicode.IsDigit(prev)):
			b.WriteByte('_')
			b.WriteRune(r)
		default:
			b.WriteRune(unicode.ToUpper(r))
		}
		prev = r
	}
	return b.String()
}

// Keys returns every key in default.yaml order.
func (s *Settings) Keys() []string { return s.keys }

// Text returns a key's current value as its source wrote it, e.g. 1.0 for a
// default from default.yaml.
func (s *Settings) Text(key string) string { return s.values[key].text }

// The typed getters panic on a key that isn't in default.yaml with that type:
// that is a bug in this program, and config_test.go reads every key.

func (s *Settings) get(key string, k kind) any {
	st, ok := s.values[key]
	if !ok || st.kind != k {
		panic(fmt.Sprintf("config: default.yaml has no %v setting %s", k, key))
	}
	s.read[key] = true
	return st.value
}

func (s *Settings) int(key string) int64     { return s.get(key, kindInt).(int64) }
func (s *Settings) float(key string) float64 { return s.get(key, kindFloat).(float64) }
func (s *Settings) bool(key string) bool     { return s.get(key, kindBool).(bool) }
func (s *Settings) string(key string) string { return s.get(key, kindString).(string) }
func (s *Settings) source(key string) string { return s.values[key].source }
