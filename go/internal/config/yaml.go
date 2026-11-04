package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"go.yaml.in/yaml/v3"
)

// leaf is one setting of a YAML file: its dotted key and its value as text.
type leaf struct {
	key    string
	text   string
	tag    string // the YAML library's type of the value; only default.yaml's are used
	source string // the file and line, for error messages
}

// pathShape is what default.yaml has at a dotted path.
type pathShape int

const (
	isUnknown pathShape = iota
	isSetting
	isSection // e.g. kafka, whose settings are kafka.bootstrapServers and so on
)

// readYAML reads a file of settings: nested sections ("kafka:") down to
// settings ("bootstrapServers: x"), which become dotted keys
// (kafka.bootstrapServers).
//
// It reads the file as a tree of nodes and keeps each value's text, which
// set parses by the type of the default. The YAML library never types a
// value: Java's SnakeYAML follows YAML 1.1, where yes, 010 and 1_000 mean
// true, 8 and 1000, so both implementations parse the text themselves with
// the same rules. Anything without a plain meaning is an error: a null, a
// list, an anchor or alias, a duplicate key, a second document. An empty
// file, or one with only comments or only ~, sets nothing.
//
// shape checks the file against default.yaml; nil accepts any shape, for
// reading default.yaml itself.
func readYAML(data []byte, source string, shape func(path string) pathShape) ([]leaf, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var doc yaml.Node
	switch err := dec.Decode(&doc); {
	case errors.Is(err, io.EOF):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("%s: %w", source, err)
	}
	var next yaml.Node
	switch err := dec.Decode(&next); {
	case err == nil:
		return nil, fmt.Errorf("%s: holds more than one YAML document", source)
	case !errors.Is(err, io.EOF):
		return nil, fmt.Errorf("%s: %w", source, err)
	}

	root := doc.Content[0]
	if root.Kind == yaml.ScalarNode && root.ShortTag() == "!!null" && root.Anchor == "" {
		return nil, nil
	}
	if root.Kind != yaml.MappingNode || root.Anchor != "" {
		return nil, fmt.Errorf("%s: line %d: expected settings (key: value)", source, root.Line)
	}
	r := yamlReader{source: source, shape: shape}
	if err := r.section(root, ""); err != nil {
		return nil, err
	}
	return r.leaves, nil
}

type yamlReader struct {
	source string
	shape  func(path string) pathShape
	leaves []leaf
}

// at names a node's place in the file, for error messages.
func (r *yamlReader) at(n *yaml.Node) string { return fmt.Sprintf("%s, line %d", r.source, n.Line) }

// expect fails unless default.yaml has the shape want at path.
func (r *yamlReader) expect(n *yaml.Node, path string, want pathShape) error {
	if r.shape == nil {
		return nil
	}
	switch got := r.shape(path); {
	case got == want:
		return nil
	case got == isSetting:
		return fmt.Errorf("%s takes a single value, not a section (from %s)", path, r.at(n))
	case got == isSection:
		return fmt.Errorf("%s is a section of settings, not a single value (from %s)", path, r.at(n))
	default:
		return fmt.Errorf("unknown setting %s (from %s)", path, r.at(n))
	}
}

func (r *yamlReader) node(n *yaml.Node, path string) error {
	if n.Anchor != "" || n.Kind == yaml.AliasNode {
		return fmt.Errorf("%s uses a YAML anchor or alias; write the value out instead (from %s)", path, r.at(n))
	}
	switch n.Kind {
	case yaml.MappingNode:
		return r.section(n, path)
	case yaml.ScalarNode:
		return r.value(n, path)
	default:
		return fmt.Errorf("%s is a list, but a setting takes a single value (from %s)", path, r.at(n))
	}
}

// section reads the keys of a mapping at path ("" for the whole file).
func (r *yamlReader) section(n *yaml.Node, path string) error {
	if path != "" {
		if err := r.expect(n, path, isSection); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		if k.Kind != yaml.ScalarNode || k.Anchor != "" {
			return fmt.Errorf("%s: a key must be a plain name", r.at(k))
		}
		key := k.Value
		if path != "" {
			key = path + "." + k.Value
		}
		if seen[k.Value] {
			return fmt.Errorf("%s appears twice (from %s)", key, r.at(k))
		}
		seen[k.Value] = true
		if err := r.node(v, key); err != nil {
			return err
		}
	}
	return nil
}

// value reads the scalar at path.
func (r *yamlReader) value(n *yaml.Node, path string) error {
	if err := r.expect(n, path, isSetting); err != nil {
		return err
	}
	if n.ShortTag() == "!!null" {
		return fmt.Errorf("%s has no value (from %s)", path, r.at(n))
	}
	r.leaves = append(r.leaves, leaf{key: path, text: n.Value, tag: n.ShortTag(), source: r.at(n)})
	return nil
}
