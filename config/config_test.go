package config

import (
	"os"
	"reflect"
	"testing"
)

func createConfig(t *testing.T, contents string) Config {
	t.Helper()

	f, err := os.CreateTemp(os.TempDir(), "config.toml")
	if err != nil {
		t.Fatalf("Couldn't create a temp file: %v", err)
	}
	defer f.Close()

	name := f.Name()
	defer os.Remove(name)

	_, err = f.WriteString(contents)
	if err != nil {
		t.Fatalf("Couldn't write the config contents: %v", err)
	}
	c, err := ParseFile(name)
	if err != nil {
		t.Fatalf("Could not parse config: %v", err)
	}
	return c
}

func TestParseFile(t *testing.T) {
	contents := `[[shards]]
	name = "A1"
	idx = 0
	address = "localhost:8080"
	`
	c := createConfig(t, contents)
	want := Config{
		Shards: []Shard{
			{
				Name:    "A1",
				Idx:     0,
				Address: "localhost:8080",
			},
		},
	}
	if !reflect.DeepEqual(c, want) {
		t.Errorf("The config does not match: got : %+v, want: %+v", c, want)
	}
}

func TestParseShards(t *testing.T) {
	contents := `
	[[shards]]
	name = "A1"
	idx = 0
	address = "localhost:8080"

	[[shards]]
	name = "B1"
	idx = 1
	address = "localhost:8081"
	`
	c := createConfig(t, contents)

	got, err := ParseShards(c.Shards, "B1")
	if err != nil {
		t.Fatalf("Could not parse shards %+v: %v", c.Shards, err)
	}
	want := &Shards{
		Count:  2,
		CurIdx: 1,
		Addrs: map[int]string{
			0: "localhost:8080",
			1: "localhost:8081",
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("The shards config does not match: got: %+v, want: %+v", got, want)
	}
}
