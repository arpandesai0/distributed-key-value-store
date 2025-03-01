package db

import (
	"bytes"
	"os"
	"reflect"
	"testing"
)

func CreateTempDatabase(t *testing.T, readOnly bool) *Database {
	f, err := os.CreateTemp(os.TempDir(), "tempdb")
	if err != nil {
		t.Fatalf("Couldn't create a temp file: %v", err)
	}
	name := f.Name()
	f.Close()
	defer os.Remove(name)

	db, closeFunc, err := NewDatabase(name, readOnly)
	t.Cleanup(func() {
		closeFunc()
	})
	if err != nil {
		t.Fatalf("Couldn't create a new database %v", err)
	}
	return db
}

func SetKey(t *testing.T, d *Database, key, value []byte) {
	t.Helper()
	if err := d.SetKey(key, value); err != nil {
		t.Fatalf("SetKey(%q, %q) failed: %v", key, value, err)
	}

}

func GetKey(t *testing.T, d *Database, key []byte) []byte {
	t.Helper()
	value, err := d.GetKey(key)
	if err != nil {
		t.Fatalf("GetKey(%q, %q) failed: %v", key, value, err)
	}
	return value
}

func TestGetSet(t *testing.T) {
	db := CreateTempDatabase(t, false)

	SetKey(t, db, []byte("hello"), []byte("world"))
	value := GetKey(t, db, []byte("hello"))
	if !reflect.DeepEqual(value, []byte("world")) {
		t.Fatalf("Unexpected value for key 'hello', expected: 'world', got: %s", value)
	}
}

func TestDeleteExtraKeys(t *testing.T) {
	db := CreateTempDatabase(t, false)

	SetKey(t, db, []byte("hello"), []byte("world"))
	SetKey(t, db, []byte("hello1"), []byte("world1"))
	SetKey(t, db, []byte("hello2"), []byte("world2"))
	value := GetKey(t, db, []byte("hello"))
	if !reflect.DeepEqual(value, []byte("world")) {
		t.Fatalf("Unexpected value for key 'hello', expected: 'world', got: %s", value)
	}

	if err := db.DeleteExtraKeys(func(name []byte) bool { return bytes.Equal(name, []byte("hello1")) }); err != nil {
		t.Fatalf("Could not delete extra keys: %v", err)
	}

	if value := GetKey(t, db, []byte("hello")); !bytes.Equal(value, []byte("world")) {
		t.Errorf("Unexpected value for key 'hello1: got %q, want %q", value, "")
	}

	if value := GetKey(t, db, []byte("hello1")); !bytes.Equal(value, []byte("")) {
		t.Errorf("Unexpected value for key 'hello1: got %q, want %q", value, "")
	}

	if value := GetKey(t, db, []byte("hello2")); !bytes.Equal(value, []byte("world2")) {
		t.Errorf("Unexpected value for key 'hello1: got %q, want %q", value, "")
	}
}

func TestSetReadOnlyDatabase(t *testing.T) {
	db := CreateTempDatabase(t, true)
	if err := db.SetKey([]byte("hello"), []byte("world")); err == nil {
		t.Errorf("Got nil error for set key in readonly database, expected non-nil error")
	}

}
