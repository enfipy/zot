package local_test

import (
	"bytes"
	"os"
	"path"
	"testing"

	"zotregistry.dev/zot/v2/pkg/storage/local"
)

func TestCloneCopiesContentToNewFile(t *testing.T) {
	dir := t.TempDir()
	src := path.Join(dir, "src")
	dst := path.Join(dir, "nested", "dst")
	content := bytes.Repeat([]byte("clone-content "), 4096)

	if err := os.WriteFile(src, content, 0o600); err != nil {
		t.Fatal(err)
	}

	driver := local.New(true)
	if err := driver.Clone(src, dst); err != nil {
		t.Fatalf("clone: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("clone content differs: %v", err)
	}

	if err := driver.Clone(src, dst); err == nil {
		t.Fatal("clone over an existing file must fail")
	}

	if err := driver.Clone(path.Join(dir, "missing"), path.Join(dir, "other")); err == nil {
		t.Fatal("clone of a missing file must fail")
	}

	if _, err := os.Stat(path.Join(dir, "other")); !os.IsNotExist(err) {
		t.Fatal("a failed clone must leave no file")
	}
}
