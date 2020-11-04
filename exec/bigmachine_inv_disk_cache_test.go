package exec

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/testutil/assert"
)

func TestInvDiskCache(t *testing.T) {
	c := newInvDiskCache()

	rc, err := c.getOrCreate(0, func(w io.Writer) error {
		_, err := w.Write([]byte("entry0"))
		return err
	})
	assert.NoError(t, err)
	got, err := ioutil.ReadAll(rc)
	assert.NoError(t, err)
	assert.EQ(t, string(got), "entry0")

	rc, err = c.getOrCreate(3, func(w io.Writer) error {
		_, err2 := w.Write([]byte("entry3"))
		return err2
	})
	assert.NoError(t, err)
	got, err = ioutil.ReadAll(rc)
	assert.NoError(t, err)
	assert.EQ(t, string(got), "entry3")

	rc, err = c.getOrCreate(0, func(w io.Writer) error {
		t.Fatal("should be cached")
		panic("failed")
	})
	assert.NoError(t, err)
	got, err = ioutil.ReadAll(rc)
	assert.NoError(t, err)
	assert.EQ(t, string(got), "entry0")

	_, err = c.getOrCreate(10, func(w io.Writer) error {
		return errors.E("planned error")
	})
	assert.NotNil(t, err)
	rc, err = c.getOrCreate(10, func(w io.Writer) error {
		_, err2 := w.Write([]byte("entry10"))
		return err2
	})
	assert.NoError(t, err)
	got, err = ioutil.ReadAll(rc)
	assert.NoError(t, err)
	assert.EQ(t, string(got), "entry10")

	c.close()
}
