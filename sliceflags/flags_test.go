package sliceflags_test

import (
	"testing"

	"github.com/grailbio/bigslice/sliceflags"
)

func TestProvider(t *testing.T) {
	local := &sliceflags.Local{}
	if got, want := local.Name(), "local"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	internal := &sliceflags.Internal{}
	if got, want := internal.Name(), "internal"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	ec2 := &sliceflags.EC2{}
	if got, want := ec2.Name(), "EC2"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if err := ec2.Set("x=y"); err == nil {
		t.Errorf("expected an error")
	}
	if err := ec2.Set("dataspace=122"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestFlags(t *testing.T) {
	tf := &sliceflags.Flags{}
	if err := tf.System.Set("local"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := tf.System.Set("local:an=option"); err == nil {
		t.Errorf("expected an error")
	}
	tf = &sliceflags.Flags{}
	if err := tf.System.Set("internal"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := tf.System.Set("internal:an=option"); err == nil {
		t.Errorf("expected an error")
	}
	tf = &sliceflags.Flags{}
	if err := tf.System.Set("ec2"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := tf.System.Set("ec2:an=option"); err == nil {
		t.Errorf("expected an error")
	}
	if err := tf.System.Set("ec2:dataspace=200,rootsize=10,securitygroup=sg-12345678"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got, want := tf.System.String(), "EC2:dataspace=200,rootsize=10,securitygroup=sg-12345678"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestProfiles(t *testing.T) {
	sliceflags.RegisterSystemProfile("my-app", "ec2:dataspace=500,profile=arn:iam:blah")
	tf := &sliceflags.Flags{}
	if err := tf.System.Set("my-app"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got, want := tf.System.String(), "EC2:dataspace=500,profile=arn:iam:blah"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	tf = &sliceflags.Flags{}
	if err := tf.System.Set("my-app:dataspace=1000"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got, want := tf.System.String(), "EC2:dataspace=500,profile=arn:iam:blah,dataspace=1000"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
