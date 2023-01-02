// Test alist filesystem interface
package alist_test

import (
	"testing"

	"github.com/rclone/rclone/backend/alist"
	"github.com/rclone/rclone/fstest/fstests"
)

// TestIntegration runs integration tests against the remote
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestAlist:",
		NilObject:  (*alist.Object)(nil),
	})
}
