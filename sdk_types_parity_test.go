package conformancetests

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestSDKTypesStayInSync prevents one platform from silently publishing a
// different Beak-facing sdk contract. The release workspace contains all SDK
// siblings; a standalone checkout without them cannot perform this gate.
func TestSDKTypesStayInSync(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve conformance test path")
	}
	root := filepath.Dir(filepath.Dir(currentFile))
	paths := []string{
		"beak-agent-dingtalk/sdk/types.go",
		"beak-agent-lark/sdk/types.go",
		"beak-agent-weixin/sdk/types.go",
		"truewatch-beak-agent-channel-slack/sdk/types.go",
		"truewatch-beak-agent-channel-teams/sdk/types.go",
		"truewatch-beak-agent-channel-telegram/sdk/types.go",
	}

	canonicalPath := filepath.Join(root, paths[0])
	canonical, err := os.ReadFile(canonicalPath)
	if os.IsNotExist(err) {
		t.Skip("SDK sibling repositories are not available")
	}
	if err != nil {
		t.Fatalf("read canonical sdk types: %v", err)
	}
	for _, relative := range paths[1:] {
		relative := relative
		t.Run(relative, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(root, relative))
			if err != nil {
				t.Fatalf("read sdk types: %v", err)
			}
			if !bytes.Equal(data, canonical) {
				t.Fatalf("%s differs from %s; update every SDK public contract together", relative, paths[0])
			}
		})
	}
}
