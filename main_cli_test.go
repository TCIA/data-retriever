package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMainCLIHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	argsIndex := -1
	for index, value := range os.Args {
		if value == "--" {
			argsIndex = index
			break
		}
	}
	if argsIndex == -1 || argsIndex+1 >= len(os.Args) {
		os.Exit(2)
	}

	os.Args = append([]string{os.Args[0]}, os.Args[argsIndex+1:]...)
	main()
	os.Exit(0)
}

func runCLIHelper(t *testing.T, args ...string) (string, error) {
	t.Helper()
	commandArgs := append([]string{"-test.run=TestMainCLIHelperProcess", "--"}, args...)
	cmd := exec.Command(os.Args[0], commandArgs...)
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func TestCLIHelpExitsWithUsage(t *testing.T) {
	output, err := runCLIHelper(t, "--help")
	if err == nil {
		t.Fatalf("expected non-zero exit for --help")
	}
	if !strings.Contains(output, "show help information") {
		t.Fatalf("expected help output, got: %s", output)
	}
}

func TestCLIVersionExitsZero(t *testing.T) {
	output, err := runCLIHelper(t, "--version")
	if err != nil {
		t.Fatalf("expected zero exit for --version, got err=%v output=%s", err, output)
	}
}

func TestCLIIncompatibleNoDecompressExitsNonZero(t *testing.T) {
	output, err := runCLIHelper(t, "--input", "in.tcia", "--no-decompress")
	if err == nil {
		t.Fatalf("expected non-zero exit for incompatible flags")
	}
	if !strings.Contains(output, "incompatible") {
		t.Fatalf("expected incompatibility output, got: %s", output)
	}
}

func TestCLIInvalidFlagExitsNonZero(t *testing.T) {
	output, err := runCLIHelper(t, "--input", "in.tcia", "--definitely-invalid")
	if err == nil {
		t.Fatalf("expected non-zero exit for invalid flag")
	}
	if !strings.Contains(strings.ToLower(output), "unknown") {
		t.Fatalf("expected unknown option output, got: %s", output)
	}
}
