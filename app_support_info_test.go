package main

import "testing"

func TestBuildSupportInfoUsesFallbacks(t *testing.T) {
	got := buildSupportInfo("", "", "")

	if got.AppVersion != "dev" {
		t.Fatalf("AppVersion = %q, want %q", got.AppVersion, "dev")
	}
	if got.OSPlatform != "unknown" {
		t.Fatalf("OSPlatform = %q, want %q", got.OSPlatform, "unknown")
	}
	if got.OSVersion != "Unknown" {
		t.Fatalf("OSVersion = %q, want %q", got.OSVersion, "Unknown")
	}
}

func TestBuildSupportInfoTrimsValues(t *testing.T) {
	got := buildSupportInfo(" v1.2.3 ", " linux ", " Ubuntu 24.04 ")

	if got.AppVersion != "v1.2.3" {
		t.Fatalf("AppVersion = %q, want %q", got.AppVersion, "v1.2.3")
	}
	if got.OSPlatform != "linux" {
		t.Fatalf("OSPlatform = %q, want %q", got.OSPlatform, "linux")
	}
	if got.OSVersion != "Ubuntu 24.04" {
		t.Fatalf("OSVersion = %q, want %q", got.OSVersion, "Ubuntu 24.04")
	}
}

func TestParseLinuxOSReleasePrefersPrettyName(t *testing.T) {
	input := `NAME="Ubuntu"
VERSION="24.04.2 LTS (Noble Numbat)"
ID=ubuntu
ID_LIKE=debian
PRETTY_NAME="Ubuntu 24.04.2 LTS"
VERSION_ID="24.04"`

	got := parseLinuxOSRelease(input)
	want := "Ubuntu 24.04.2 LTS"
	if got != want {
		t.Fatalf("parseLinuxOSRelease() = %q, want %q", got, want)
	}
}

func TestParseLinuxOSReleaseFallsBackToNameVersionID(t *testing.T) {
	input := `NAME="Fedora Linux"
VERSION_ID="40"`

	got := parseLinuxOSRelease(input)
	want := "Fedora Linux 40"
	if got != want {
		t.Fatalf("parseLinuxOSRelease() = %q, want %q", got, want)
	}
}
