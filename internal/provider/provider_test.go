package provider

import (
	"strings"
	"testing"
	"time"
)

func validRequest() Request {
	return Request{SchemaVersion: 1, RequestID: "run", ProjectID: "project", Image: "sha256:" + strings.Repeat("a", 64), Command: []string{"python", "/recipe/recipe.py"}, InputDir: "/input", RecipeDir: "/recipe", CPUs: 1, MemoryMB: 256, PIDsLimit: 64, TimeoutSeconds: 30, MaxOutputBytes: 65536, Isolation: "reviewed-local", AllowSupervisedReaper: true}
}
func TestAdmissionRefusesUnsafeOrUnknown(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Request)
	}{
		{"tag", func(r *Request) { r.Image = "python:latest" }},
		{"shell-in-image", func(r *Request) { r.Image = "$(touch /tmp/bad)" }},
		{"strong-isolation", func(r *Request) { r.Isolation = "T-strong" }},
		{"default-expiry", func(r *Request) { r.AllowSupervisedReaper = false }},
		{"no-lifetime", func(r *Request) { r.TimeoutSeconds = 0 }},
		{"no-budget", func(r *Request) { r.CPUs = 0 }},
		{"infinite-output", func(r *Request) { r.MaxOutputBytes = 1 << 30 }},
		{"mount-option-injection", func(r *Request) { r.InputDir = "/tmp/x,readonly=false" }},
		{"relative-input", func(r *Request) { r.InputDir = "../input" }},
		{"invalid-id", func(r *Request) { r.RequestID = "../other" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validRequest()
			tt.change(&r)
			if r.Validate() == nil {
				t.Fatal("unsafe request admitted")
			}
		})
	}
}
func TestReservationMustBeCurrentAndSufficient(t *testing.T) {
	r := validRequest()
	for _, v := range []Reservation{{}, {ID: "run", Amount: 29, Deadline: time.Now().Add(time.Hour), Owner: "x", Name: "x"}, {ID: "run", Amount: 30, Deadline: time.Now().Add(-time.Second), Owner: "x", Name: "x"}} {
		if v.Validate(r) == nil {
			t.Fatal("invalid reservation accepted")
		}
	}
}
func TestUnknownOwnershipRefused(t *testing.T) {
	r := Record{Receipt: Receipt{RequestID: "a"}, Owner: "owner"}
	if CheckOwnership(r, State{Exists: true, Owner: "someone", RequestID: "a"}) == nil {
		t.Fatal("foreign resource accepted")
	}
}
