package owneddata

import (
	"bytes"
	"os"
	"testing"
)

func TestOwnedReplyGoldenFrames(t *testing.T) {
	for _, name := range []string{"status-reply", "ready-ack", "missing-error", "invalid-error", "disabled-error"} {
		t.Run(name, func(t *testing.T) {
			raw, e := os.ReadFile("testdata/" + name + ".json")
			if e != nil {
				t.Fatal(e)
			}
			var value any
			switch name {
			case "status-reply":
				var x Status
				if ClosedJSON(raw, &x, MaxReplyBytes) != nil {
					t.Fatal("status decode")
				}
				value = x
			case "ready-ack":
				var x Ready
				if ClosedJSON(raw, &x, MaxReplyBytes) != nil {
					t.Fatal("ready decode")
				}
				value = x
			default:
				var x ErrorReply
				if ClosedJSON(raw, &x, MaxReplyBytes) != nil {
					t.Fatal("error decode")
				}
				value = x
			}
			got, e := CanonicalReply(value)
			if e != nil {
				t.Fatal(e)
			}
			want, e := os.ReadFile("testdata/" + name + ".canonical.json")
			if e != nil {
				t.Fatal(e)
			}
			want = append(want, '\n')
			if !bytes.Equal(got, want) {
				t.Fatal("closed reply framing differs from shared fixture")
			}
			for _, bad := range [][]byte{bytes.Replace(raw, []byte(`"schema"`), []byte(`"Schema"`), 1), bytes.Replace(raw, []byte(`"schema":`), []byte(`"extra":true,"schema":`), 1), bytes.Replace(raw, []byte(`"schema":`), []byte(`"schema":"duplicate","schema":`), 1)} {
				var target any
				switch name {
				case "status-reply":
					target = new(Status)
				case "ready-ack":
					target = new(Ready)
				default:
					target = new(ErrorReply)
				}
				if ClosedJSON(bad, target, MaxReplyBytes) == nil {
					t.Fatal("open/aliased/duplicate reply accepted")
				}
			}
		})
	}
}
