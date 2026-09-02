package cluster

import (
	"reflect"
	"testing"
)

func TestParseMembershipParsesSortedBrokerList(t *testing.T) {
	m, err := ParseMembership("2@localhost:9093,1@localhost:9092,3@localhost:9094", 2, "localhost", 9093)
	if err != nil {
		t.Fatalf("ParseMembership returned error: %v", err)
	}

	want := []Broker{
		{ID: 1, Host: "localhost", Port: 9092},
		{ID: 2, Host: "localhost", Port: 9093},
		{ID: 3, Host: "localhost", Port: 9094},
	}
	if got := m.All(); !reflect.DeepEqual(got, want) {
		t.Fatalf("All() = %#v, want %#v", got, want)
	}
	if got := m.Self(); got != want[1] {
		t.Fatalf("Self() = %#v, want %#v", got, want[1])
	}
	if got, ok := m.Get(3); !ok || got != want[2] {
		t.Fatalf("Get(3) = %#v, %t; want %#v, true", got, ok, want[2])
	}
}

func TestParseMembershipEmptyPeersBuildsSingleBrokerMembership(t *testing.T) {
	m, err := ParseMembership("", 1, "localhost", 9092)
	if err != nil {
		t.Fatalf("ParseMembership returned error: %v", err)
	}

	want := []Broker{{ID: 1, Host: "localhost", Port: 9092}}
	if got := m.All(); !reflect.DeepEqual(got, want) {
		t.Fatalf("All() = %#v, want %#v", got, want)
	}
	if got := m.Self(); got != want[0] {
		t.Fatalf("Self() = %#v, want %#v", got, want[0])
	}
}

func TestParseMembershipRejectsMissingSelf(t *testing.T) {
	if _, err := ParseMembership("2@localhost:9093,3@localhost:9094", 1, "localhost", 9092); err == nil {
		t.Fatal("ParseMembership returned nil error for missing self")
	}
}

func TestParseMembershipRejectsMalformedPeer(t *testing.T) {
	tests := []string{
		"1localhost:9092",
		"bad@localhost:9092",
		"1@localhost",
		"1@localhost:not-a-port",
	}

	for _, peers := range tests {
		t.Run(peers, func(t *testing.T) {
			if _, err := ParseMembership(peers, 1, "localhost", 9092); err == nil {
				t.Fatal("ParseMembership returned nil error for malformed peers")
			}
		})
	}
}
