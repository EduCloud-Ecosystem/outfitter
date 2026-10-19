package provider

import (
	"errors"
	"regexp"
)

// OfflineKubernetes is intentionally not a selectable CLI/backend transport.
const OfflineKubernetes = "kubernetes-offline"

// Placement binds one attempt to an exact target, never a mutable context name.
// A nil placement preserves the legacy Docker/local request encoding and hash.
type Placement struct {
	Backend      string `json:"backend"`
	ClusterID    string `json:"cluster_id"`
	Namespace    string `json:"namespace"`
	NamespaceUID string `json:"namespace_uid"`
	AttemptID    string `json:"attempt_id"`
}

type PlacementState struct {
	DispatchClaimed bool   `json:"dispatch_claimed"`
	ResourceUID     string `json:"resource_uid"`
	Stopped         bool   `json:"stopped"`
}

var clusterID = regexp.MustCompile(`^[a-f0-9]{64}$`)
var namespace = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

func (p Placement) Validate() error {
	if p.Backend != OfflineKubernetes || !clusterID.MatchString(p.ClusterID) || !namespace.MatchString(p.Namespace) || !ValidID(p.NamespaceUID) || !ValidID(p.AttemptID) {
		return errors.New("invalid disabled offline placement identity")
	}
	return nil
}

// PlacementVerifier must validate the complete configured target without effects.
type PlacementVerifier interface {
	ValidatePlacement(Placement) error
}

func CheckPlacement(p Describer, r Request) error {
	if r.Placement == nil {
		if p.Name() == OfflineKubernetes {
			return errors.New("offline Kubernetes requires an explicit placement")
		}
		return nil
	}
	if e := r.Placement.Validate(); e != nil {
		return e
	}
	v, ok := p.(PlacementVerifier)
	if !ok || p.Name() != r.Placement.Backend {
		return errors.New("record belongs to a different execution backend")
	}
	return v.ValidatePlacement(*r.Placement)
}

// ErrNotDispatched is only for adapter failures before its durable dispatch claim.
// Other creation errors must retain the existing ambiguous-create liability.
var ErrNotDispatched = errors.New("provider refused before dispatch")
