package ledger

import (
	"errors"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

func validatePlacements(d Data) error {
	seen := map[provider.Placement]bool{}
	for _, r := range d.Runs {
		p, s := r.Request.Placement, r.PlacementState
		if p == nil {
			if s != nil {
				return errors.New("unbound record has placement state")
			}
			continue
		}
		if (d.Version != 5 && d.Version != 6 && d.Version != 7) || p.Validate() != nil || s == nil || seen[*p] {
			return errors.New("invalid or repeated placement attempt")
		}
		seen[*p] = true
		if _, collision := d.NativeRuns[r.RequestID]; collision {
			return errors.New("placement/native request collision")
		}
		if s.DispatchClaimed && !r.CreateAttempted || s.ResourceUID != "" && (!s.DispatchClaimed || !provider.ValidID(s.ResourceUID)) || s.Stopped && s.ResourceUID == "" {
			return errors.New("invalid placement dispatch/resource state")
		}
		if r.CleanupVerified && s.DispatchClaimed && !s.Stopped {
			return errors.New("Pod absence alone cannot verify workload cleanup")
		}
	}
	if d.Version == 5 && len(seen) == 0 {
		return errors.New("offline authority lost its placement records")
	}
	return nil
}

type placementGuard struct {
	hash, owner, name, project string
	created, deadline          time.Time
	reserved, charged          int64
	state                      provider.PlacementState
}

func placementGuards(d Data) map[string]placementGuard {
	result := map[string]placementGuard{}
	for id, r := range d.Runs {
		if r.Request.Placement != nil && r.PlacementState != nil {
			result[id] = placementGuard{r.PayloadHash, r.Owner, r.Name, r.ProjectID, r.CreatedAt, r.Deadline, r.Compute.Reserved, r.Compute.Charged, *r.PlacementState}
		}
	}
	return result
}

func preservePlacements(before map[string]placementGuard, d Data) error {
	for id, old := range before {
		r, ok := d.Runs[id]
		if !ok || r.Request.Placement == nil || r.PlacementState == nil || (d.Version != 5 && d.Version != 6 && d.Version != 7) || r.Request.Hash() != old.hash || r.PayloadHash != old.hash || r.Owner != old.owner || r.Name != old.name || r.ProjectID != old.project || !r.CreatedAt.Equal(old.created) || !r.Deadline.Equal(old.deadline) || r.Compute.Reserved != old.reserved || r.Compute.Charged < old.charged {
			return errors.New("offline placement identity/reservation cannot be removed, rebound or refunded")
		}
		s := r.PlacementState
		if old.state.DispatchClaimed && !s.DispatchClaimed || old.state.ResourceUID != "" && s.ResourceUID != old.state.ResourceUID || old.state.Stopped && !s.Stopped {
			return errors.New("offline dispatch/UID evidence cannot be reset")
		}
	}
	return nil
}
