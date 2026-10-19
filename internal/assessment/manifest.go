package assessment

import (
	"errors"
	"slices"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

func oneOf(value string, allowed ...string) bool { return slices.Contains(allowed, value) }
func between(value, low, high int64) bool        { return value >= low && value <= high }
func timestamp(value string) bool {
	t, e := time.Parse("2006-01-02T15:04:05Z", value)
	return e == nil && t.Format("2006-01-02T15:04:05Z") == value
}

func DecodeManifest(raw []byte) (Manifest, error) {
	var m Manifest
	if decodeClosed(raw, &m, MaxReceiptBytes) != nil || validateManifest(m) != nil {
		return Manifest{}, errors.New("INVALID_MANIFEST_BINDING")
	}
	return m, nil
}

// This is structural input/binding validation, never Waypoint's active software
// catalog, membership, reservation admission or workload approval decision.
func validateManifest(m Manifest) error {
	invalid := errors.New("INVALID_MANIFEST_BINDING")
	if m.Schema != "educloud.benchmark-manifest/v1" || !provider.ValidID(m.AssessmentID) || !provider.ValidID(m.ProjectID) || !oneOf(m.Pathway, "research", "instructional") || !oneOf(m.DataClassification, "public", "synthetic") || !timestamp(m.ExpiresAt) {
		return invalid
	}
	w, h, d, c, q := m.Workload, m.Hardware, m.Design, m.Ceilings, m.Quality
	if !oneOf(w.RecipeID, "summary-python", "summary-r") || !digest(w.RecipeSHA256) || !provider.ValidID(w.EnvironmentID) || len(w.Image) > 512 || !immutableImage.MatchString(w.Image) || len(w.Stages) != 1 || w.Stages[0] != "analysis" || w.Dependencies == nil || len(w.Dependencies) != 0 {
		return invalid
	}
	if h.Kind != "local_docker" || !oneOf(h.Architecture, "arm64", "amd64") || !between(h.CPUCount, 1, 128) || !between(h.MemoryBytes, 1, 1<<50) || h.GPUCount != 0 || m.Measurement != (Method{"outfitter_normalized", 1}) {
		return invalid
	}
	if d.SizeUnit != "rows" || len(d.FittingSizes) < 1 || len(d.FittingSizes) > 3 || !between(d.HeldOutSize, 1, 1000) || !between(d.Repetitions, 1, 3) || int64(len(d.Seeds)) != d.Repetitions || !oneOf(d.Sampling, "synthetic_fixture", "random", "stratified", "convenience", "full") || !oneOf(d.Exclusions, "none", "declared") || !oneOf(d.Skew, "unknown", "declared", "none") {
		return invalid
	}
	previous := int64(0)
	for _, size := range d.FittingSizes {
		if !between(size, 1, 1000) || size <= previous || size == d.HeldOutSize {
			return invalid
		}
		previous = size
	}
	seeds := map[int64]bool{}
	for _, seed := range d.Seeds {
		if !between(seed, 0, 1<<32-1) || seeds[seed] {
			return invalid
		}
		seeds[seed] = true
	}
	smaller := len(d.FittingSizes) != 3 || d.Repetitions != 3
	if smaller != (d.SmallerDesign != nil) || smaller && (!d.SmallerDesign.ApprovedByUser || !oneOf(d.SmallerDesign.Reason, "allowance_limited", "capacity_limited", "operator_selected")) {
		return invalid
	}
	attempts := int64(len(d.FittingSizes)+1) * d.Repetitions
	if q.Check != "reviewed_summary" || q.AbsoluteToleranceNanounits != 1 || q.RelativeTolerancePartsPerTrillion != 1 || !between(q.MaximumFailurePartsPerMillion, 0, 1000000) || c.MaxAttempts != attempts || c.CPUSeconds != attempts*30 || c.MemoryBytes != 268435456 || c.MemoryBytes > h.MemoryBytes || !between(c.ScratchBytes, 1, 128<<20) || !between(c.OutputBytes, 1, 65536) || c.TimeoutSecondsPerAttempt != 30 {
		return invalid
	}
	if (m.Pathway == "research") != (m.Instruction == nil) {
		return invalid
	}
	if i := m.Instruction; i != nil && (!between(i.Enrollment, 1, 100000) || !between(i.SimultaneousSessions, 1, i.Enrollment) || !between(i.DeadlinePeakSessions, 1, i.Enrollment) || !between(i.AssignmentCount, 1, 10000) || !between(i.PerStudentStorageBytes, 1, 1<<50)) {
		return invalid
	}
	if m.Reservations == nil || int64(len(m.Reservations)) > c.MaxAttempts {
		return invalid
	}
	ids, reservationIDs := map[string]bool{}, map[string]bool{}
	slots := map[ReservationBinding]bool{}
	for _, r := range m.Reservations {
		if !provider.ValidID(r.AttemptID) || !provider.ValidID(r.ReservationID) || !validSlot(m, r) || ids[r.AttemptID] || reservationIDs[r.ReservationID] {
			return invalid
		}
		ids[r.AttemptID], reservationIDs[r.ReservationID] = true, true
		r.AttemptID, r.ReservationID = "", ""
		if slots[r] {
			return invalid
		}
		slots[r] = true
	}
	return nil
}

func validSlot(m Manifest, slot ReservationBinding) bool {
	return slices.Contains(m.Workload.Stages, slot.StageID) && between(slot.Repetition, 1, m.Design.Repetitions) && (slot.SampleRole == "fitting" && slices.Contains(m.Design.FittingSizes, slot.SampleSize) || slot.SampleRole == "held_out" && slot.SampleSize == m.Design.HeldOutSize)
}

func validateBinding(m Manifest, b Binding) error {
	manifestHash, e := hashCanonical(m)
	r := ReservationBinding{b.AttemptID, b.StageID, b.SampleRole, b.SampleSize, b.Repetition, b.ReservationID}
	if e != nil || b.ManifestSHA256 != manifestHash || b.AssessmentID != m.AssessmentID || b.ProjectID != m.ProjectID || b.EnvironmentID != m.Workload.EnvironmentID || b.Image != m.Workload.Image || b.Hardware != m.Hardware || b.MeasurementMethod != m.Measurement || !slices.Contains(m.Reservations, r) {
		return errors.New("RECEIPT_BINDING_MISMATCH")
	}
	return nil
}
