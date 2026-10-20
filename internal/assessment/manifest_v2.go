package assessment

import (
	"errors"
	"slices"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

func validProfileV2(p ExecutionProfile) bool {
	return p == (ExecutionProfile{ID: "cpu-python-r-bounded", Version: 1})
}

func DecodeManifestV2(raw []byte) (ManifestV2, error) {
	var m ManifestV2
	if decodeClosed(raw, &m, MaxManifestV2Bytes) != nil || validateManifestV2(m) != nil {
		return ManifestV2{}, errors.New("INVALID_MANIFEST_BINDING")
	}
	return m, nil
}

// This validates finite imported metadata only. In particular, the profile is
// not an admission decision, hardware qualification, or an enforced capability.
func validateManifestV2(m ManifestV2) error {
	invalid := errors.New("INVALID_MANIFEST_BINDING")
	if m.Schema != ManifestSchemaV2 || !validProfileV2(m.ExecutionProfile) || !provider.ValidID(m.AssessmentID) || !provider.ValidID(m.ProjectID) || !oneOf(m.Pathway, "research", "instructional") || !oneOf(m.DataClassification, "public", "synthetic") || !timestamp(m.ExpiresAt) {
		return invalid
	}
	w, h, d, c, q := m.Workload, m.Hardware, m.Design, m.Ceilings, m.Quality
	if !oneOf(w.RecipeID, "summary-python", "summary-r") || !digest(w.RecipeSHA256) || !provider.ValidID(w.EnvironmentID) || len(w.Image) > 512 || !immutableImage.MatchString(w.Image) || len(w.Stages) != 1 || w.Stages[0] != "analysis" || w.Dependencies == nil || len(w.Dependencies) != 0 {
		return invalid
	}
	if h.Kind != "local_docker" || !oneOf(h.Architecture, "arm64", "amd64") || !between(h.CPUCount, 1, 128) || !between(h.MemoryBytes, 1, 1<<50) || h.GPUCount != 0 || m.Measurement != (Method{"outfitter_normalized", 1}) {
		return invalid
	}
	// Check each factor before addition/multiplication. None of these scalars
	// controls an allocation, file size, loop over rows, or execution deadline.
	if c.CPUCountPerAttempt != 1 || c.CPUCountPerAttempt > h.CPUCount || !between(c.TimeoutSecondsPerAttempt, 1, 1800) || !between(c.MemoryBytes, 33554432, 1073741824) || c.MemoryBytes > h.MemoryBytes || !between(c.InputBytes, 1, 5000000000) || !between(c.InputRows, 1, 1000000) || !between(c.ScratchBytes, 2, 10000000000) || !between(c.OutputBytes, 1, 65536) || c.InputBytes+c.OutputBytes > c.ScratchBytes {
		return invalid
	}
	if d.SizeUnit != "rows" || len(d.FittingSizes) < 1 || len(d.FittingSizes) > 3 || !between(d.HeldOutSize, 1, c.InputRows) || !between(d.Repetitions, 1, 3) || int64(len(d.Seeds)) != d.Repetitions || !oneOf(d.Sampling, "synthetic_fixture", "random", "stratified", "convenience", "full") || !oneOf(d.Exclusions, "none", "declared") || !oneOf(d.Skew, "unknown", "declared", "none") {
		return invalid
	}
	previous := int64(0)
	for _, size := range d.FittingSizes {
		if !between(size, 1, c.InputRows) || size <= previous || size == d.HeldOutSize {
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
	if q.Check != "reviewed_summary" || q.AbsoluteToleranceNanounits != 1 || q.RelativeTolerancePartsPerTrillion != 1 || !between(q.MaximumFailurePartsPerMillion, 0, 1000000) || c.MaxAttempts != attempts || c.CPUSeconds != attempts*c.CPUCountPerAttempt*c.TimeoutSecondsPerAttempt {
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
		validSlot := slices.Contains(w.Stages, r.StageID) && between(r.Repetition, 1, d.Repetitions) && (r.SampleRole == "fitting" && slices.Contains(d.FittingSizes, r.SampleSize) || r.SampleRole == "held_out" && r.SampleSize == d.HeldOutSize)
		if !provider.ValidID(r.AttemptID) || !provider.ValidID(r.ReservationID) || !validSlot || ids[r.AttemptID] || reservationIDs[r.ReservationID] {
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

func validateBindingV2(m ManifestV2, b BindingV2) error {
	manifestHash, e := hashCanonical(m)
	r := ReservationBinding{b.AttemptID, b.StageID, b.SampleRole, b.SampleSize, b.Repetition, b.ReservationID}
	if e != nil || b.ManifestSchema != m.Schema || b.ExecutionProfile != m.ExecutionProfile || b.ManifestSHA256 != manifestHash || b.AssessmentID != m.AssessmentID || b.ProjectID != m.ProjectID || b.EnvironmentID != m.Workload.EnvironmentID || b.Image != m.Workload.Image || b.Hardware != m.Hardware || b.MeasurementMethod != m.Measurement || !slices.Contains(m.Reservations, r) {
		return errors.New("RECEIPT_BINDING_MISMATCH")
	}
	return nil
}
