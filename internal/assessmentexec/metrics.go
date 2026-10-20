package assessmentexec

import (
	"reflect"
	"strings"

	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

var metricUnits = []string{"nanoseconds", "nanoseconds", "nanoseconds", "nanoseconds", "nanoseconds", "nanoseconds", "nanoseconds", "nanoseconds", "nanoseconds", "nanoseconds", "bytes", "bytes", "bytes", "bytes", "bytes", "bytes", "parts_per_million", "count"}

func UnknownMetrics() MetricSet {
	var out MetricSet
	v := reflect.ValueOf(&out).Elem()
	for i, unit := range metricUnits {
		m := Metric{Availability: "unknown", Unit: unit, Scope: "whole_attempt", Source: "none"}
		if i == 0 {
			m.Scope = "sandbox"
		}
		if i == 1 || i == 15 || i == 16 {
			m.Availability = "unsupported"
		}
		v.Field(i).Set(reflect.ValueOf(m))
	}
	return out
}
func UnknownMeasurements() Measurements {
	return Measurements{UnknownMetrics(), UnknownMetrics(), UnknownMetrics()}
}
func (m MetricSet) Validate() error {
	v := reflect.ValueOf(m)
	for i, unit := range metricUnits {
		x := v.Field(i).Interface().(Metric)
		if x.Unit != unit {
			return ErrInvalid
		}
		if i == 0 {
			if !one(x.Scope, "process", "sandbox") {
				return ErrInvalid
			}
		} else if i == 6 {
			if !one(x.Scope, "execution_only", "whole_attempt") {
				return ErrInvalid
			}
		} else if !one(x.Scope, "whole_attempt", "process", "sandbox") {
			return ErrInvalid
		}
		if (i == 1 || i == 15 || i == 16) && x.Availability != "unsupported" {
			return ErrInvalid
		}
		switch x.Availability {
		case "available":
			if x.Value == nil || !owneddata.Uint(*x.Value) || x.Source != "unverified_collector" {
				return ErrInvalid
			}
		case "unknown", "unsupported":
			if x.Value != nil || x.Source != "none" {
				return ErrInvalid
			}
		default:
			return ErrInvalid
		}
	}
	return nil
}
func (m Measurements) Validate() error {
	for _, s := range []MetricSet{m.Workload, m.Checker, m.Attempt} {
		if s.Validate() != nil {
			return ErrInvalid
		}
	}
	return nil
}
func (m MetricSet) Map() map[string]Metric {
	out := map[string]Metric{}
	v := reflect.ValueOf(m)
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		out[strings.Split(t.Field(i).Tag.Get("json"), ",")[0]] = v.Field(i).Interface().(Metric)
	}
	return out
}
