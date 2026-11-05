package authz

import (
	"log/slog"
	"strings"
)

// fitting returns the view with only the segments that fit this component's resource definitions (P8.14; security
// 09a's EN08-EN10, security 07's FX01): a segment missing a declared dimension would otherwise leave that dimension
// unconstrained, as if it were all. A refused segment takes part in nothing; the rest of the view is used. Segments
// of resource types this component does not declare are kept: no lookup ever asks for them. One warning line per
// refused segment, on adoption only (not per request).
func fitting(v Shard, defs *Definitions, log *slog.Logger) Shard {
	if defs == nil {
		return v
	}
	subjects := make(map[string]SubjectEntry, len(v.Subjects))
	for p, e := range v.Subjects {
		var keep []Segment
		for _, s := range e.Segments {
			r, ok := defs.Resource(s.ResourceType)
			if !ok {
				keep = append(keep, s)
				continue
			}
			if err := s.Check(r); err != nil {
				log.Warn("a view segment does not fit this component's resource definition; it is not used",
					"event", "segment_refused", "resource_type", s.ResourceType, "operation", s.Operation,
					"reason", strings.TrimPrefix(err.Error(), "segment for "+s.Operation+": "))
				continue
			}
			keep = append(keep, s)
		}
		e.Segments = keep
		subjects[p] = e
	}
	v.Subjects = subjects
	return v
}
