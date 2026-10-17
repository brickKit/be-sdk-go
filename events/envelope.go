package events

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"
)

// Epoch is a database epoch: (deployment recovery generation, write timeline), compared in that order (P6.7).
type Epoch struct{ Generation, Timeline int64 }

func (e Epoch) String() string {
	return strconv.FormatInt(e.Generation, 10) + "." + strconv.FormatInt(e.Timeline, 10)
}

// Less orders epochs.
func (e Epoch) Less(o Epoch) bool {
	return e.Generation < o.Generation || (e.Generation == o.Generation && e.Timeline < o.Timeline)
}

// Header names (P6.13): CloudEvents binary mode with the ce- prefix.
const (
	HeaderID          = "ce-id"
	HeaderType        = "ce-type"
	HeaderSource      = "ce-source"
	HeaderSubject     = "ce-subject"
	HeaderStream      = "ce-bestream"
	HeaderPartition   = "ce-bepartition"
	HeaderImpl        = "ce-beimpl"
	HeaderEpoch       = "ce-beepoch"
	HeaderSeq         = "ce-beseq"
	HeaderVersion     = "ce-beversion"
	HeaderProjection  = "ce-beprojection"
	HeaderDigest      = "ce-bedigest"
	HeaderHops        = "ce-behops"
	HeaderCausation   = "ce-becausation"
	HeaderLegalEntity = "ce-belegalentity"
	HeaderSubjects    = "ce-besubjects"
	HeaderClaim       = "ce-beclaim"
)

// Row is one outbox row as the forwarder publishes it.
type Row struct {
	Ord         int64
	EventID     string
	Stream      string
	Partition   int
	Type        string
	OccurredAt  time.Time
	RecordID    string
	Version     int64
	Projection  Projection
	LegalEntity string
	Subjects    []string
	Causation   string
	Hops        int
	Trace       map[string]string
	Payload     []byte
	Claim       bool
	Digest      string
	Seq         int64
	Epoch       Epoch
}

// Digest is the row digest of P6.4: SHA-256 of the event ID, a zero byte and the payload, lower-case hex.
func Digest(eventID string, payload []byte) string {
	h := sha256.New()
	h.Write([]byte(eventID))
	h.Write([]byte{0})
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

// Headers renders the envelope of a numbered row (P6.13).
func (r Row) Headers(source, impl string) map[string]string {
	h := map[string]string{"ce-specversion": "1.0", HeaderID: r.EventID, HeaderSource: "urn:be:" + source,
		HeaderType: r.Type, "ce-time": r.OccurredAt.UTC().Format("2006-01-02T15:04:05.000Z"), HeaderSubject: r.RecordID,
		"ce-datacontenttype": "application/json", HeaderStream: r.Stream, HeaderPartition: strconv.Itoa(r.Partition),
		HeaderImpl: impl, HeaderEpoch: r.Epoch.String(), HeaderSeq: strconv.FormatInt(r.Seq, 10),
		HeaderVersion: strconv.FormatInt(r.Version, 10), HeaderProjection: string(r.Projection), HeaderDigest: r.Digest,
		HeaderHops: strconv.Itoa(r.Hops)}
	if r.Causation != "" {
		h[HeaderCausation] = r.Causation
	}
	if r.LegalEntity != "" {
		h[HeaderLegalEntity] = r.LegalEntity
	}
	if r.Subjects != nil {
		b, _ := json.Marshal(r.Subjects)
		h[HeaderSubjects] = string(b)
	}
	if r.Claim {
		h[HeaderClaim] = "true"
	}
	for k, v := range r.Trace {
		h["ce-"+k] = v
	}
	return h
}
