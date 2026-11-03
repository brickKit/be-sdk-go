package projection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/internal/sourcepb"
	"google.golang.org/grpc"
)

// CallTimeout bounds one pull, read or snapshot page: calls need a deadline, and a page is at most 500 rows (a
// caller's earlier deadline wins).
const CallTimeout = 10 * time.Second

// RecordClient is a Source over the source's standard RecordService (P9.30-P9.33): Snapshot pages by its page token,
// Read asks for at most events.RecordPage IDs at a time; claim-checked contents are fetched and checked (P6.14).
type RecordClient struct {
	Conn   grpc.ClientConnInterface // the source's system surface
	Stream string
	Claims events.ClaimReader // for contents over 64 KiB; nil: such a content fails the read
}

// content is a record's data: as sent, or the claimed object checked against the reference's size and digest.
func (c RecordClient) content(ctx context.Context, id string, payload []byte, claim bool) (json.RawMessage, error) {
	if !claim {
		return payload, nil
	}
	var ref struct{ Claim events.ClaimRef }
	if err := json.Unmarshal(payload, &ref); err != nil || ref.Claim.URI == "" {
		return nil, fmt.Errorf("record %s of %s: the claim reference cannot be read", id, c.Stream)
	}
	if c.Claims == nil {
		return nil, fmt.Errorf("record %s of %s: a claim-checked content and no object storage reader", id, c.Stream)
	}
	data, err := c.Claims.Get(ctx, ref.Claim.URI)
	if err != nil {
		return nil, fmt.Errorf("record %s of %s: fetching the claim: %w", id, c.Stream, err)
	}
	sum := sha256.Sum256(data)
	if len(data) != ref.Claim.Size || hex.EncodeToString(sum[:]) != ref.Claim.SHA256 {
		return nil, fmt.Errorf("record %s of %s: the claimed object does not match its size and digest", id, c.Stream)
	}
	return data, nil
}

// Snapshot implements Source.
func (c RecordClient) Snapshot(ctx context.Context, cursor string) ([]Record, string, Identity, error) {
	out := new(sourcepb.SnapshotResponse)
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	if err := c.Conn.Invoke(ctx, sourcepb.RecordService_Snapshot_FullMethodName,
		&sourcepb.SnapshotRequest{Stream: c.Stream, PageToken: cursor}, out, calls.Idempotent()); err != nil {
		return nil, "", Identity{}, err
	}
	recs := make([]Record, 0, len(out.GetRecords()))
	for _, r := range out.GetRecords() {
		data, err := c.content(ctx, r.GetRecordId(), r.GetPayload(), r.GetClaim())
		if err != nil {
			return nil, "", Identity{}, err
		}
		recs = append(recs, Record{ID: r.GetRecordId(), Version: r.GetVersion(), Data: data})
	}
	return recs, out.GetNextPageToken(), declared(out.GetSource()), nil
}

// Read implements Source; more IDs than one read takes go in several reads, which must declare the same source.
func (c RecordClient) Read(ctx context.Context, ids []string) ([]ReadResult, Identity, error) {
	var all []ReadResult
	var from Identity
	for start := 0; start < len(ids); start += events.RecordPage {
		part := ids[start:min(start+events.RecordPage, len(ids))]
		out := new(sourcepb.ReadResponse)
		cctx, cancel := context.WithTimeout(ctx, CallTimeout)
		err := c.Conn.Invoke(cctx, sourcepb.RecordService_Read_FullMethodName,
			&sourcepb.ReadRequest{Stream: c.Stream, RecordIds: part}, out, calls.Idempotent())
		cancel()
		if err != nil {
			return nil, Identity{}, err
		}
		d := declared(out.GetSource())
		if start > 0 && d != from {
			return nil, Identity{}, fmt.Errorf("%w: the reads of one batch declared %+v and %+v", ErrOtherSource, from, d)
		}
		from = d
		if len(out.GetResults()) != len(part) {
			return nil, Identity{}, fmt.Errorf("%s: the read answered %d of %d IDs", c.Stream, len(out.GetResults()), len(part))
		}
		for i, r := range out.GetResults() {
			if r.GetRecordId() != part[i] {
				return nil, Identity{}, fmt.Errorf("%s: the read answered %s where %s was asked", c.Stream, r.GetRecordId(), part[i])
			}
			res := ReadResult{ID: r.GetRecordId(), Version: r.GetVersion()}
			switch r.GetState() {
			case sourcepb.ReadState_READ_STATE_EXISTS:
				data, err := c.content(ctx, r.GetRecordId(), r.GetPayload(), r.GetClaim())
				if err != nil {
					return nil, Identity{}, err
				}
				res.State, res.Data = Exists, data
			case sourcepb.ReadState_READ_STATE_TOMBSTONE:
				res.State = Gone
			case sourcepb.ReadState_READ_STATE_PURGED:
				res.State = Purged
			default:
				return nil, Identity{}, fmt.Errorf("%s: record %s read as %v", c.Stream, r.GetRecordId(), r.GetState())
			}
			all = append(all, res)
		}
	}
	return all, from, nil
}

// PullClient is a Puller over the source's standard WatermarkService (P9.13-P9.18).
type PullClient struct {
	Conn grpc.ClientConnInterface // the source's system surface
}

// Pull implements Puller.
func (c PullClient) Pull(ctx context.Context, req *sourcepb.PullRequest) (*sourcepb.PullResponse, error) {
	out := new(sourcepb.PullResponse)
	ctx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	return out, c.Conn.Invoke(ctx, sourcepb.WatermarkService_Pull_FullMethodName, req, out, calls.Idempotent())
}
