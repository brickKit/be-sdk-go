package authz

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/jackc/pgx/v5"
)

// Plan is a compiled list authorization (P8.17): a predicate to AND with the request's own filter, its arguments,
// and one boolean expression per segment whose results give each row's readable fields.
type Plan struct {
	Where string
	Args  []any
	Hits  []string
	sets  [][]string
}

// Fields returns a row's readable fields from its hit bitmap (never a secret field).
func (p *Plan) Fields(hits []bool) []string {
	seen := map[string]bool{}
	for i, h := range hits {
		if h && i < len(p.sets) {
			for _, f := range p.sets[i] {
				seen[f] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for f := range seen {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

type compiler struct {
	r         *Resource
	alias     string
	principal string
	args      []any
	base      int
}

func (c *compiler) arg(v any) string {
	c.args = append(c.args, v)
	return "$" + strconv.Itoa(c.base+len(c.args))
}

func (c *compiler) col(name string) string {
	if c.alias == "" {
		return pgx.Identifier{name}.Sanitize()
	}
	return pgx.Identifier{c.alias, name}.Sanitize()
}

func likeEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

func (c *compiler) atom(dim string, a Atom) (string, error) {
	d := c.r.Dimensions[dim]
	col := c.col(d.Column)
	switch a.Kind {
	case "all":
		return "", nil
	case "in":
		if len(a.Values) == 0 {
			return "FALSE", nil
		}
		return fmt.Sprintf("%s = ANY(%s)", col, c.arg(a.Values)), nil
	case "self":
		return fmt.Sprintf("%s = %s", col, c.arg(c.principal)), nil
	case "tree":
		var parts []string
		for _, it := range a.Items {
			if it.Mode == "subtree" {
				parts = append(parts, fmt.Sprintf("(%s = %s OR %s LIKE %s ESCAPE '\\')", col, c.arg(it.Path), col,
					c.arg(likeEscape(it.Path)+"/%")))
			} else {
				parts = append(parts, fmt.Sprintf("%s = %s", col, c.arg(it.Path)))
			}
		}
		if len(parts) == 0 {
			return "FALSE", nil
		}
		return "(" + strings.Join(parts, " OR ") + ")", nil
	case "interval":
		parts := []string{col + " IS NOT NULL", fmt.Sprintf("%s = %s", c.col(d.CurrencyColumn), c.arg(a.Currency))}
		if a.Lo != nil {
			op := ">"
			if *a.LoClosed {
				op = ">="
			}
			parts = append(parts, fmt.Sprintf("%s %s %s::numeric", col, op, c.arg(*a.Lo)))
		}
		if a.Hi != nil {
			op := "<"
			if *a.HiClosed {
				op = "<="
			}
			parts = append(parts, fmt.Sprintf("%s %s %s::numeric", col, op, c.arg(*a.Hi)))
		}
		return "(" + strings.Join(parts, " AND ") + ")", nil
	}
	return "", errs.New(errs.ReasonQueryNotCompletable, "a segment cannot be compiled into the query").
		Wrap(fmt.Errorf("atom kind %q on %s", a.Kind, dim))
}

func (c *compiler) segment(s Segment) (string, error) {
	dims := make([]string, 0, len(s.Atoms))
	for dim := range s.Atoms {
		dims = append(dims, dim)
	}
	sort.Strings(dims) // a stable text for the same segment
	var parts []string
	for _, dim := range dims {
		p, err := c.atom(dim, s.Atoms[dim])
		if err != nil {
			return "", err
		}
		if p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) == 0 {
		return "TRUE", nil
	}
	return "(" + strings.Join(parts, " AND ") + ")", nil
}

func (c *compiler) union(segs []Segment) ([]string, error) {
	var preds []string
	for _, s := range segs {
		p, err := c.segment(s)
		if err != nil {
			return nil, err
		}
		preds = append(preds, p)
	}
	return preds, nil
}

func orAll(preds []string) string {
	if len(preds) == 0 {
		return "FALSE"
	}
	return "(" + strings.Join(preds, " OR ") + ")"
}

// Compile builds the list plan for operation op over table alias (empty: unqualified columns); argument
// placeholders start after argBase. Visible = the view operation's segments; for another operation the list is
// visible AND permitted, and the fields come from that operation's segments.
func Compile(r *Resource, segs []Segment, op, principal string, at time.Time, alias string, argBase int) (*Plan, error) {
	c := &compiler{r: r, alias: alias, principal: principal, base: argBase}
	active := func(op string) []Segment {
		var out []Segment
		for _, s := range segs {
			if s.Operation == op && s.ActiveAt(at) {
				out = append(out, s)
			}
		}
		return out
	}
	view := active(r.ViewOperation)
	viewPreds, err := c.union(view)
	if err != nil {
		return nil, err
	}
	p := &Plan{Where: orAll(viewPreds)}
	fieldSegs, hitPreds := view, viewPreds
	if op != r.ViewOperation {
		opSegs := active(op)
		opPreds, err := c.union(opSegs)
		if err != nil {
			return nil, err
		}
		p.Where = p.Where + " AND " + orAll(opPreds)
		fieldSegs, hitPreds = opSegs, opPreds
	}
	p.Hits = hitPreds
	for _, s := range fieldSegs {
		var fs []string
		for _, f := range s.ReadFields {
			if fd, ok := r.Fields[f]; ok && fd.Class != Secret {
				fs = append(fs, f)
			}
		}
		p.sets = append(p.sets, fs)
	}
	p.Args = c.args
	return p, nil
}
