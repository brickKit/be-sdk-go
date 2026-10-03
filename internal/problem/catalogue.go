package problem

import (
	"fmt"
	"io/fs"
	"strings"
	"sync"

	beprotocol "github.com/brickKit/be-protocol"
	"google.golang.org/grpc/codes"
	"gopkg.in/yaml.v3"
)

// reasonEntry is one row of a reason catalogue (schemas/errors-yaml.schema.json).
type reasonEntry struct {
	code    codes.Code
	params  []string
	title   map[string]string
	message map[string]string
}

// Catalogue holds the reason catalogues the runtime renders text from: the reserved reasons of domain
// be (always) plus the component's own contracts/errors.yaml and any family catalogue it adds (P4.4).
// It is immutable after the Add calls made at start, so readers need no lock.
type Catalogue struct {
	domains map[string]map[string]reasonEntry
}

type yamlCatalogue struct {
	Domain  string `yaml:"domain"`
	Reasons []struct {
		Reason  string            `yaml:"reason"`
		Code    string            `yaml:"code"`
		Params  []string          `yaml:"params"`
		Title   map[string]string `yaml:"title"`
		Message map[string]string `yaml:"message"`
	} `yaml:"reasons"`
}

// beCatalogue parses schemas/errors-be.yaml of the pinned protocol once; it is read-only data.
var beCatalogue = sync.OnceValue(func() *Catalogue {
	b, err := fs.ReadFile(beprotocol.FS, "schemas/errors-be.yaml")
	if err != nil {
		panic("be-protocol errors-be.yaml missing: " + err.Error())
	}
	c := &Catalogue{domains: map[string]map[string]reasonEntry{}}
	if err := c.add(b, nil); err != nil {
		panic("be-protocol errors-be.yaml invalid: " + err.Error())
	}
	return c
})

// NewCatalogue returns a catalogue holding the reserved reasons of domain be.
func NewCatalogue() *Catalogue {
	c := &Catalogue{domains: map[string]map[string]reasonEntry{}}
	for d, rs := range beCatalogue().domains {
		c.domains[d] = rs
	}
	return c
}

// Add parses one catalogue file (contracts/errors.yaml format) and adds its domain. A component may
// not redefine the be domain or use a reserved reason name in its own domain (P4.7).
func (c *Catalogue) Add(yamlBytes []byte) error { return c.add(yamlBytes, ValidateReason) }

func (c *Catalogue) add(yamlBytes []byte, check func(reason, domain string) error) error {
	var y yamlCatalogue
	if err := yaml.Unmarshal(yamlBytes, &y); err != nil {
		return fmt.Errorf("errors catalogue: %w", err)
	}
	if y.Domain == "" {
		return fmt.Errorf("errors catalogue: no domain")
	}
	if y.Domain == DomainBe && check != nil {
		return fmt.Errorf("errors catalogue: the be domain is the protocol's own")
	}
	if _, exists := c.domains[y.Domain]; exists {
		return fmt.Errorf("errors catalogue: domain %s added twice", y.Domain)
	}
	rs := make(map[string]reasonEntry, len(y.Reasons))
	for _, r := range y.Reasons {
		if check != nil {
			if err := check(r.Reason, y.Domain); err != nil {
				return fmt.Errorf("errors catalogue %s: %w", y.Domain, err)
			}
		}
		code, err := ParseCode(r.Code)
		if err != nil {
			return fmt.Errorf("errors catalogue %s reason %s: %w", y.Domain, r.Reason, err)
		}
		rs[r.Reason] = reasonEntry{code: code, params: r.Params, title: r.Title, message: r.Message}
	}
	c.domains[y.Domain] = rs
	return nil
}

func (c *Catalogue) lookup(domain, reason string) (reasonEntry, bool) {
	r, ok := c.domains[domain][reason]
	return r, ok
}

// Has reports whether domain:reason is catalogued.
func (c *Catalogue) Has(domain, reason string) bool {
	_, ok := c.lookup(domain, reason)
	return ok
}

// Text renders the title and detail of an error in a BCP 47 locale (P4.1: DEFAULT_LOCALE). A
// catalogued reason renders its templates with the metadata; an uncatalogued one (a dependency's
// reason relayed as it is) keeps its own detail, its reason standing in for the title.
func (c *Catalogue) Text(e *Error, locale string) (title, detail string) {
	lang := language(locale)
	if r, ok := c.lookup(e.Domain, e.Reason); ok {
		return pick(r.title, lang), render(pick(r.message, lang), e.Metadata)
	}
	title = e.Reason
	if title == "" {
		title = CodeName(e.Code)
	}
	detail = e.Detail
	if detail == "" {
		detail = title
	}
	return title, detail
}

// language reduces a BCP 47 tag to the catalogue's language keys (zh, en).
func language(locale string) string {
	l := strings.ToLower(locale)
	if i := strings.IndexAny(l, "-_"); i > 0 {
		l = l[:i]
	}
	if l == "" {
		return "zh"
	}
	return l
}

func pick(texts map[string]string, lang string) string {
	if s, ok := texts[lang]; ok {
		return s
	}
	if s, ok := texts["en"]; ok {
		return s
	}
	for _, s := range texts {
		return s
	}
	return ""
}

func render(template string, meta map[string]string) string {
	if !strings.Contains(template, "{") {
		return template
	}
	pairs := make([]string, 0, 2*len(meta))
	for k, v := range meta {
		pairs = append(pairs, "{"+k+"}", v)
	}
	return strings.NewReplacer(pairs...).Replace(template)
}
