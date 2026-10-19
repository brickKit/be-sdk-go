package httpx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenAPIProblems(t *testing.T) {
	doc := []byte(`openapi: 3.1.0
paths:
  /api/demo/items:
    get: {x-operation: demo.item.read}
  /api/demo/items/{id}:
    parameters: [{name: id, in: path}]
    get: {x-operation: demo.item.read}
    patch: {x-operation: demo.item.edit}
  /api/demo/open:
    get: {}
`)
	ok := []Route{{Method: "GET", Path: "/api/demo/items", Operation: "demo.item.read"},
		{Method: "GET", Path: "/api/demo/items/:id", Operation: "demo.item.read"},
		{Method: "PATCH", Path: "/api/demo/items/:id", Operation: "demo.item.edit"},
		{Method: "GET", Path: "/api/demo/open", Public: true}}
	assert.Empty(t, OpenAPIProblems(doc, ok))
	bad := []Route{{Method: "GET", Path: "/api/demo/items", Operation: "demo.item.list"},
		{Method: "DELETE", Path: "/api/demo/items/:id", Operation: "demo.item.edit"},
		{Method: "GET", Path: "/api/demo/open", Public: true}}
	assert.Equal(t, []string{
		"DELETE /api/demo/items/{id}: registered but not in the OpenAPI document",
		"GET /api/demo/items/{id}: in the OpenAPI document but not registered",
		`GET /api/demo/items: x-operation "demo.item.read" in the document, "demo.item.list" registered`,
		"PATCH /api/demo/items/{id}: in the OpenAPI document but not registered",
	}, OpenAPIProblems(doc, bad))
}
