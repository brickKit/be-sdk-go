package httpx

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

// A download is saved, never run: whatever its content, the headers make the browser save it (assembly protocol 12).
func TestADownloadIsSavedNeverRun(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	g, _ := gin.CreateTestContext(w)
	g.Request = httptest.NewRequest("GET", "/", nil)
	c := &Context{gin: g}
	body := "<svg onload=alert(1)/>"
	assert.NoError(t, c.Download("", "合同 1.svg", int64(len(body)), strings.NewReader(body)))
	assert.Equal(t, body, w.Body.String())
	assert.Equal(t, "application/octet-stream", w.Header().Get("Content-Type"))
	assert.Equal(t, "attachment; filename*=UTF-8''%E5%90%88%E5%90%8C%201.svg", w.Header().Get("Content-Disposition"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	assert.Contains(t, w.Header().Get("Content-Security-Policy"), "sandbox")
	assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
}
