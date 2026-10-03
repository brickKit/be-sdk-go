package config

import "bytes"

// SecretText turns a secret file's content into its text value (P2.9, vectors secret_text): exactly
// one trailing LF or CRLF is removed, nothing else. An empty result is not set: CONFIG_MISSING when
// the key is required.
func SecretText(key string, content []byte, required bool) (text string, set bool, err error) {
	c := content
	if b, ok := bytes.CutSuffix(c, []byte("\r\n")); ok {
		c = b
	} else {
		c = bytes.TrimSuffix(c, []byte("\n"))
	}
	if len(c) == 0 {
		if required {
			return "", false, newErr(ReasonMissing, key, "secret file is empty")
		}
		return "", false, nil
	}
	return string(c), true, nil
}
