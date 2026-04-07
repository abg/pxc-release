package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"net/http"
)

func basicAuthMiddleware(username, password string, hmacKey []byte, next http.Handler) http.Handler {
	usernameHash := computeHMAC(hmacKey, username)
	passwordHash := computeHMAC(hmacKey, password)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok {
			writeUnauthorized(w)
			return
		}

		uMatch := hmac.Equal(computeHMAC(hmacKey, u), usernameHash)
		pMatch := hmac.Equal(computeHMAC(hmacKey, p), passwordHash)

		if !uMatch || !pMatch {
			writeUnauthorized(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func computeHMAC(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(value))
	return mac.Sum(nil)
}

func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Basic realm="Authorization Required"`)
	http.Error(w, "Not Authorized", http.StatusUnauthorized)
}
