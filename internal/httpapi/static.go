package httpapi

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

// dist holds the built web UI. Run `npm --prefix web run build` to regenerate
// it (Vite writes straight into this directory).
//
//go:embed all:dist
var dist embed.FS

// staticHandler serves the embedded UI, falling back to index.html for any
// path that isn't a real asset so client-side navigation works.
func staticHandler() http.Handler {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clean := strings.TrimPrefix(r.URL.Path, "/")
		if clean == "" {
			clean = "index.html"
		}
		if _, err := fs.Stat(sub, clean); err != nil {
			r = r.Clone(r.Context())
			r.URL.Path = "/"
		}
		fileServer.ServeHTTP(w, r)
	})
}
