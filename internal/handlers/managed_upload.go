package handlers

import (
	"context"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"

	"github.com/zever4/Zchat/internal/uploadstorage"
)

type storageContextKey struct{}
type preparedUploadKey struct{}
type preparedUpload struct {
	ticket      *uploadstorage.Upload
	file        multipart.File
	header      *multipart.FileHeader
	field       string
	reauthorize func(context.Context) (func(), error)
}

var errUploadReauth = errors.New("upload final authorization failed")

var errStorageUnavailable = errors.New("managed upload storage is not configured")

func requestStorage(r *http.Request) *uploadstorage.Manager {
	m, _ := r.Context().Value(storageContextKey{}).(*uploadstorage.Manager)
	return m
}
func requestUpload(r *http.Request) *preparedUpload {
	p, _ := r.Context().Value(preparedUploadKey{}).(*preparedUpload)
	return p
}

// Limit header/field read-ahead BEFORE mime/multipart can allocate large header
// maps. File bytes alone use the streaming mode. At most one file is admitted.
type multipartBudgetReader struct {
	ctx       context.Context
	src       io.Reader
	nonfile   bool
	remaining int64
}

func (r *multipartBudgetReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.nonfile {
		if r.remaining == 0 {
			return 0, errUploadTooLarge
		}
		if int64(len(p)) > r.remaining {
			p = p[:r.remaining]
		}
	}
	n, err := r.src.Read(p)
	if r.nonfile {
		r.remaining -= int64(n)
	}
	return n, err
}

func parseManagedUpload(w http.ResponseWriter, r *http.Request, ticket *uploadstorage.Upload) (*preparedUpload, error) {
	contentType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "multipart/form-data" || params["boundary"] == "" || len(params["boundary"]) > 70 {
		return nil, errors.New("expected multipart/form-data")
	}
	field := "avatar"
	if r.Pattern == "POST /api/files/upload" {
		field = "file"
	} else if r.Pattern == "POST /api/media/upload" {
		field = "media"
	}
	limit, known := uploadLimit(r.Pattern)
	if !known {
		return nil, errStorageUnavailable
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	reader := &multipartBudgetReader{ctx: r.Context(), src: r.Body, nonfile: true, remaining: multipartOverhead}
	parts := multipart.NewReader(reader, params["boundary"])
	values := make(url.Values)
	p := &preparedUpload{ticket: ticket, field: field}
	count := 0
	for {
		reader.nonfile = true
		part, err := parts.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		count++
		if count > 32 {
			return nil, errUploadTooLarge
		}
		name := part.FormName()
		if name == "" || len(name) > 128 {
			return nil, errors.New("invalid multipart field")
		}
		if part.FileName() != "" {
			if name != field || p.file != nil {
				return nil, errors.New("exactly one expected file is required")
			}
			reader.nonfile = false
			file, n, err := ticket.Spool(part)
			if err != nil {
				return nil, err
			}
			p.file = file
			p.header = &multipart.FileHeader{Filename: part.FileName(), Header: part.Header, Size: n}
		} else {
			data, err := io.ReadAll(io.LimitReader(part, multipartOverhead+1))
			if err != nil {
				return nil, err
			}
			if int64(len(data)) > multipartOverhead {
				return nil, errUploadTooLarge
			}
			values.Add(name, string(data))
		}
	}
	if p.file == nil {
		return nil, errors.New("file is required")
	}
	// Consume only the bounded suffix, outside the session barrier, so a valid
	// body reaches EOF and net/http can reuse the connection without a later drain.
	reader.nonfile = true
	if _, err = io.CopyBuffer(io.Discard, reader, make([]byte, 32<<10)); err != nil {
		return nil, err
	}
	r.MultipartForm = &multipart.Form{Value: values, File: make(map[string][]*multipart.FileHeader)}
	r.PostForm = values
	r.Form = make(url.Values, len(values))
	for k, v := range values {
		r.Form[k] = v
	}
	for k, v := range r.URL.Query() {
		r.Form[k] = append(r.Form[k], v...)
	}
	return p, nil
}
func discardUpload(r *http.Request, url string) error {
	p := requestUpload(r)
	if p == nil || !strings.HasPrefix(url, "/uploads/") {
		return errStorageUnavailable
	}
	return p.ticket.Discard(strings.TrimPrefix(url, "/uploads/"))
}
