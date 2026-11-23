package domain

// Path is a server-generated relative URL, never a client filesystem path.
type Attachment struct {
	Path, Name, MIME, Kind string
	ChatID, OwnerID, Size  int64
}
