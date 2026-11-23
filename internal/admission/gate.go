// Package admission bounds admitted database-bound work, not socket lifetimes.
// Acquisition never waits or starts a goroutine. Protocols own retry/backoff.
package admission

import (
	"context"
	"errors"
	"sync"
)

type Class uint8

const (
	Data Class = iota
	Control
	Login
)

func (c Class) String() string {
	switch c {
	case Data:
		return "data"
	case Control:
		return "control"
	case Login:
		return "login"
	default:
		return "invalid"
	}
}

var ErrBusy = errors.New("request admission exhausted")
var ErrInvalid = errors.New("invalid request admission scope")

type Config struct {
	Total, Data, Login, PerAccount, PerControl int
}

func DefaultConfig() Config {
	return Config{Total: 20, Data: 16, Login: 2, PerAccount: 4, PerControl: 1}
}

type Snapshot struct {
	Active, Data, Control, Login, Accounts int
	Peak, PeakData                         int
	Accepted, Rejected                     uint64
}
type account struct{ data, control int }
type Gate struct {
	mu       sync.Mutex
	config   Config
	state    Snapshot
	accounts map[int64]account
	observe  func(class, result string)
}

func New(config Config, observe func(class, result string)) *Gate {
	if config.Total < 1 || config.Data < 1 || config.Data >= config.Total || config.Login < 1 ||
		config.Login > config.Total-config.Data || config.PerAccount < 1 || config.PerAccount > config.Data || config.PerControl < 1 || config.PerControl > config.Total-config.Data {
		panic("invalid admission configuration")
	}
	return &Gate{config: config, accounts: make(map[int64]account), observe: observe}
}

// Data/Control IDs must come from a verified signature or an existing server-owned
// connection. They group work only: database session/device validation still follows.
// Login uses zero because its email/name is untrusted and must not grow a key map.
func (g *Gate) Try(ctx context.Context, id int64, class Class) (func(), error) {
	if class > Login || class == Login && id != 0 || class != Login && id <= 0 {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	a := g.accounts[id]
	reason := ""
	switch {
	case g.state.Active >= g.config.Total:
		reason = "busy_global"
	case class == Data && g.state.Data >= g.config.Data:
		reason = "busy_data"
	case class == Login && g.state.Login >= g.config.Login:
		reason = "busy_login"
	case class == Data && a.data >= g.config.PerAccount, class == Control && a.control >= g.config.PerControl:
		reason = "busy_account"
	}
	if reason != "" {
		g.state.Rejected++
		g.mu.Unlock()
		g.event(class, reason)
		return nil, ErrBusy
	}
	g.state.Active++
	g.state.Accepted++
	switch class {
	case Data:
		g.state.Data++
		a.data++
	case Control:
		g.state.Control++
		a.control++
	case Login:
		g.state.Login++
	}
	if id != 0 {
		g.accounts[id] = a
	}
	g.state.Accounts = len(g.accounts)
	g.state.Peak = max(g.state.Peak, g.state.Active)
	g.state.PeakData = max(g.state.PeakData, g.state.Data)
	g.mu.Unlock()
	g.event(class, "accepted")
	return sync.OnceFunc(func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.state.Active--
		a := g.accounts[id]
		switch class {
		case Data:
			g.state.Data--
			a.data--
		case Control:
			g.state.Control--
			a.control--
		case Login:
			g.state.Login--
		}
		if id != 0 {
			if a.data == 0 && a.control == 0 {
				delete(g.accounts, id)
			} else {
				g.accounts[id] = a
			}
		}
		g.state.Accounts = len(g.accounts)
	}), nil
}

func (g *Gate) event(class Class, result string) {
	if g.observe != nil {
		g.observe(class.String(), result)
	}
}
func (g *Gate) Snapshot() Snapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state
}
