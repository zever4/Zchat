package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net"
	"sync"
	"time"

	"github.com/lib/pq"
)

const (
	connectTimeout   = 5 * time.Second
	operationTimeout = 30 * time.Second
)

// lib/pq cancels through a second connection. When the network drops both
// replies, that does not unblock the original socket. Close its transport on
// cancellation; database/sql must discard it rather than reuse a broken tx.
type connector struct {
	config pq.Config
	mu     sync.Mutex
	groups map[*socketDialer]struct{}
	closed bool
}

func open(dsn string) (*sql.DB, *connector, error) {
	cfg, err := pq.NewConfig(dsn)
	if err != nil {
		return nil, nil, err
	}
	cfg.ConnectTimeout = connectTimeout
	c := &connector{config: cfg, groups: make(map[*socketDialer]struct{})}
	db := sql.OpenDB(c)
	db.SetMaxOpenConns(25)
	// if we ever appear to have more than 1 instance of Go server, decrease MaxIdleConns
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return db, c, nil
}

// Open creates the server's bounded pool, without running migrations.
func Open(dsn string) (*sql.DB, error) {
	db, _, err := open(dsn)
	return db, err
}

type poolDriver struct{ pool *connector }

func (d *poolDriver) Open(string) (driver.Conn, error) { return d.pool.Connect(context.Background()) }
func (c *connector) Driver() driver.Driver             { return &poolDriver{pool: c} }

// Close first interrupts all transports, including idle prepared statements.
// sql.DB.Close alone may deallocate those statements before closing the socket.
func Close(db *sql.DB) error {
	if d, ok := db.Driver().(*poolDriver); ok {
		d.pool.close()
	}
	return db.Close()
}
func (c *connector) Connect(parent context.Context) (driver.Conn, error) {
	ctx, cancel := context.WithTimeout(parent, connectTimeout)
	defer cancel()
	d := &socketDialer{sockets: make(map[net.Conn]struct{})}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, context.Canceled
	}
	c.groups[d] = struct{}{}
	c.mu.Unlock()
	remove := func() { c.mu.Lock(); delete(c.groups, d); c.mu.Unlock() }
	stop := watch(ctx, d.close)
	defer stop()
	base, err := pq.NewConnectorConfig(c.config.Clone())
	if err == nil {
		base.Dialer(d)
		var conn driver.Conn
		conn, err = base.Connect(ctx)
		if err == nil && ctx.Err() == nil {
			return &connection{pgConnection: conn.(pgConnection), sockets: d, remove: remove}, nil
		}
	}
	d.close()
	remove()
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return nil, err
}
func (c *connector) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for d := range c.groups {
		d.close()
	}
}

type socketDialer struct {
	mu      sync.Mutex
	sockets map[net.Conn]struct{}
	closed  bool
}

func (d *socketDialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}
func (d *socketDialer) DialTimeout(network, addr string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.DialContext(ctx, network, addr)
}
func (d *socketDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		return nil, net.ErrClosed
	}
	conn, err := (&net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		conn.Close()
		return nil, net.ErrClosed
	}
	d.sockets[conn] = struct{}{}
	return conn, nil
}
func (d *socketDialer) close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for conn := range d.sockets {
		_ = conn.Close()
		delete(d.sockets, conn)
	}
}
func (d *socketDialer) valid() bool { d.mu.Lock(); defer d.mu.Unlock(); return !d.closed }

// Wait for a running callback before returning the socket to the pool: otherwise
// the old cancellation could close the next borrower's connection.
func watch(ctx context.Context, closeTransport func()) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { closeTransport(); close(done) })
	var once sync.Once
	return func() {
		once.Do(func() {
			if !stop() {
				<-done
			}
		})
	}
}

type pgConnection interface {
	driver.Conn
	driver.ExecerContext
	driver.QueryerContext
	driver.ConnPrepareContext
	driver.ConnBeginTx
	driver.Pinger
	driver.SessionResetter
	driver.Validator
	driver.NamedValueChecker
}
type connection struct {
	pgConnection
	sockets *socketDialer
	remove  func()
}

func (c *connection) Close() error  { c.sockets.close(); c.remove(); return nil }
func (c *connection) IsValid() bool { return c.sockets.valid() && c.pgConnection.IsValid() }
func (c *connection) ResetSession(ctx context.Context) error {
	if !c.sockets.valid() {
		return driver.ErrBadConn
	}
	return c.pgConnection.ResetSession(ctx)
}
func (c *connection) operation(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithTimeout(parent, operationTimeout)
	stop := watch(ctx, c.sockets.close)
	return ctx, func() { stop(); cancel() }
}
func contextError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
func (c *connection) Ping(parent context.Context) error {
	ctx, finish := c.operation(parent)
	defer finish()
	return contextError(ctx, c.pgConnection.Ping(ctx))
}
func (c *connection) ExecContext(parent context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	ctx, finish := c.operation(parent)
	defer finish()
	r, err := c.pgConnection.ExecContext(ctx, q, args)
	return r, contextError(ctx, err)
}
func (c *connection) QueryContext(parent context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	ctx, finish := c.operation(parent)
	r, err := c.pgConnection.QueryContext(ctx, q, args)
	if err = contextError(ctx, err); err != nil {
		finish()
		return nil, err
	}
	return &rows{pgRows: r.(pgRows), finish: finish}, nil
}
func (c *connection) BeginTx(parent context.Context, opts driver.TxOptions) (driver.Tx, error) {
	ctx, finish := c.operation(parent)
	tx, err := c.pgConnection.BeginTx(ctx, opts)
	if err = contextError(ctx, err); err != nil {
		finish()
		return nil, err
	}
	return &transaction{Tx: tx, finish: finish, ctx: ctx}, nil
}
func (c *connection) PrepareContext(parent context.Context, q string) (driver.Stmt, error) {
	ctx, finish := c.operation(parent)
	defer finish()
	s, err := c.pgConnection.PrepareContext(ctx, q)
	if err = contextError(ctx, err); err != nil {
		return nil, err
	}
	return &statement{pgStatement: s.(pgStatement), connection: c}, nil
}

type transaction struct {
	driver.Tx
	finish func()
	ctx    context.Context
}

func (t *transaction) Commit() error   { defer t.finish(); return contextError(t.ctx, t.Tx.Commit()) }
func (t *transaction) Rollback() error { defer t.finish(); return t.Tx.Rollback() }

type pgRows interface {
	driver.Rows
	driver.RowsNextResultSet
	driver.RowsColumnTypeDatabaseTypeName
	driver.RowsColumnTypeScanType
	driver.RowsColumnTypeLength
	driver.RowsColumnTypePrecisionScale
}
type rows struct {
	pgRows
	finish func()
}

func (r *rows) Close() error { defer r.finish(); return r.pgRows.Close() }

type pgStatement interface {
	driver.Stmt
	driver.StmtExecContext
	driver.StmtQueryContext
}
type statement struct {
	pgStatement
	connection *connection
}

func (s *statement) Close() error {
	if !s.connection.sockets.valid() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stop := watch(ctx, s.connection.sockets.close)
	defer stop()
	return contextError(ctx, s.pgStatement.Close())
}

func (s *statement) ExecContext(parent context.Context, args []driver.NamedValue) (driver.Result, error) {
	ctx, finish := s.connection.operation(parent)
	defer finish()
	r, err := s.pgStatement.ExecContext(ctx, args)
	return r, contextError(ctx, err)
}
func (s *statement) QueryContext(parent context.Context, args []driver.NamedValue) (driver.Rows, error) {
	ctx, finish := s.connection.operation(parent)
	r, err := s.pgStatement.QueryContext(ctx, args)
	if err = contextError(ctx, err); err != nil {
		finish()
		return nil, err
	}
	return &rows{pgRows: r.(pgRows), finish: finish}, nil
}
