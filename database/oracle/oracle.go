package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jjfeiler/migrate/v4"
	"github.com/jjfeiler/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	"io"
	"io/ioutil"
	"log"
	nurl "net/url"
)

import (
	_ "gopkg.in/goracle.v2"
)

func init() {
	gor := GOracle{}
	database.Register("oracle", &gor)
	database.Register("goracle", &gor)
}

var DefaultMigrationsTable = "schema_migrations"

var (
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
	ErrNoSchema       = fmt.Errorf("no schema")
	ErrDatabaseDirty  = fmt.Errorf("database is dirty")
)

type Config struct {
	MigrationsTable string
	DatabaseName    string
	SchemaName      string
}

type GOracle struct {
	conn       *sql.Conn
	db         *sql.DB
	lockhandle string
	isLocked   bool

	config *Config
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	query := "select SYS_CONTEXT('USERENV', 'SERVICE_NAME') from dual"
	var databaseName string
	if err := instance.QueryRow(query).Scan(&databaseName); err != nil {
		log.Printf("oracle.WithInstance() error getting database name")
		return nil, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if len(databaseName) == 0 {
		return nil, ErrNoDatabaseName
	}

	config.DatabaseName = databaseName

	query = "select SYS_CONTEXT( 'USERENV', 'CURRENT_SCHEMA' ) from dual"
	var schemaName string
	if err := instance.QueryRow(query).Scan(&schemaName); err != nil {
		log.Printf("oracle.WithInstance() error getting schema name")
		return nil, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if len(schemaName) == 0 {
		return nil, ErrNoSchema
	}

	config.SchemaName = schemaName

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	conn, err := instance.Conn(context.Background())

	if err != nil {
		log.Printf("oracle.WithInstance() error getting conn")
		return nil, err
	}

	gor := &GOracle{
		conn:   conn,
		db:     instance,
		config: config,
	}

	if err := gor.ensureVersionTable(); err != nil {
		log.Printf("oracle.WithInstance() error calling ensureVersionTable %s", err)
		return nil, err
	}

	return gor, nil
}

func (g *GOracle) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("goracle", migrate.FilterCustomQuery(purl).String())
	if err != nil {
		return nil, err
	}

	migrationsTable := purl.Query().Get("x-migrations-table")

	gor, err := WithInstance(db, &Config{
		DatabaseName:    purl.Path,
		MigrationsTable: migrationsTable,
	})
	if err != nil {
		return nil, err
	}

	return gor, nil
}

func (g *GOracle) Close() error {
	connErr := g.conn.Close()
	dbErr := g.db.Close()
	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

func (g *GOracle) Lock() error {
	if g.isLocked {
		return database.ErrLocked
	}

	if g.lockhandle == "" {
		lockName := fmt.Sprintf("%s:%s", g.config.DatabaseName, g.config.MigrationsTable)

		lockAllocateQuery := `BEGIN DBMS_LOCK.ALLOCATE_UNIQUE(:lockname, :lockhandle); END;`
		_, err := g.conn.ExecContext(context.Background(), lockAllocateQuery,
			sql.Named("lockname", lockName),
			sql.Named("lockhandle", sql.Out{Dest: &g.lockhandle}))
		if err != nil {
			errstr := fmt.Sprintf("error creating lock with lockName %s %s", lockName, err)
			return &database.Error{OrigErr: err, Err: errstr, Query: []byte(lockAllocateQuery)}
		}
	}

	lockQuery := `BEGIN :result := DBMS_LOCK.REQUEST(:lockHandle); END;`
	var result int
	_, err := g.conn.ExecContext(context.Background(),
		lockQuery, sql.Named("result", &result), sql.Named("lockHandle", g.lockhandle))
	if err != nil {
		errstr := fmt.Sprintf("error requesting lock with handle %s %s", g.lockhandle, err)
		return &database.Error{OrigErr: err, Err: errstr, Query: []byte(lockQuery)}
	} else {
		resstr := fmt.Sprintf("DBMS_LOCK.REQUEST() call returned %d", result)
		if result == 0 {
			g.isLocked = true
			return nil
		} else {
			return &database.Error{Err: resstr, Query: []byte(lockQuery)}
		}
	}
}

func (g *GOracle) Unlock() error {
	if !g.isLocked {
		return nil
	}

	if g.lockhandle == "" {
		log.Printf("lockhandle is nil")
		return nil
	}

	lockQuery := `BEGIN :result := DBMS_LOCK.RELEASE(:lockHandle); END;`
	var result int
	_, err := g.conn.ExecContext(context.Background(),
		lockQuery, sql.Named("lockHandle", g.lockhandle), sql.Named("result", &result))
	if err != nil {
		errstr := fmt.Sprintf("error releasing lock with handle %s %s", g.lockhandle, err)
		return &database.Error{OrigErr: err, Err: errstr, Query: []byte(lockQuery)}
	} else {
		if result == 0 {
			g.isLocked = false
			return nil
		} else {
			errstr := fmt.Sprintf("DBMS_LOCK.RELEASE() call returned %d", result)
			return &database.Error{Err: errstr, Query: []byte(lockQuery)}
		}
	}
}

func (g *GOracle) Run(migration io.Reader) error {
	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	query := string(migr[:])
	if _, err := g.conn.ExecContext(context.Background(), query); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

func (g *GOracle) SetVersion(version int, dirty bool) error {
	tx, err := g.conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := fmt.Sprintf("TRUNCATE TABLE %s", g.config.MigrationsTable)
	if _, err := tx.Exec(query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if version >= 0 {
		query = fmt.Sprintf("INSERT INTO %s (version, dirty) VALUES (:1, :2)", g.config.MigrationsTable)
		if _, err := tx.Exec(query, version, asChar(dirty)); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = multierror.Append(err, errRollback)
			}
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

func (g *GOracle) Version() (version int, dirty bool, err error) {
	query := fmt.Sprintf("SELECT version, dirty FROM %s FETCH NEXT 1 ROWS ONLY", g.config.MigrationsTable)
	var dirtyStr string
	err = g.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirtyStr)

	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		dirty, ok := readBool(dirtyStr);
		if !ok {
			errstr := fmt.Sprintf("Unexpected value in dirty column %s", dirtyStr)
			return 0, false, &database.Error{Err: errstr, Query: []byte(query)}
		}
		return version, dirty, nil
	}
}

func (g *GOracle) Drop() error {
	query := `SELECT TABLE_NAME FROM USER_TABLES`
	tables, err := g.conn.QueryContext(context.Background(), query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	// delete one table after another
	tableNames := make([]string, 0)
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			return err
		}
		if len(tableName) > 0 {
			tableNames = append(tableNames, tableName)
		}
	}

	if len(tableNames) > 0 {
		// delete one by one ...
		for _, t := range tableNames {
			query = "DROP TABLE " + t + " CASCADE CONSTRAINTS"
			if _, err := g.conn.ExecContext(context.Background(), query); err != nil {
				log.Printf("error dropping table %s", t)
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the GOracle type.
func (g *GOracle) ensureVersionTable() (err error) {
	if err = g.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := g.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	query := fmt.Sprintf(`
BEGIN
	execute immediate 'create table %s (version number(19) not null primary key, dirty char(1) not null)';
EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE = -955 THEN
        NULL; -- suppresses ORA-00955 exception
      ELSE
        RAISE;
      END IF;
END;`, g.config.MigrationsTable)

	if _, err = g.conn.ExecContext(context.Background(), query); err != nil {
		log.Printf("error creating migrations table %s", err)
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

// Returns the bool value of the input.
// Oracle recommends using char(1) to store booleans with Y/N as the values
// The 2nd return value indicates if the input was a valid bool value
func readBool(input string) (value bool, valid bool) {
	switch input {
	case "Y", "y":
		return true, true
	case "N", "n":
		return false, true
	}

	// Not a valid bool value
	return false, false
}

func asChar(value bool) string {
	if value {
		return "Y"
	}
	return "N"
}
