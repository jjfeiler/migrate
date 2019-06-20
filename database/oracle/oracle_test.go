package oracle

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"io"
	"log"
	"testing"
	"github.com/jjfeiler/dktest"
	"time"
)

// database driver specific imports
import (
	"gopkg.in/goracle.v2"
)

// module imports, see go.mod
import (
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var (
	opts = dktest.Options{
		PortRequired: true,
		ReadyFunc:    isReady,
		Timeout:      time.Minute * 10,
		ShmSize:      1073741824,
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "localhost:5000/goracle-migrate-test:1.0.1", Options: opts},
	}
	connectionLogged = false
)

const defaultPort = 1521

func oracleConnectionString(host, port, username, password, dbname string, sysdba bool) string {
	params := &goracle.ConnectionParams{
		Username:     username,
		Password:     password,
		SID:          host + ":" + port + "/" + dbname,
		MinSessions:  1,
		MaxSessions:  4,
		IsSysDBA:     sysdba,
	}

	if !connectionLogged {
		//		connectionLogged = true
		log.Printf("oracle connection string %s", params.StringWithPassword())
	}
	return params.StringWithPassword()
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.FirstPort()
	if err != nil {
		return false
	}

	db, err := sql.Open("goracle", oracleConnectionString(ip, port, "goracle", "goracle", "migratetest", false))
	if err != nil {
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()
	if err = db.PingContext(ctx); err != nil {
		switch err {
		case sqldriver.ErrBadConn, io.EOF:
			return false
		default:
			log.Println(err)
		}
		return false
	}

	return true
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := oracleConnectionString(ip, port, "goracle", "goracle", "migratetest", false)
		p := &GOracle{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		log.Printf("connected")
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "migratetest", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m, []byte("SELECT 1 FROM DUAL"))

		// check ensureVersionTable
		if err := d.(*GOracle).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
		// check again
		if err := d.(*GOracle).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
	})
}
