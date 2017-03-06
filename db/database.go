package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/op/go-logging"
)

// Database represents the database.
type Database struct {
	connection  *sql.DB
	transaction *sql.Tx
}

var (
	log = logging.MustGetLogger("bkp")
)

func GetDatabasePath() string {
	return filepath.Join(filepath.Base(os.Args[0]), ".db")
}

// Remove deletes any existing database, returning whether it was found and,
// if so, whether any error occurred along the way.
func Remove(path string) (bool, error) {
	var err error
	filepath := filepath.Join(path, GetDatabasePath())

	_, err = os.Stat(filepath)
	switch {
	case err == nil:
		log.Debugf("file %s exists\n", filepath)
		if err = os.Remove(filepath); err != nil {
			log.Errorf("error removing file: %v\n", err)
		}
		return true, err
	case os.IsNotExist(err):
		log.Debugf("file %s does not exist\n", filepath)
		return false, nil
	default:
		log.Errorf("error checking if file exists already: %v\n", err)
		return false, nil
	}
}

// Open ensures that a Database exists as "<executable>.db" under the given path;
// if it does not exist yet, it creates it, otherwise it's simply opened.
func Open(path string) (*Database, error) {

	var err error

	db := Database{}

	// load the database driver
	if db.connection, err = sql.Open("sqlite3", filepath.Join(path, GetDatabasePath())); err != nil {
		log.Errorf("error loading database driver: %v\n", err)
		return nil, err
	}
	log.Debug("database driver loaded")

	// actually check that the database connection is OK
	if err = db.connection.Ping(); err != nil {
		log.Errorf("error connecting to database: %v\n", err)
		defer db.connection.Close()
		return nil, err
	}
	log.Debug("database connection open")

	return &db, nil
}

// Execute runs the given query against the current database.
func (db *Database) Execute(query string, args ...interface{}) (sql.Result, error) {
	return db.connection.Exec(query, args)
}

// OpenTransaction starts a new transaction on this connection.
func (db *Database) OpenTransaction() (*Database, error) {

	if db.transaction != nil {
		log.Errorf("there is an active transaction already")
		return db, fmt.Errorf("there is an active transaction already")
	}
	var err error
	db.transaction, err = db.connection.Begin()
	return db, err
}

// Commit commits the existing transaction.
func (db *Database) Commit() (*Database, error) {
	var err error
	if db.transaction != nil {
		err = db.transaction.Commit()
		db.transaction = nil
	} else {
		log.Errorf("there is an transaction to commit")
		err = fmt.Errorf("there is no transaction to commit")
	}
	return db, err
}

// Rollback rolls back an existing transaction.
func (db *Database) Rollback() (*Database, error) {
	var err error
	if db.transaction != nil {
		err = db.transaction.Rollback()
		db.transaction = nil
	} else {
		log.Errorf("there is an no transaction to rollback")
		err = fmt.Errorf("there is an no transaction to rollback")
	}
	return db, err
}

// Prepare prepares a statement; if there's a transction in place, the statement
// is prepared against it, otherwise it is prepared against the DB connection.
func (db *Database) Prepare(query string) (*sql.Stmt, error) {
	if db.transaction != nil {
		log.Debug("preparing statement on existing transaction")
		return db.transaction.Prepare(query)
	}
	log.Debug("preparing statement on raw connection")
	return db.connection.Prepare(query)
}

// Close releases the connection to the Database on which it is called.
func (db *Database) Close() error {
	if db.transaction != nil {
		_ = db.transaction.Commit()
		db.transaction = nil
	}
	err := db.connection.Close()
	db.connection = nil
	return err
}
