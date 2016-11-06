package db

import (
	"database/sql"
	"os"
	"path/filepath"

	"fmt"

	"github.com/dihedron/go-log"
)

// Database represents the database.
type Database struct {
	connection  *sql.DB
	transaction *sql.Tx
}

var path = "database.db"

// Remove deletes any existing database, returning whether it was found and,
// if so, whether any error occurred along the way.
func Remove(path string) (bool, error) {
	var err error
	filepath := filepath.Join(path, "database.db")

	_, err = os.Stat(filepath)
	switch {
	case err == nil:
		log.DBG.Printf("file %s exists\n", filepath)
		if err = os.Remove(filepath); err != nil {
			log.ERR.Printf("error removing file: %v\n", err)
		}
		return true, err
	case os.IsNotExist(err):
		log.DBG.Printf("file %s does not exist\n", filepath)
		return false, nil
	default:
		log.ERR.Printf("error checking if file exists already: %v\n", err)
		return false, nil
	}
}

// Open ensures that a Database exists as "database.sb" under thegiven path;
// if it does not exist yet, it creates it, otherwise it opens.
func Open(path string) (*Database, error) {

	var err error

	db := Database{}

	// load the database driver
	if db.connection, err = sql.Open("sqlite3", filepath.Join(path, "database.db")); err != nil {
		log.ERR.Printf("error loading database driver: %v\n", err)
		return nil, err
	}
	log.DBG.Println("database driver loaded")

	// actually check that the database connection is OK
	if err = db.connection.Ping(); err != nil {
		log.ERR.Printf("error connecting to database: %v\n", err)
		defer db.connection.Close()
		return nil, err
	}
	log.DBG.Println("database connection open")

	return &db, nil
}

// Execute runs the given query against the current database.
func (db *Database) Execute(query string, args ...interface{}) (sql.Result, error) {
	return db.connection.Exec(query, args)
}

// OpenTransaction starts a new transaction on this connection.
func (db *Database) OpenTransaction() (*Database, error) {

	if db.transaction != nil {
		log.ERR.Printf("there is an active transaction already")
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
		log.ERR.Printf("there is an transaction to commit")
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
		log.ERR.Printf("there is an no transaction to rollback")
		err = fmt.Errorf("there is an no transaction to rollback")
	}
	return db, err
}

func (db *Database) Prepare(query string) (*sql.Stmt, error) {
	if db.transaction != nil {
		log.DBG.Printn("preparing statement on existing transaction")
		return db.transaction.Prepare(query)
	} else {
		log.DBG.Printn("preparing statement on raw connection")
		db.connection.Prepare(query)
	}
}

// Close releases the connection to the Database on which it is called.
func (db *Database) Close() error {
	err := db.connection.Close()
	db.connection = nil
	return err
}
