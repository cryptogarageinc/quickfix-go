// Copyright (c) quickfixengine.org  All rights reserved.
//
// This file may be distributed under the terms of the quickfixengine.org
// license as defined by quickfixengine.org and appearing in the file
// LICENSE included in the packaging of this file.
//
// This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING
// THE WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A
// PARTICULAR PURPOSE.
//
// See http://www.quickfixengine.org/LICENSE for licensing information.
//
// Contact ask@quickfixengine.org if any conditions of this licensing
// are not clear to you.

package quickfix

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cryptogarageinc/quickfix-go/config"
	"github.com/jinzhu/gorm"
)

type sqlStoreFactory struct {
	settings *Settings
}

type sqlStore struct {
	sessionID          SessionID
	cache              *memoryStore
	sqlDriver          string
	sqlDataSourceName  string
	sqlConnMaxLifetime time.Duration
	sqlConnMaxIdle     int
	sqlConnMaxOpen     int
	db                 *gorm.DB

	*messageBuilder
}

type dbSettings struct {
	connMaxLifetime time.Duration
	connMaxIdle     int
	connMaxOpen     int
}

// NewSQLStoreFactory returns a sql-based implementation of MessageStoreFactory.
func NewSQLStoreFactory(settings *Settings) MessageStoreFactory {
	return sqlStoreFactory{settings: settings}
}

// Create creates a new SQLStore implementation of the MessageStore interface.
func (f sqlStoreFactory) Create(sessionID SessionID) (msgStore MessageStore, err error) {
	var sqlDriver string
	var sqlDataSourceName string
	sqlConnMaxLifetime := 0 * time.Second
	sqlConnMaxIdle := 0
	sqlConnMaxOpen := 0

	settings := make([]*SessionSettings, 1, 2)
	settings[0] = f.settings.GlobalSettings()
	if sessionSettings, ok := f.settings.SessionSettings()[sessionID]; ok {
		settings = append(settings, sessionSettings)
	}
	for _, sessionSettings := range settings {
		if sessionSettings.HasSetting(config.SQLStoreDriver) {
			sqlDriver, err = sessionSettings.Setting(config.SQLStoreDriver)
			if err != nil {
				return nil, err
			}
		}
		if sessionSettings.HasSetting(config.SQLStoreDataSourceName) {
			sqlDataSourceName, err = sessionSettings.Setting(config.SQLStoreDataSourceName)
			if err != nil {
				return nil, err
			}
		}
		if sessionSettings.HasSetting(config.SQLStoreConnMaxLifetime) {
			sqlConnMaxLifetime, err = sessionSettings.DurationSetting(config.SQLStoreConnMaxLifetime)
			if err != nil {
				return nil, err
			}
		}
		if sessionSettings.HasSetting(config.SQLStoreConnMaxIdle) {
			sqlConnMaxIdle, err = sessionSettings.IntSetting(config.SQLStoreConnMaxIdle)
			if err != nil {
				return nil, err
			}
		}
		if sessionSettings.HasSetting(config.SQLStoreConnMaxOpen) {
			sqlConnMaxOpen, err = sessionSettings.IntSetting(config.SQLStoreConnMaxOpen)
			if err != nil {
				return nil, err
			}
		}
	}

	if len(sqlDriver) == 0 {
		return nil, fmt.Errorf("SQLStoreDriver configuration is not found. session: %v", sessionID)
	} else if len(sqlDataSourceName) == 0 {
		return nil, fmt.Errorf("SQLStoreDataSourceName configuration is not found. session: %v", sessionID)
	}
	return newSQLStore(sessionID, sqlDriver, sqlDataSourceName, dbSettings{
		connMaxLifetime: sqlConnMaxLifetime,
		connMaxIdle:     sqlConnMaxIdle,
		connMaxOpen:     sqlConnMaxOpen,
	})
}

func newSQLStore(sessionID SessionID, driver string, dataSourceName string, dbs dbSettings) (store *sqlStore, err error) {
	store = &sqlStore{
		sessionID:          sessionID,
		cache:              &memoryStore{},
		sqlDriver:          driver,
		sqlDataSourceName:  dataSourceName,
		sqlConnMaxLifetime: dbs.connMaxLifetime,
		sqlConnMaxIdle:     dbs.connMaxIdle,
		sqlConnMaxOpen:     dbs.connMaxOpen,
	}
	store.messageBuilder = newMessageBuilder(store)
	store.cache.Reset()

	if store.db, err = gorm.Open(store.sqlDriver, store.sqlDataSourceName); err != nil {
		return nil, err
	}
	store.db.DB().SetConnMaxLifetime(dbs.connMaxLifetime)
	store.db.DB().SetMaxIdleConns(dbs.connMaxIdle)
	store.db.DB().SetMaxOpenConns(dbs.connMaxOpen)

	if err = store.db.DB().Ping(); err != nil { // ensure immediate connection
		return nil, err
	}
	if err = store.populateCache(); err != nil {
		return nil, err
	}

	return store, nil
}

// Reset deletes the store records and sets the seqnums back to 1.
func (store *sqlStore) Reset() (err error) {
	if store.db == nil {
		return ErrAccessToClosedStore
	}
	return store.reset(store.db)
}

func (store *sqlStore) reset(tx *gorm.DB) (err error) {
	s := store.sessionID
	if err = tx.Exec(`DELETE FROM messages
		WHERE beginstring = ? AND session_qualifier = ?
		AND sendercompid = ? AND sendersubid = ? AND senderlocid = ?
		AND targetcompid = ? AND targetsubid = ? AND targetlocid = ?`,
		s.BeginString, s.Qualifier,
		s.SenderCompID, s.SenderSubID, s.SenderLocationID,
		s.TargetCompID, s.TargetSubID, s.TargetLocationID).Error; err != nil {
		return err
	}

	if err = store.cache.Reset(); err != nil {
		return err
	}

	return tx.Exec(`UPDATE sessions
		SET creation_time = ?, incoming_seqnum = ?, outgoing_seqnum = ?
		WHERE beginstring = ? AND session_qualifier = ?
		AND sendercompid= ? AND sendersubid = ? AND senderlocid = ?
		AND targetcompid = ? AND targetsubid = ? AND targetlocid = ?`,
		store.cache.CreationTime(), store.cache.NextTargetMsgSeqNum(), store.cache.NextSenderMsgSeqNum(),
		s.BeginString, s.Qualifier,
		s.SenderCompID, s.SenderSubID, s.SenderLocationID,
		s.TargetCompID, s.TargetSubID, s.TargetLocationID).Error
}

// Refresh reloads the store from the database.
func (store *sqlStore) Refresh() (err error) {
	if err = store.cache.Reset(); err != nil {
		return err
	}
	return store.populateCache()
}

func (store *sqlStore) populateCache() (err error) {
	if store.db == nil {
		return ErrAccessToClosedStore
	}
	s := store.sessionID
	var creationTime time.Time
	var incomingSeqNum, outgoingSeqNum int
	row := store.db.Raw(`SELECT creation_time, incoming_seqnum, outgoing_seqnum
	  	FROM sessions
		WHERE beginstring = ? AND session_qualifier = ?
		AND sendercompid = ? AND sendersubid = ? AND senderlocid = ?
		AND targetcompid = ? AND targetsubid = ? AND targetlocid = ?`,
		s.BeginString, s.Qualifier,
		s.SenderCompID, s.SenderSubID, s.SenderLocationID,
		s.TargetCompID, s.TargetSubID, s.TargetLocationID).Row()

	err = row.Scan(&creationTime, &incomingSeqNum, &outgoingSeqNum)
	// session record found, load it
	if err == nil {
		store.cache.creationTime = creationTime
		store.cache.SetNextTargetMsgSeqNum(incomingSeqNum)
		store.cache.SetNextSenderMsgSeqNum(outgoingSeqNum)
		return nil
	}

	// fatal error, give up
	if err != sql.ErrNoRows {
		return err
	}

	// session record not found, create it
	return store.db.Exec(`INSERT INTO sessions (
			creation_time, incoming_seqnum, outgoing_seqnum,
			beginstring, session_qualifier,
			sendercompid, sendersubid, senderlocid,
			targetcompid, targetsubid, targetlocid)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		store.cache.creationTime,
		store.cache.NextTargetMsgSeqNum(),
		store.cache.NextSenderMsgSeqNum(),
		s.BeginString, s.Qualifier,
		s.SenderCompID, s.SenderSubID, s.SenderLocationID,
		s.TargetCompID, s.TargetSubID, s.TargetLocationID).Error
}

// NextSenderMsgSeqNum returns the next MsgSeqNum that will be sent.
func (store *sqlStore) NextSenderMsgSeqNum() int {
	return store.cache.NextSenderMsgSeqNum()
}

// NextTargetMsgSeqNum returns the next MsgSeqNum that should be received.
func (store *sqlStore) NextTargetMsgSeqNum() int {
	return store.cache.NextTargetMsgSeqNum()
}

// SetNextSenderMsgSeqNum sets the next MsgSeqNum that will be sent.
func (store *sqlStore) SetNextSenderMsgSeqNum(next int) error {
	if store.db == nil {
		return ErrAccessToClosedStore
	}
	s := store.sessionID
	if err := store.db.Exec(`UPDATE sessions SET outgoing_seqnum = ?
		WHERE beginstring = ? AND session_qualifier = ?
		AND sendercompid = ? AND sendersubid = ? AND senderlocid = ?
		AND targetcompid = ? AND targetsubid = ? AND targetlocid = ?`,
		next, s.BeginString, s.Qualifier,
		s.SenderCompID, s.SenderSubID, s.SenderLocationID,
		s.TargetCompID, s.TargetSubID, s.TargetLocationID).Error; err != nil {
		return err
	}
	return store.cache.SetNextSenderMsgSeqNum(next)
}

// SetNextTargetMsgSeqNum sets the next MsgSeqNum that should be received.
func (store *sqlStore) SetNextTargetMsgSeqNum(next int) error {
	if store.db == nil {
		return ErrAccessToClosedStore
	}
	s := store.sessionID
	if err := store.db.Exec(`UPDATE sessions SET incoming_seqnum = ?
		WHERE beginstring = ? AND session_qualifier = ?
		AND sendercompid = ? AND sendersubid = ? AND senderlocid = ?
		AND targetcompid = ? AND targetsubid = ? AND targetlocid = ?`,
		next, s.BeginString, s.Qualifier,
		s.SenderCompID, s.SenderSubID, s.SenderLocationID,
		s.TargetCompID, s.TargetSubID, s.TargetLocationID).Error; err != nil {
		return err
	}
	return store.cache.SetNextTargetMsgSeqNum(next)
}

// IncrNextSenderMsgSeqNum increments the next MsgSeqNum that will be sent.
func (store *sqlStore) IncrNextSenderMsgSeqNum() error {
	store.cache.IncrNextSenderMsgSeqNum()
	return store.SetNextSenderMsgSeqNum(store.cache.NextSenderMsgSeqNum())
}

// IncrNextTargetMsgSeqNum increments the next MsgSeqNum that should be received.
func (store *sqlStore) IncrNextTargetMsgSeqNum() error {
	store.cache.IncrNextTargetMsgSeqNum()
	return store.SetNextTargetMsgSeqNum(store.cache.NextTargetMsgSeqNum())
}

// CreationTime returns the creation time of the store.
func (store *sqlStore) CreationTime() time.Time {
	return store.cache.CreationTime()
}

func (store *sqlStore) SaveMessage(seqNum int, msg []byte) error {
	if store.db == nil {
		return ErrAccessToClosedStore
	}
	s := store.sessionID

	return store.db.Exec(`INSERT INTO messages (
			msgseqnum, message,
			beginstring, session_qualifier,
			sendercompid, sendersubid, senderlocid,
			targetcompid, targetsubid, targetlocid)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		seqNum, string(msg),
		s.BeginString, s.Qualifier,
		s.SenderCompID, s.SenderSubID, s.SenderLocationID,
		s.TargetCompID, s.TargetSubID, s.TargetLocationID).Error
}

func (store *sqlStore) SaveMessageAndIncrNextSenderMsgSeqNum(seqNum int, msg []byte) error {
	if store.db == nil {
		return ErrAccessToClosedStore
	}
	s := store.sessionID

	err := store.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`INSERT INTO messages (
			msgseqnum, message,
			beginstring, session_qualifier,
			sendercompid, sendersubid, senderlocid,
			targetcompid, targetsubid, targetlocid)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			seqNum, string(msg),
			s.BeginString, s.Qualifier,
			s.SenderCompID, s.SenderSubID, s.SenderLocationID,
			s.TargetCompID, s.TargetSubID, s.TargetLocationID).Error; err != nil {
			return err
		}

		checkSeqNum := store.cache.NextSenderMsgSeqNum()
		if seqNum != checkSeqNum {
			return errors.New("internal error: unmatch seqnum")
		}
		nextSeqNum := seqNum + 1
		if err := tx.Exec(`UPDATE sessions SET outgoing_seqnum = ?
			WHERE beginstring = ? AND session_qualifier = ?
			AND sendercompid = ? AND sendersubid = ? AND senderlocid = ?
			AND targetcompid = ? AND targetsubid = ? AND targetlocid = ?`,
			nextSeqNum, s.BeginString, s.Qualifier,
			s.SenderCompID, s.SenderSubID, s.SenderLocationID,
			s.TargetCompID, s.TargetSubID, s.TargetLocationID).Error; err != nil {
			return err
		}
		return store.cache.SetNextSenderMsgSeqNum(nextSeqNum)
	})
	if err != nil {
		_ = store.Refresh()
		// Response should also be returned in case of an error.
		return err
	}
	return nil
}

func (store *sqlStore) GetMessages(beginSeqNum, endSeqNum int) ([][]byte, error) {
	if store.db == nil {
		return nil, ErrAccessToClosedStore
	}
	s := store.sessionID
	var msgs [][]byte
	rows, err := store.db.Raw(`SELECT message FROM messages
		WHERE beginstring= ? AND session_qualifier= ?
		AND sendercompid= ? AND sendersubid= ? AND senderlocid= ?
		AND targetcompid= ? AND targetsubid= ? AND targetlocid= ?
		AND msgseqnum>= ? AND msgseqnum<= ?
		ORDER BY msgseqnum`,
		s.BeginString, s.Qualifier,
		s.SenderCompID, s.SenderSubID, s.SenderLocationID,
		s.TargetCompID, s.TargetSubID, s.TargetLocationID,
		beginSeqNum, endSeqNum).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var message string
		if err := rows.Scan(&message); err != nil {
			return nil, err
		}
		msgs = append(msgs, []byte(message))
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return msgs, nil
}

func (store *sqlStore) SaveMessageWithTx(messageBuildData *BuildMessageInput) (output *BuildMessageOutput, err error) {
	if store.db == nil {
		return nil, ErrAccessToClosedStore
	}
	s := store.sessionID

	err = store.db.Transaction(func(tx *gorm.DB) error {
		var outgoingSeqNum int
		row := tx.Raw(`SELECT outgoing_seqnum FROM sessions
			WHERE beginstring = ? AND session_qualifier = ?
			AND sendercompid = ? AND sendersubid = ? AND senderlocid = ?
			AND targetcompid = ? AND targetsubid = ? AND targetlocid = ?`,
			s.BeginString, s.Qualifier,
			s.SenderCompID, s.SenderSubID, s.SenderLocationID,
			s.TargetCompID, s.TargetSubID, s.TargetLocationID).Row()

		if err := row.Scan(&outgoingSeqNum); err != nil {
			return err
		}
		if outgoingSeqNum != store.cache.NextSenderMsgSeqNum() {
			store.cache.SetNextSenderMsgSeqNum(outgoingSeqNum) // refresh
		}

		input := *messageBuildData
		input.IsResetSeqNum = false // For execute store.reset(tx) after BuildMessage is executed.
		outputData, err := store.BuildMessage(&input)
		if err != nil {
			return err
		}
		if messageBuildData.IsResetSeqNum {
			if err = store.reset(tx); err != nil {
				return err
			}
			outputData.SentReset = true
			outputData.SeqNum = store.NextSenderMsgSeqNum()
			outputData.Msg.Header.SetField(tagMsgSeqNum, FIXInt(outputData.SeqNum))
			outputData.MsgBytes = outputData.Msg.build()
		}

		output = outputData // Response should also be returned in case of an error.
		seqNum := store.cache.NextSenderMsgSeqNum()
		if seqNum != output.SeqNum {
			return errors.New("internal error: unmatch seqnum")
		}
		nextSeqNum := seqNum + 1

		if err := tx.Exec(`INSERT INTO messages (
			msgseqnum, message,
			beginstring, session_qualifier,
			sendercompid, sendersubid, senderlocid,
			targetcompid, targetsubid, targetlocid)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			seqNum, string(outputData.MsgBytes),
			s.BeginString, s.Qualifier,
			s.SenderCompID, s.SenderSubID, s.SenderLocationID,
			s.TargetCompID, s.TargetSubID, s.TargetLocationID).Error; err != nil {
			return err
		}

		if err := tx.Exec(`UPDATE sessions SET outgoing_seqnum = ?
			WHERE beginstring = ? AND session_qualifier = ?
			AND sendercompid = ? AND sendersubid = ? AND senderlocid = ?
			AND targetcompid = ? AND targetsubid = ? AND targetlocid = ?`,
			nextSeqNum, s.BeginString, s.Qualifier,
			s.SenderCompID, s.SenderSubID, s.SenderLocationID,
			s.TargetCompID, s.TargetSubID, s.TargetLocationID).Error; err != nil {
			return err
		}
		return store.cache.SetNextSenderMsgSeqNum(nextSeqNum)
	})
	if err != nil {
		_ = store.Refresh()
		// Response should also be returned in case of an error.
		return
	}
	return output, nil
}

// Close closes the store's database connection.
func (store *sqlStore) Close() error {
	if store.db != nil {
		store.db.Close()
		store.db = nil
	}
	return nil
}
