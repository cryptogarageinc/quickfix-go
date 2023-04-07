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
	"context"
	"fmt"
	"time"

	"github.com/cryptogarageinc/quickfix-go/config"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/pkg/errors"
)

type mongoStoreFactory struct {
	settings           *Settings
	messagesCollection string
	sessionsCollection string
}

type mongoStore struct {
	sessionID          SessionID
	cache              *memoryStore
	mongoURL           string
	mongoDatabase      string
	db                 *mongo.Client
	messagesCollection string
	sessionsCollection string
	allowTransactions  bool

	*messageBuilder
}

// NewMongoStoreFactory returns a mongo-based implementation of MessageStoreFactory.
func NewMongoStoreFactory(settings *Settings) MessageStoreFactory {
	return NewMongoStoreFactoryPrefixed(settings, "")
}

// NewMongoStoreFactoryPrefixed returns a mongo-based implementation of MessageStoreFactory, with prefix on collections.
func NewMongoStoreFactoryPrefixed(settings *Settings, collectionsPrefix string) MessageStoreFactory {
	return mongoStoreFactory{
		settings:           settings,
		messagesCollection: collectionsPrefix + "messages",
		sessionsCollection: collectionsPrefix + "sessions",
	}
}

// Create creates a new MongoStore implementation of the MessageStore interface.
func (f mongoStoreFactory) Create(sessionID SessionID) (msgStore MessageStore, err error) {
	var mongoConnectionURL string
	var mongoDatabase string

	settings := make([]*SessionSettings, 1, 2)
	settings[0] = f.settings.GlobalSettings()
	if sessionSettings, ok := f.settings.SessionSettings()[sessionID]; ok {
		settings = append(settings, sessionSettings)
	}
	for _, sessionSettings := range settings {
		if sessionSettings.HasSetting(config.MongoStoreConnection) {
			mongoConnectionURL, err = sessionSettings.Setting(config.MongoStoreConnection)
			if err != nil {
				return nil, err
			}
		}
		if sessionSettings.HasSetting(config.MongoStoreDatabase) {
			mongoDatabase, err = sessionSettings.Setting(config.MongoStoreDatabase)
			if err != nil {
				return nil, err
			}
		}
	}

	if len(mongoConnectionURL) == 0 {
		return nil, fmt.Errorf("MongoStoreConnection configuration is not found. session: %v", sessionID)
	} else if len(mongoDatabase) == 0 {
		return nil, fmt.Errorf("MongoStoreDatabase configuration is not found. session: %v", sessionID)
	}
	return newMongoStore(sessionID, mongoConnectionURL, mongoDatabase, f.messagesCollection, f.sessionsCollection)
}

func newMongoStore(sessionID SessionID, mongoURL, mongoDatabase, mongoReplicaSet, messagesCollection, sessionsCollection string) (store *mongoStore, err error) {

	allowTransactions := len(mongoReplicaSet) > 0
	store = &mongoStore{
		sessionID:          sessionID,
		cache:              &memoryStore{},
		mongoURL:           mongoURL,
		mongoDatabase:      mongoDatabase,
		messagesCollection: messagesCollection,
		sessionsCollection: sessionsCollection,
		allowTransactions:  allowTransactions,
	}
	store.messageBuilder = newMessageBuilder(store)

	if err = store.cache.Reset(); err != nil {
		err = errors.Wrap(err, "cache reset")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store.db, err = mongo.Connect(ctx, options.Client().ApplyURI(mongoURL).SetDirect(len(mongoReplicaSet) == 0).SetReplicaSet(mongoReplicaSet))
	if err != nil {
		return
	}
	err = store.populateCache()

	return
}

func generateMessageFilter(s *SessionID) (messageFilter *mongoQuickFixEntryData) {
	messageFilter = &mongoQuickFixEntryData{
		BeginString:      s.BeginString,
		SessionQualifier: s.Qualifier,
		SenderCompID:     s.SenderCompID,
		SenderSubID:      s.SenderSubID,
		SenderLocID:      s.SenderLocationID,
		TargetCompID:     s.TargetCompID,
		TargetSubID:      s.TargetSubID,
		TargetLocID:      s.TargetLocationID,
	}
	return
}

type mongoQuickFixEntryData struct {
	// Message specific data.
	Msgseq  int    `bson:"msgseq,omitempty"`
	Message []byte `bson:"message,omitempty"`
	// Session specific data.
	CreationTime   time.Time `bson:"creation_time,omitempty"`
	IncomingSeqNum int       `bson:"incoming_seq_num,omitempty"`
	OutgoingSeqNum int       `bson:"outgoing_seq_num,omitempty"`
	// Indexed data.
	BeginString      string `bson:"begin_string"`
	SessionQualifier string `bson:"session_qualifier"`
	SenderCompID     string `bson:"sender_comp_id"`
	SenderSubID      string `bson:"sender_sub_id"`
	SenderLocID      string `bson:"sender_loc_id"`
	TargetCompID     string `bson:"target_comp_id"`
	TargetSubID      string `bson:"target_sub_id"`
	TargetLocID      string `bson:"target_loc_id"`
}

// Reset deletes the store records and sets the seqnums back to 1.
func (store *mongoStore) Reset() error {
	if store.db == nil {
		return ErrAccessToClosedStore
	}
	msgFilter := generateMessageFilter(&store.sessionID)
	_, err := store.db.Database(store.mongoDatabase).Collection(store.messagesCollection).DeleteMany(context.Background(), msgFilter)

	if err != nil {
		return err
	}

	if err = store.cache.Reset(); err != nil {
		return err
	}

	sessionUpdate := generateMessageFilter(&store.sessionID)
	sessionUpdate.CreationTime = store.cache.CreationTime()
	sessionUpdate.IncomingSeqNum = store.cache.NextTargetMsgSeqNum()
	sessionUpdate.OutgoingSeqNum = store.cache.NextSenderMsgSeqNum()
	_, err = store.db.Database(store.mongoDatabase).Collection(store.sessionsCollection).UpdateOne(context.Background(), msgFilter, bson.M{"$set": sessionUpdate})

	return err
}

// Refresh reloads the store from the database.
func (store *mongoStore) Refresh() error {
	if err := store.cache.Reset(); err != nil {
		return err
	}
	return store.populateCache()
}

func (store *mongoStore) populateCache() error {
	if store.db == nil {
		return ErrAccessToClosedStore
	}
	msgFilter := generateMessageFilter(&store.sessionID)
	res := store.db.Database(store.mongoDatabase).Collection(store.sessionsCollection).FindOne(context.Background(), msgFilter)
	if res.Err() != nil && res.Err() != mongo.ErrNoDocuments {
		return errors.Wrap(res.Err(), "query")
	}

	if res.Err() != mongo.ErrNoDocuments {
		// session record found, load it
		sessionData := &mongoQuickFixEntryData{}
		if err := res.Decode(&sessionData); err != nil {
			return errors.Wrap(err, "decode")
		}

		store.cache.creationTime = sessionData.CreationTime
		if err := store.cache.SetNextTargetMsgSeqNum(sessionData.IncomingSeqNum); err != nil {
			return errors.Wrap(err, "cache set next target")
		}

		if err := store.cache.SetNextSenderMsgSeqNum(sessionData.OutgoingSeqNum); err != nil {
			return errors.Wrap(err, "cache set next sender")
		}

		return nil
	}

	// session record not found, create it
	msgFilter.CreationTime = store.cache.creationTime
	msgFilter.IncomingSeqNum = store.cache.NextTargetMsgSeqNum()
	msgFilter.OutgoingSeqNum = store.cache.NextSenderMsgSeqNum()

	if _, err := store.db.Database(store.mongoDatabase).Collection(store.sessionsCollection).InsertOne(context.Background(), msgFilter); err != nil {
		return errors.Wrap(err, "insert")
	}
	return nil
}

// NextSenderMsgSeqNum returns the next MsgSeqNum that will be sent.
func (store *mongoStore) NextSenderMsgSeqNum() int {
	return store.cache.NextSenderMsgSeqNum()
}

// NextTargetMsgSeqNum returns the next MsgSeqNum that should be received.
func (store *mongoStore) NextTargetMsgSeqNum() int {
	return store.cache.NextTargetMsgSeqNum()
}

// SetNextSenderMsgSeqNum sets the next MsgSeqNum that will be sent.
func (store *mongoStore) SetNextSenderMsgSeqNum(next int) error {
	if store.db == nil {
		return ErrAccessToClosedStore
	}
	msgFilter := generateMessageFilter(&store.sessionID)
	sessionUpdate := generateMessageFilter(&store.sessionID)
	sessionUpdate.IncomingSeqNum = store.cache.NextTargetMsgSeqNum()
	sessionUpdate.OutgoingSeqNum = next
	sessionUpdate.CreationTime = store.cache.CreationTime()
	if _, err := store.db.Database(store.mongoDatabase).Collection(store.sessionsCollection).UpdateOne(context.Background(), msgFilter, bson.M{"$set": sessionUpdate}); err != nil {
		return err
	}
	return store.cache.SetNextSenderMsgSeqNum(next)
}

// SetNextTargetMsgSeqNum sets the next MsgSeqNum that should be received.
func (store *mongoStore) SetNextTargetMsgSeqNum(next int) error {
	if store.db == nil {
		return ErrAccessToClosedStore
	}
	msgFilter := generateMessageFilter(&store.sessionID)
	sessionUpdate := generateMessageFilter(&store.sessionID)
	sessionUpdate.IncomingSeqNum = next
	sessionUpdate.OutgoingSeqNum = store.cache.NextSenderMsgSeqNum()
	sessionUpdate.CreationTime = store.cache.CreationTime()
	if _, err := store.db.Database(store.mongoDatabase).Collection(store.sessionsCollection).UpdateOne(context.Background(), msgFilter, bson.M{"$set": sessionUpdate}); err != nil {
		return err
	}
	return store.cache.SetNextTargetMsgSeqNum(next)
}

// IncrNextSenderMsgSeqNum increments the next MsgSeqNum that will be sent.
func (store *mongoStore) IncrNextSenderMsgSeqNum() error {
	if err := store.cache.IncrNextSenderMsgSeqNum(); err != nil {
		return errors.Wrap(err, "cache incr")
	}
	return store.SetNextSenderMsgSeqNum(store.cache.NextSenderMsgSeqNum())
}

// IncrNextTargetMsgSeqNum increments the next MsgSeqNum that should be received.
func (store *mongoStore) IncrNextTargetMsgSeqNum() error {
	if err := store.cache.IncrNextTargetMsgSeqNum(); err != nil {
		return errors.Wrap(err, "cache incr")
	}
	return store.SetNextTargetMsgSeqNum(store.cache.NextTargetMsgSeqNum())
}

// CreationTime returns the creation time of the store.
func (store *mongoStore) CreationTime() time.Time {
	return store.cache.CreationTime()
}

func (store *mongoStore) SaveMessage(seqNum int, msg []byte) (err error) {
	if store.db == nil {
		err = ErrAccessToClosedStore
		return
	}
	msgFilter := generateMessageFilter(&store.sessionID)
	msgFilter.Msgseq = seqNum
	msgFilter.Message = msg
	_, err = store.db.Database(store.mongoDatabase).Collection(store.messagesCollection).InsertOne(context.Background(), msgFilter)
	return
}

func (store *mongoStore) SaveMessageAndIncrNextSenderMsgSeqNum(seqNum int, msg []byte) error {

	if !store.allowTransactions {
		err := store.SaveMessage(seqNum, msg)
		if err != nil {
			return err
		}
		return store.IncrNextSenderMsgSeqNum()
	}

	// If the mongodb supports replicasets, perform this operation as a transaction instead-
	var next int
	err := store.db.UseSession(context.Background(), func(sessionCtx mongo.SessionContext) error {
		if err := sessionCtx.StartTransaction(); err != nil {
			return err
		}

		msgFilter := generateMessageFilter(&store.sessionID)
		msgFilter.Msgseq = seqNum
		msgFilter.Message = msg
		_, err := store.db.Database(store.mongoDatabase).Collection(store.messagesCollection).InsertOne(sessionCtx, msgFilter)
		if err != nil {
			return err
		}

		next = store.cache.NextSenderMsgSeqNum() + 1

		msgFilter = generateMessageFilter(&store.sessionID)
		sessionUpdate := generateMessageFilter(&store.sessionID)
		sessionUpdate.IncomingSeqNum = store.cache.NextTargetMsgSeqNum()
		sessionUpdate.OutgoingSeqNum = next
		sessionUpdate.CreationTime = store.cache.CreationTime()
		_, err = store.db.Database(store.mongoDatabase).Collection(store.sessionsCollection).UpdateOne(sessionCtx, msgFilter, bson.M{"$set": sessionUpdate})
		if err != nil {
			return err
		}

		return sessionCtx.CommitTransaction(context.Background())
	})
	if err != nil {
		return err
	}

	return store.cache.SetNextSenderMsgSeqNum(next)
}

func (store *mongoStore) GetMessages(beginSeqNum, endSeqNum int) (msgs [][]byte, err error) {
	if store.db == nil {
		err = ErrAccessToClosedStore
		return
	}
	msgFilter := generateMessageFilter(&store.sessionID)
	// Marshal into database form.
	msgFilterBytes, err := bson.Marshal(msgFilter)
	if err != nil {
		return
	}
	seqFilter := bson.M{}
	err = bson.Unmarshal(msgFilterBytes, &seqFilter)
	if err != nil {
		return
	}
	// Modify the query to use a range for the sequence filter.
	seqFilter["msgseq"] = bson.M{
		"$gte": beginSeqNum,
		"$lte": endSeqNum,
	}

	iter := store.db.DB(store.mongoDatabase).C(store.messagesCollection).Find(seqFilter).Sort("msgseq").Iter()
	for iter.Next(msgFilter) {
		msgs = append(msgs, msgFilter.Message)
	}
	err = iter.Close()
	return
}

func (store *mongoStore) SaveMessageWithTx(messageBuildData *BuildMessageInput) (output *BuildMessageOutput, err error) {
	output, err = store.BuildMessage(messageBuildData)
	if err != nil {
		return
	}
	err = store.IncrNextSenderMsgSeqNum()
	if err != nil {
		return
	}

	err = store.SaveMessage(output.SeqNum, output.MsgBytes)
	return
}

// Close closes the store's database connection.
func (store *mongoStore) Close() error {
	if store.db != nil {
		err := store.db.Disconnect(context.Background())
		if err != nil {
			return errors.Wrap(err, "error disconnecting from database")
		}
		store.db = nil
	}
	return nil
}
