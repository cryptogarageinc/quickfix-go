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
	"time"

	"github.com/pkg/errors"
)

// The MessageStore interface provides methods to record and retrieve messages for resend purposes.
type MessageStore interface {
	NextSenderMsgSeqNum() int
	NextTargetMsgSeqNum() int

	IncrNextSenderMsgSeqNum() error
	IncrNextTargetMsgSeqNum() error

	SetNextSenderMsgSeqNum(next int) error
	SetNextTargetMsgSeqNum(next int) error

	CreationTime() time.Time

	SaveMessage(seqNum int, msg []byte) error
	SaveMessageAndIncrNextSenderMsgSeqNum(seqNum int, msg []byte) error
	GetMessages(beginSeqNum, endSeqNum int) ([][]byte, error)

	Refresh() error
	Reset() error

	Close() error

	SaveMessageWithTx(messageBuildData *BuildMessageInput) (output *BuildMessageOutput, err error)
	BuildMessage(messageBuildData *BuildMessageInput) (output *BuildMessageOutput, err error)
}

// The MessageStoreFactory interface is used by session to create a session specific message store.
type MessageStoreFactory interface {
	Create(sessionID SessionID) (MessageStore, error)
}

type memoryStore struct {
	senderMsgSeqNum, targetMsgSeqNum int
	creationTime                     time.Time
	messageMap                       map[int][]byte

	isClosed bool
	*messageBuilder
}

func (store *memoryStore) NextSenderMsgSeqNum() int {
	return store.senderMsgSeqNum + 1
}

func (store *memoryStore) NextTargetMsgSeqNum() int {
	return store.targetMsgSeqNum + 1
}

func (store *memoryStore) IncrNextSenderMsgSeqNum() error {
	store.senderMsgSeqNum++
	return nil
}

func (store *memoryStore) IncrNextTargetMsgSeqNum() error {
	store.targetMsgSeqNum++
	return nil
}

func (store *memoryStore) SetNextSenderMsgSeqNum(nextSeqNum int) error {
	if store.isClosed {
		return ErrAccessToClosedStore
	}
	store.senderMsgSeqNum = nextSeqNum - 1
	return nil
}
func (store *memoryStore) SetNextTargetMsgSeqNum(nextSeqNum int) error {
	if store.isClosed {
		return ErrAccessToClosedStore
	}
	store.targetMsgSeqNum = nextSeqNum - 1
	return nil
}

func (store *memoryStore) CreationTime() time.Time {
	return store.creationTime
}

func (store *memoryStore) Reset() error {
	if store.isClosed {
		return ErrAccessToClosedStore
	}
	store.senderMsgSeqNum = 0
	store.targetMsgSeqNum = 0
	store.creationTime = time.Now()
	store.messageMap = nil
	return nil
}

func (store *memoryStore) Refresh() error {
	if store.isClosed {
		return ErrAccessToClosedStore
	}
	return nil
}

func (store *memoryStore) Close() error {
	store.isClosed = true
	return nil
}

func (store *memoryStore) SaveMessage(seqNum int, msg []byte) error {
	if store.isClosed {
		return ErrAccessToClosedStore
	}
	if store.messageMap == nil {
		store.messageMap = make(map[int][]byte)
	}

	store.messageMap[seqNum] = msg
	return nil
}

func (store *memoryStore) SaveMessageAndIncrNextSenderMsgSeqNum(seqNum int, msg []byte) error {
	err := store.SaveMessage(seqNum, msg)
	if err != nil {
		return err
	}
	return store.IncrNextSenderMsgSeqNum()
}

func (store *memoryStore) GetMessages(beginSeqNum, endSeqNum int) ([][]byte, error) {
	if store.isClosed {
		return nil, ErrAccessToClosedStore
	}
	var msgs [][]byte
	for seqNum := beginSeqNum; seqNum <= endSeqNum; seqNum++ {
		if m, ok := store.messageMap[seqNum]; ok {
			msgs = append(msgs, m)
		}
	}
	return msgs, nil
}

func (store *memoryStore) SaveMessageWithTx(messageBuildData *BuildMessageInput) (output *BuildMessageOutput, err error) {
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

type memoryStoreFactory struct{}

func (f memoryStoreFactory) Create(sessionID SessionID) (MessageStore, error) {
	m := new(memoryStore)
	m.messageBuilder = newMessageBuilder(m)
	if err := m.Reset(); err != nil {
		return m, errors.Wrap(err, "reset")
	}
	return m, nil
}

// NewMemoryStoreFactory returns a MessageStoreFactory instance that created in-memory MessageStores.
func NewMemoryStoreFactory() MessageStoreFactory { return memoryStoreFactory{} }

// append API ------------------------------------------------------------------

// ErrAccessToClosedStore defines error on accessing to closed message store.
var ErrAccessToClosedStore = errors.New("this store is already closed")
