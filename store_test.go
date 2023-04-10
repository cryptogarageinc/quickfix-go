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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// MessageStoreTestSuite is the suite of all tests that should be run against all MessageStore implementations.
type MessageStoreTestSuite struct {
	suite.Suite
	msgStore MessageStore
}

func (suite *MessageStoreTestSuite) getTestName(t *testing.T) string {
	testNames := strings.Split(t.Name(), "/")
	return testNames[len(testNames)-1]
}

// MemoryStoreTestSuite runs all tests in the MessageStoreTestSuite against the MemoryStore implementation.
type MemoryStoreTestSuite struct {
	MessageStoreTestSuite
}

func (suite *MemoryStoreTestSuite) SetupTest() {
	var err error
	suite.msgStore, err = NewMemoryStoreFactory().Create(SessionID{})
	require.Nil(suite.T(), err)
}

func TestMemoryStoreTestSuite(t *testing.T) {
	suite.Run(t, new(MemoryStoreTestSuite))
}

func (suite *MessageStoreTestSuite) TestMessageStore_SetNextMsgSeqNum_Refresh_IncrNextMsgSeqNum() {
	t := suite.T()

	// Given a MessageStore with the following sender and target seqnums
	require.Nil(t, suite.msgStore.SetNextSenderMsgSeqNum(867))
	require.Nil(t, suite.msgStore.SetNextTargetMsgSeqNum(5309))

	// When the store is refreshed from its backing store
	require.Nil(t, suite.msgStore.Refresh())

	// Then the sender and target seqnums should still be
	assert.Equal(t, 867, suite.msgStore.NextSenderMsgSeqNum())
	assert.Equal(t, 5309, suite.msgStore.NextTargetMsgSeqNum())

	// When the sender and target seqnums are incremented
	require.Nil(t, suite.msgStore.IncrNextSenderMsgSeqNum())
	require.Nil(t, suite.msgStore.IncrNextTargetMsgSeqNum())

	// Then the sender and target seqnums should be
	assert.Equal(t, 868, suite.msgStore.NextSenderMsgSeqNum())
	assert.Equal(t, 5310, suite.msgStore.NextTargetMsgSeqNum())

	// When the store is refreshed from its backing store
	require.Nil(t, suite.msgStore.Refresh())

	// Then the sender and target seqnums should still be
	assert.Equal(t, 868, suite.msgStore.NextSenderMsgSeqNum())
	assert.Equal(t, 5310, suite.msgStore.NextTargetMsgSeqNum())
}

func (suite *MessageStoreTestSuite) TestMessageStore_Reset() {
	t := suite.T()

	// Given a MessageStore with the following sender and target seqnums
	require.Nil(t, suite.msgStore.SetNextSenderMsgSeqNum(1234))
	require.Nil(t, suite.msgStore.SetNextTargetMsgSeqNum(5678))

	// When the store is reset
	require.Nil(t, suite.msgStore.Reset())

	// Then the sender and target seqnums should be
	assert.Equal(t, 1, suite.msgStore.NextSenderMsgSeqNum())
	assert.Equal(t, 1, suite.msgStore.NextTargetMsgSeqNum())

	// When the store is refreshed from its backing store
	require.Nil(t, suite.msgStore.Refresh())

	// Then the sender and target seqnums should still be
	assert.Equal(t, 1, suite.msgStore.NextSenderMsgSeqNum())
	assert.Equal(t, 1, suite.msgStore.NextTargetMsgSeqNum())
}

func (suite *MessageStoreTestSuite) TestMessageStore_SaveMessage_GetMessage() {
	t := suite.T()

	// Given the following saved messages
	expectedMsgsBySeqNum := []string{
		"In the frozen land of Nador",
		"they were forced to eat Robin's minstrels",
		"and there was much rejoicing",
	}
	for i, msg := range expectedMsgsBySeqNum {
		seqNum := i + 1
		require.Nil(t, suite.msgStore.SaveMessage(seqNum, []byte(msg)))
	}
	// When the messages are retrieved from the MessageStore
	actualMsgs, err := suite.msgStore.GetMessages(1, 3)
	require.Nil(t, err)

	// Then the messages should be
	require.Len(t, actualMsgs, 3)
	assert.Equal(t, expectedMsgsBySeqNum[0], string(actualMsgs[0]))
	assert.Equal(t, expectedMsgsBySeqNum[1], string(actualMsgs[1]))
	assert.Equal(t, expectedMsgsBySeqNum[2], string(actualMsgs[2]))

	// When the store is refreshed from its backing store
	require.Nil(t, suite.msgStore.Refresh())

	// And the messages are retrieved from the MessageStore
	actualMsgs, err = suite.msgStore.GetMessages(1, 3)
	require.Nil(t, err)

	// Then the messages should still be
	require.Len(t, actualMsgs, 3)
	assert.Equal(t, expectedMsgsBySeqNum[0], string(actualMsgs[0]))
	assert.Equal(t, expectedMsgsBySeqNum[1], string(actualMsgs[1]))
	assert.Equal(t, expectedMsgsBySeqNum[2], string(actualMsgs[2]))
}

func (s *MessageStoreTestSuite) TestMessageStore_GetMessages_EmptyStore() {
	// When messages are retrieved from an empty store
	messages, err := s.msgStore.GetMessages(1, 2)
	require.Nil(s.T(), err)

	// Then no messages should be returned
	require.Empty(s.T(), messages, "Did not expect messages from empty store")
}

func (s *MessageStoreTestSuite) TestMessageStore_GetMessages_VariousRanges() {
	t := s.T()

	// Given the following saved messages
	require.Nil(t, s.msgStore.SaveMessage(1, []byte("hello")))
	require.Nil(t, s.msgStore.SaveMessage(2, []byte("cruel")))
	require.Nil(t, s.msgStore.SaveMessage(3, []byte("world")))

	// When the following requests are made to the store
	var testCases = []struct {
		beginSeqNo, endSeqNo int
		expectedBytes        [][]byte
	}{
		{beginSeqNo: 1, endSeqNo: 1, expectedBytes: [][]byte{[]byte("hello")}},
		{beginSeqNo: 1, endSeqNo: 2, expectedBytes: [][]byte{[]byte("hello"), []byte("cruel")}},
		{beginSeqNo: 1, endSeqNo: 3, expectedBytes: [][]byte{[]byte("hello"), []byte("cruel"), []byte("world")}},
		{beginSeqNo: 1, endSeqNo: 4, expectedBytes: [][]byte{[]byte("hello"), []byte("cruel"), []byte("world")}},
		{beginSeqNo: 2, endSeqNo: 3, expectedBytes: [][]byte{[]byte("cruel"), []byte("world")}},
		{beginSeqNo: 3, endSeqNo: 3, expectedBytes: [][]byte{[]byte("world")}},
		{beginSeqNo: 3, endSeqNo: 4, expectedBytes: [][]byte{[]byte("world")}},
		{beginSeqNo: 4, endSeqNo: 4, expectedBytes: [][]byte{}},
		{beginSeqNo: 4, endSeqNo: 10, expectedBytes: [][]byte{}},
	}

	// Then the returned messages should be
	for _, tc := range testCases {
		actualMsgs, err := s.msgStore.GetMessages(tc.beginSeqNo, tc.endSeqNo)
		require.Nil(t, err)
		require.Len(t, actualMsgs, len(tc.expectedBytes))
		for i, expectedMsg := range tc.expectedBytes {
			assert.Equal(t, string(expectedMsg), string(actualMsgs[i]))
		}
	}
}

func (suite *MessageStoreTestSuite) TestMessageStore_CreationTime() {
	require.Nil(suite.T(), suite.msgStore.Reset())
	assert.False(suite.T(), suite.msgStore.CreationTime().IsZero())

	// This test will be called multiple times in parallel.
	t0 := time.Now()
	time.Sleep(time.Second * 2)
	require.Nil(suite.T(), suite.msgStore.Reset())
	time.Sleep(time.Second * 2)
	t1 := time.Now()
	require.True(suite.T(), suite.msgStore.CreationTime().After(t0))
	require.True(suite.T(), suite.msgStore.CreationTime().Before(t1))
}

func (suite *MessageStoreTestSuite) TestMessageStore_Close_After() {
	t := suite.T()
	require.Nil(suite.T(), suite.msgStore.Reset())

	// Given the following saved messages
	expectedMsgsBySeqNum := []string{
		"In the frozen land of Nador",
		"they were forced to eat Robin's minstrels",
		"and there was much rejoicing",
	}
	for i, msg := range expectedMsgsBySeqNum {
		seqNum := i + 1
		require.Nil(t, suite.msgStore.SaveMessage(seqNum, []byte(msg)))
	}

	// Given a MessageStore with the following sender and target seqnums
	require.Nil(t, suite.msgStore.SetNextSenderMsgSeqNum(867))
	require.Nil(t, suite.msgStore.SetNextTargetMsgSeqNum(5309))

	require.Nil(suite.T(), suite.msgStore.Close())

	// check for panic
	err := suite.msgStore.SetNextSenderMsgSeqNum(867)
	require.Equal(t, err, ErrAccessToClosedStore)
	err = suite.msgStore.SetNextTargetMsgSeqNum(5309)
	require.Equal(t, err, ErrAccessToClosedStore)
	_, err = suite.msgStore.GetMessages(0, 100)
	require.Equal(t, err, ErrAccessToClosedStore)
	err = suite.msgStore.Refresh()
	require.Equal(t, err, ErrAccessToClosedStore)
	err = suite.msgStore.Reset()
	require.Equal(t, err, ErrAccessToClosedStore)
}

func (suite *MessageStoreTestSuite) TestMessageTxStore_SaveMessageWithTx() {
	t := suite.T()

	createMsgFn := func() *Message {
		message := NewMessage()
		message.Header.SetString(35, "BE")
		message.Body.SetString(923, "testID")
		message.Body.SetInt(924, 1)
		message.Body.SetString(553, "test")
		sessionID := SessionID{}
		fillDefaultHeader(message, nil, sessionID, Seconds)
		return message
	}

	reqMsg1 := createMsgFn()
	reqMsg2 := createMsgFn()
	reqMsg3 := createMsgFn()
	data := BuildMessageInput{}
	arr := []*Message{reqMsg1.ToMessage(), reqMsg2.ToMessage(), reqMsg3.ToMessage()}
	for i, msg := range arr {
		tmpData := data
		tmpData.Msg = msg
		_, err := suite.msgStore.SaveMessageWithTx(&tmpData)
		assert.NoError(t, err)
		assert.Equal(t, 2+i, suite.msgStore.NextSenderMsgSeqNum())
	}
}

func (suite *MessageStoreTestSuite) TestMessageTxStore_SaveMessageWithTx_ResetLogon() {
	t := suite.T()

	createMsgFn := func() *Message {
		message := NewMessage()
		message.Header.SetString(35, "A") // Logon
		message.Body.SetInt(98, 0)
		message.Body.SetInt(108, 30)
		sessionID := SessionID{}
		fillDefaultHeader(message, nil, sessionID, Seconds)
		return message
	}

	msg := createMsgFn()
	data := BuildMessageInput{
		Msg:           msg,
		IsResetSeqNum: true,
	}
	msg.Body.SetBool(141, true) // reset

	_, err := suite.msgStore.SaveMessageWithTx(&data)
	assert.NoError(t, err)
	assert.Equal(t, 2, suite.msgStore.NextSenderMsgSeqNum())

	_, err = suite.msgStore.SaveMessageWithTx(&data)
	assert.NoError(t, err)
	assert.Equal(t, 2, suite.msgStore.NextSenderMsgSeqNum())
}
