package quickfix

import (
	"errors"
	"time"
)

// BuildMessageInput stores for building message data.
type BuildMessageInput struct {
	Msg                          *Message
	InReplyTo                    *Message
	EnableLastMsgSeqNumProcessed bool
	IsResetSeqNum                bool
}

// BuildMessageOutput stores build message output data.
type BuildMessageOutput struct {
	MsgBytes  []byte
	Msg       *Message
	SeqNum    int
	SentReset bool
}

// MsgSeqNumCursor interface provides methods for referencing/resetting the SeqNum position.
type MsgSeqNumCursor interface {
	NextSenderMsgSeqNum() int
	NextTargetMsgSeqNum() int

	Reset() error
}

type messageBuilder struct {
	cursor MsgSeqNumCursor
}

func newMessageBuilder(store MsgSeqNumCursor) *messageBuilder {
	return &messageBuilder{store}
}

// BuildMessage ...
func (m *messageBuilder) BuildMessage(bd *BuildMessageInput) (output *BuildMessageOutput, err error) {
	if m == nil || m.cursor == nil {
		err = errors.New("failed to initialize. please to set cursor")
		return
	}
	msg := bd.Msg

	if bd.EnableLastMsgSeqNumProcessed {
		if bd.InReplyTo != nil {
			if lastSeqNum, err := bd.InReplyTo.Header.GetInt(tagMsgSeqNum); err == nil {
				msg.Header.SetInt(tagLastMsgSeqNumProcessed, lastSeqNum)
			}
		} else {
			msg.Header.SetInt(tagLastMsgSeqNumProcessed, m.cursor.NextTargetMsgSeqNum()-1)
		}
	}

	outputData := BuildMessageOutput{}
	outputData.SeqNum = m.cursor.NextSenderMsgSeqNum()
	msg.Header.SetField(tagMsgSeqNum, FIXInt(outputData.SeqNum))

	if bd.IsResetSeqNum { // for Logon message's ResetSeqNumFlag
		if err = m.cursor.Reset(); err != nil {
			return
		}
		outputData.SentReset = true
		outputData.SeqNum = m.cursor.NextSenderMsgSeqNum()
		msg.Header.SetField(tagMsgSeqNum, FIXInt(outputData.SeqNum))
	}

	outputData.MsgBytes = msg.build()
	outputData.Msg = msg
	output = &outputData
	return
}

func insertSendingTime(msg *Message, sessionID SessionID, timestampPrecision TimestampPrecision) {
	sendingTime := time.Now().UTC()

	if sessionID.BeginString >= BeginStringFIX42 {
		msg.Header.SetField(tagSendingTime, FIXUTCTimestamp{Time: sendingTime, Precision: timestampPrecision})
	} else {
		msg.Header.SetField(tagSendingTime, FIXUTCTimestamp{Time: sendingTime, Precision: Seconds})
	}
}

func fillDefaultHeader(msg *Message, inReplyTo *Message, sessionID SessionID, timestampPrecision TimestampPrecision) {
	msg.Header.SetString(tagBeginString, sessionID.BeginString)
	msg.Header.SetString(tagSenderCompID, sessionID.SenderCompID)
	optionallySetID(msg, tagSenderSubID, sessionID.SenderSubID)
	optionallySetID(msg, tagSenderLocationID, sessionID.SenderLocationID)

	msg.Header.SetString(tagTargetCompID, sessionID.TargetCompID)
	optionallySetID(msg, tagTargetSubID, sessionID.TargetSubID)
	optionallySetID(msg, tagTargetLocationID, sessionID.TargetLocationID)

	insertSendingTime(msg, sessionID, timestampPrecision)
}
