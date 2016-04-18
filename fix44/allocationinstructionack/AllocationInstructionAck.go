//Package allocationinstructionack msg type = P.
package allocationinstructionack

import (
	"github.com/quickfixgo/quickfix"
	"github.com/quickfixgo/quickfix/enum"
	"github.com/quickfixgo/quickfix/fix44"
	"github.com/quickfixgo/quickfix/fix44/parties"
	"time"
)

//NoAllocs is a repeating group in AllocationInstructionAck
type NoAllocs struct {
	//AllocAccount is a non-required field for NoAllocs.
	AllocAccount *string `fix:"79"`
	//AllocAcctIDSource is a non-required field for NoAllocs.
	AllocAcctIDSource *int `fix:"661"`
	//AllocPrice is a non-required field for NoAllocs.
	AllocPrice *float64 `fix:"366"`
	//IndividualAllocID is a non-required field for NoAllocs.
	IndividualAllocID *string `fix:"467"`
	//IndividualAllocRejCode is a non-required field for NoAllocs.
	IndividualAllocRejCode *int `fix:"776"`
	//AllocText is a non-required field for NoAllocs.
	AllocText *string `fix:"161"`
	//EncodedAllocTextLen is a non-required field for NoAllocs.
	EncodedAllocTextLen *int `fix:"360"`
	//EncodedAllocText is a non-required field for NoAllocs.
	EncodedAllocText *string `fix:"361"`
}

//NewNoAllocs returns an initialized NoAllocs instance
func NewNoAllocs() *NoAllocs {
	var m NoAllocs
	return &m
}

func (m *NoAllocs) SetAllocAccount(v string)        { m.AllocAccount = &v }
func (m *NoAllocs) SetAllocAcctIDSource(v int)      { m.AllocAcctIDSource = &v }
func (m *NoAllocs) SetAllocPrice(v float64)         { m.AllocPrice = &v }
func (m *NoAllocs) SetIndividualAllocID(v string)   { m.IndividualAllocID = &v }
func (m *NoAllocs) SetIndividualAllocRejCode(v int) { m.IndividualAllocRejCode = &v }
func (m *NoAllocs) SetAllocText(v string)           { m.AllocText = &v }
func (m *NoAllocs) SetEncodedAllocTextLen(v int)    { m.EncodedAllocTextLen = &v }
func (m *NoAllocs) SetEncodedAllocText(v string)    { m.EncodedAllocText = &v }

//Message is a AllocationInstructionAck FIX Message
type Message struct {
	FIXMsgType string `fix:"P"`
	fix44.Header
	//AllocID is a required field for AllocationInstructionAck.
	AllocID string `fix:"70"`
	//Parties is a non-required component for AllocationInstructionAck.
	Parties *parties.Parties
	//SecondaryAllocID is a non-required field for AllocationInstructionAck.
	SecondaryAllocID *string `fix:"793"`
	//TradeDate is a non-required field for AllocationInstructionAck.
	TradeDate *string `fix:"75"`
	//TransactTime is a required field for AllocationInstructionAck.
	TransactTime time.Time `fix:"60"`
	//AllocStatus is a required field for AllocationInstructionAck.
	AllocStatus int `fix:"87"`
	//AllocRejCode is a non-required field for AllocationInstructionAck.
	AllocRejCode *int `fix:"88"`
	//AllocType is a non-required field for AllocationInstructionAck.
	AllocType *int `fix:"626"`
	//AllocIntermedReqType is a non-required field for AllocationInstructionAck.
	AllocIntermedReqType *int `fix:"808"`
	//MatchStatus is a non-required field for AllocationInstructionAck.
	MatchStatus *string `fix:"573"`
	//Product is a non-required field for AllocationInstructionAck.
	Product *int `fix:"460"`
	//SecurityType is a non-required field for AllocationInstructionAck.
	SecurityType *string `fix:"167"`
	//Text is a non-required field for AllocationInstructionAck.
	Text *string `fix:"58"`
	//EncodedTextLen is a non-required field for AllocationInstructionAck.
	EncodedTextLen *int `fix:"354"`
	//EncodedText is a non-required field for AllocationInstructionAck.
	EncodedText *string `fix:"355"`
	//NoAllocs is a non-required field for AllocationInstructionAck.
	NoAllocs []NoAllocs `fix:"78,omitempty"`
	fix44.Trailer
}

//Marshal converts Message to a quickfix.Message instance
func (m Message) Marshal() quickfix.Message { return quickfix.Marshal(m) }

func (m *Message) SetAllocID(v string)           { m.AllocID = v }
func (m *Message) SetParties(v parties.Parties)  { m.Parties = &v }
func (m *Message) SetSecondaryAllocID(v string)  { m.SecondaryAllocID = &v }
func (m *Message) SetTradeDate(v string)         { m.TradeDate = &v }
func (m *Message) SetTransactTime(v time.Time)   { m.TransactTime = v }
func (m *Message) SetAllocStatus(v int)          { m.AllocStatus = v }
func (m *Message) SetAllocRejCode(v int)         { m.AllocRejCode = &v }
func (m *Message) SetAllocType(v int)            { m.AllocType = &v }
func (m *Message) SetAllocIntermedReqType(v int) { m.AllocIntermedReqType = &v }
func (m *Message) SetMatchStatus(v string)       { m.MatchStatus = &v }
func (m *Message) SetProduct(v int)              { m.Product = &v }
func (m *Message) SetSecurityType(v string)      { m.SecurityType = &v }
func (m *Message) SetText(v string)              { m.Text = &v }
func (m *Message) SetEncodedTextLen(v int)       { m.EncodedTextLen = &v }
func (m *Message) SetEncodedText(v string)       { m.EncodedText = &v }
func (m *Message) SetNoAllocs(v []NoAllocs)      { m.NoAllocs = v }

//A RouteOut is the callback type that should be implemented for routing Message
type RouteOut func(msg Message, sessionID quickfix.SessionID) quickfix.MessageRejectError

//Route returns the beginstring, message type, and MessageRoute for this Message type
func Route(router RouteOut) (string, string, quickfix.MessageRoute) {
	r := func(msg quickfix.Message, sessionID quickfix.SessionID) quickfix.MessageRejectError {
		m := new(Message)
		if err := quickfix.Unmarshal(msg, m); err != nil {
			return err
		}
		return router(*m, sessionID)
	}
	return enum.BeginStringFIX44, "P", r
}
