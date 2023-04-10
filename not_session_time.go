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

import "github.com/cryptogarageinc/quickfix-go/internal"

type notSessionTime struct{ latentState }

func (notSessionTime) String() string      { return "Not session time" }
func (notSessionTime) IsSessionTime() bool { return false }

func (state notSessionTime) FixMsgIn(session *session, msg *Message) (nextState sessionState) {
	session.log.OnEventf("Invalid Session State: Unexpected Msg %v while in Latent state", msg)
	return state
}

func (state notSessionTime) Timeout(session *session, event internal.Event) (nextState sessionState) {
	session.log.OnEventParams("receive event",
		LogString("sessionState", state.String()), LogObject("event", event))
	return state
}

func (state notSessionTime) Stop(*session) (nextState sessionState) {
	return state
}
