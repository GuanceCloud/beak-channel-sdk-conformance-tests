package conformancetests

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/websocket"

	beakwecom "github.com/GuanceCloud/beak-agent-wecom"
	wecomsdk "github.com/GuanceCloud/beak-agent-wecom/sdk"
	conformance "gitlab.jiagouyun.com/guance/beak-agent-channel-sdk/beak-channel-sdk-conformance"
)

const (
	wecomAuthOKMarker  = "wecom:auth:ok"
	wecomPongMarker    = "wecom:pong:ok"
	wecomSendAckMarker = "wecom:send:ack"
	wecomEventMarker   = "wecom:event:message"
	wecomStopMarker    = "wecom:event:disconnected"
)

func runWeComConformance(t *testing.T) {
	adapter := newWeComAdapter(t)
	trueValue := true
	falseValue := false
	conformance.Run(t, conformance.Config{
		Platform:                 beakwecom.Platform,
		MetadataProvider:         adapter,
		CredentialSchemaProvider: adapter,
		CredentialValidator:      adapter,
		InboundParser:            adapter,
		Acknowledger:             adapter,
		Sender:                   adapter,
		HostStreamer:             adapter,
		CredentialCases: []conformance.CredentialValidationCase{{
			Name: "valid bot credential uses websocket authentication",
			Request: conformance.CredentialValidationRequest{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				Credential:    map[string]any{"bot_id": "bot-conformance", "secret": "good"},
			},
			Expect: conformance.CredentialValidationExpectation{
				Valid:              true,
				AccountKey:         "bot-conformance",
				DisplayName:        "bot-conformance",
				MetadataPlatform:   beakwecom.Platform,
				RequireAccountID:   true,
				RequireBotIdentity: true,
			},
		}, {
			Name: "invalid bot secret is rejected",
			Request: conformance.CredentialValidationRequest{
				Credential: map[string]any{"bot_id": "bot-conformance", "secret": "bad"},
			},
			Expect: conformance.CredentialValidationExpectation{Valid: false},
		}},
		InboundCases: []conformance.InboundCase{{
			Name: "direct text callback",
			Fixture: conformance.InboundFixture{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				AccountUUID:   "account-1",
				Credential:    wecomCredential(),
				Raw: json.RawMessage(`{
					"msgid":"msg-direct-1",
					"aibotid":"bot-conformance",
					"chattype":"single",
					"from":{"userid":"user-1"},
					"msgtype":"text",
					"text":{"content":"hello wecom"}
				}`),
			},
			Expect: conformance.InboundExpectation{
				MinMessages:      1,
				ChatType:         conformance.ChatTypeDirect,
				ChatID:           "user-1",
				ChatIdentityID:   "user-1",
				SenderID:         "user-1",
				Text:             "hello wecom",
				MentionedMe:      &trueValue,
				MentionIDs:       []string{"bot-conformance"},
				RequireMessageID: true,
				RequireDedupeKey: true,
			},
		}, {
			Name: "group mixed callback includes quote",
			Fixture: conformance.InboundFixture{
				AccountUUID: "account-1",
				Credential:  wecomCredential(),
				Raw: json.RawMessage(`{
					"msgid":"msg-group-1",
					"aibotid":"bot-conformance",
					"chatid":"group-1",
					"chattype":"group",
					"from":{"userid":"user-2"},
					"msgtype":"mixed",
					"mixed":{"msg_item":[
						{"msgtype":"text","text":{"content":"inspect this"}},
						{"msgtype":"image","image":{"url":"https://example.test/image"}}
					]},
					"quote":{"msgtype":"voice","voice":{"content":"quoted voice"}}
				}`),
			},
			Expect: conformance.InboundExpectation{
				MinMessages:    1,
				ChatType:       conformance.ChatTypeGroup,
				ChatID:         "group-1",
				ChatIdentityID: "group-1",
				SenderID:       "user-2",
				Text:           "inspect this\n[图片]",
				MentionedMe:    &trueValue,
				MentionAll:     &falseValue,
				ReferencedMessage: &conformance.ReferencedMessageExpectation{
					Platform:    beakwecom.Platform,
					ChatType:    conformance.ChatTypeGroup,
					ChatID:      "group-1",
					MessageType: "voice",
					Text:        "quoted voice",
					RequireText: true,
				},
			},
		}},
		AckCases: []conformance.AckCase{{
			Name: "unsupported acknowledgement is explicit",
			Request: conformance.OutboundAck{
				AccountUUID: "account-1",
				ChatType:    conformance.ChatTypeDirect,
				ChatID:      "user-1",
				Mode:        "auto",
			},
			Expect: conformance.AckExpectation{Status: "unsupported", Mode: "auto"},
		}},
		SendCases: []conformance.SendCase{{
			Name: "markdown outbound exposes common send result",
			Request: conformance.OutboundMessage{
				AccountUUID: "account-1", ChatType: conformance.ChatTypeDirect, ChatID: "user-1",
				MessageUUID: "message-send-wecom", Text: "**WeCom outbound**", Format: "markdown",
			},
			Expect: conformance.SendExpectation{
				MessageID: "wecom-message-conformance", RequiredRawKeys: []string{"message_ids", "delivery_method", "chunk_count"},
			},
		}, {
			Name: "multipart retry resumes without duplicate chunks",
			Steps: []conformance.SendStep{{
				Name: "second chunk fails",
				Request: conformance.OutboundMessage{
					AccountUUID: "account-multipart", ChatType: conformance.ChatTypeDirect, ChatID: "user-1",
					MessageUUID: "message-multipart-wecom", Text: strings.Repeat("你", 10000), Format: "markdown",
					Raw: map[string]any{"conformance_scenario": "multipart_resume"},
				},
				Expect: conformance.SendExpectation{RequireError: true, ErrorContains: "temporary stream failure"},
			}, {
				Name: "retry resumes failed chunk",
				Request: conformance.OutboundMessage{
					AccountUUID: "account-multipart", ChatType: conformance.ChatTypeDirect, ChatID: "user-1",
					MessageUUID: "message-multipart-wecom", Text: strings.Repeat("你", 10000), Format: "markdown",
					Raw: map[string]any{"conformance_scenario": "multipart_resume"},
				},
				Expect: conformance.SendExpectation{RequireMessageID: true, RequiredRawKeys: []string{"message_ids", "chunk_count"}},
			}, {
				Name: "message uuid rejects changed payload",
				Request: conformance.OutboundMessage{
					AccountUUID: "account-multipart", ChatType: conformance.ChatTypeDirect, ChatID: "user-1",
					MessageUUID: "message-multipart-wecom", Text: strings.Repeat("你", 10000) + " changed", Format: "markdown",
					Raw: map[string]any{"conformance_scenario": "multipart_resume"},
				},
				Expect: conformance.SendExpectation{RequireError: true, ErrorContains: "different outbound payload"},
			}},
		}},
		HostStreamCases: []conformance.HostStreamCase{{
			Name: "authenticate, heartbeat, and receive callback",
			Request: conformance.HostStreamConnectRequest{
				Account: conformance.ChannelAccount{
					UUID:       "account-stream",
					Credential: wecomCredential(),
				},
			},
			Expect: conformance.HostStreamConnectExpectation{
				URLContains:         "ws://",
				ReadMessageType:     conformance.StreamMessageTypeText,
				RequireServiceID:    true,
				RequirePingInterval: true,
				RequirePongTimeout:  true,
				RequireState:        true,
				MinInitialFrames:    1,
				WaitForReady:        &trueValue,
				RuntimeHealth: conformance.RuntimeHealthExpectation{
					ConnectionState:             conformance.RuntimeHealthStateReconnecting,
					RequireReconnectRequestedAt: true,
				},
			},
			Ping: &conformance.HostStreamPingCase{Expect: conformance.HostStreamPingExpectation{
				MessageType: conformance.StreamMessageTypeText,
				RequireData: true,
			}},
			Frames: []conformance.HostStreamFrameCase{{
				Name:    "authentication response makes stream ready",
				Request: conformance.StreamFrameRequest{MessageType: conformance.StreamMessageTypeText, Data: []byte(wecomAuthOKMarker)},
				Expect: conformance.HostStreamFrameExpectation{
					Ready: &trueValue,
					RuntimeHealth: conformance.RuntimeHealthExpectation{
						ConnectionState:    conformance.RuntimeHealthStateConnected,
						RequireConnectedAt: true,
					},
				},
			}, {
				Name:    "heartbeat response updates pong",
				Request: conformance.StreamFrameRequest{MessageType: conformance.StreamMessageTypeText, Data: []byte(wecomPongMarker)},
				Expect: conformance.HostStreamFrameExpectation{RuntimeHealth: conformance.RuntimeHealthExpectation{
					RequireLastActivityAt: true,
					RequireLastPongAt:     true,
				}},
			}, {
				Name:    "request response exposes opaque correlation id",
				Request: conformance.StreamFrameRequest{MessageType: conformance.StreamMessageTypeText, Data: []byte(wecomSendAckMarker)},
				Expect: conformance.HostStreamFrameExpectation{
					ResponseTo: "send-request-conformance",
					RuntimeHealth: conformance.RuntimeHealthExpectation{
						RequireLastActivityAt: true,
					},
				},
			}, {
				Name:    "message callback reaches normalized inbound",
				Request: conformance.StreamFrameRequest{MessageType: conformance.StreamMessageTypeText, Data: []byte(wecomEventMarker)},
				Expect: conformance.HostStreamFrameExpectation{
					EventType:          "message",
					EventIgnored:       &falseValue,
					RequireEventResult: true,
					RuntimeHealth: conformance.RuntimeHealthExpectation{
						RequireLastActivityAt: true,
						RequireLastEventAt:    true,
					},
				},
			}, {
				Name:    "server disconnect is terminal",
				Request: conformance.StreamFrameRequest{MessageType: conformance.StreamMessageTypeText, Data: []byte(wecomStopMarker)},
				Expect: conformance.HostStreamFrameExpectation{
					Terminal:    &trueValue,
					CloseReason: "wecom server disconnected this connection because another connection was established",
					RuntimeHealth: conformance.RuntimeHealthExpectation{
						ConnectionState: conformance.RuntimeHealthStateStopped,
					},
				},
			}},
		}},
	})
}

type wecomAdapter struct {
	t          *testing.T
	connector  wecomsdk.Connector
	hostStream wecomsdk.HostStreamConnector
	endpoint   string
	mu         sync.Mutex
	authID     string
	pingID     string
	sendStore  *wecomStore
	retrySend  *wecomRetrySendTransport
}

func newWeComAdapter(t *testing.T) *wecomAdapter {
	t.Helper()
	endpoint, closeServer := newWeComCredentialServer(t)
	t.Cleanup(closeServer)
	connector := beakwecom.NewConnector()
	hostStream, ok := connector.(wecomsdk.HostStreamConnector)
	if !ok {
		t.Fatal("wecom connector should expose HostStreamConnector")
	}
	return &wecomAdapter{
		t: t, connector: connector, hostStream: hostStream, endpoint: endpoint,
		sendStore: &wecomStore{values: map[string]map[string]any{}},
		retrySend: &wecomRetrySendTransport{t: t},
	}
}

func (a *wecomAdapter) Metadata() conformance.ConnectorMetadata {
	return convert[conformance.ConnectorMetadata](a.t, a.connector.Metadata())
}

func (a *wecomAdapter) CredentialSchema(ctx context.Context) conformance.CredentialSchema {
	return convert[conformance.CredentialSchema](a.t, a.connector.CredentialSchema(ctx))
}

func (a *wecomAdapter) ValidateCredential(ctx context.Context, req conformance.CredentialValidationRequest) (*conformance.CredentialValidationResult, error) {
	sdkReq := convert[wecomsdk.CredentialValidationRequest](a.t, req)
	sdkReq.Runtime = wecomsdk.Runtime{Native: beakwecom.NativeRuntime{WebSocketURL: a.endpoint}}
	result, err := a.connector.ValidateCredential(ctx, sdkReq)
	if result == nil {
		return nil, err
	}
	out := convert[conformance.CredentialValidationResult](a.t, result)
	return &out, err
}

func (a *wecomAdapter) ParseInbound(ctx context.Context, fixture conformance.InboundFixture) ([]conformance.InboundMessage, error) {
	account := wecomAccount(fixture.AccountUUID, fixture.WorkspaceUUID, fixture.ChannelUUID, fixture.Credential, fixture.AccountState)
	var body map[string]any
	if err := json.Unmarshal(fixture.Raw, &body); err != nil {
		return nil, err
	}
	frame := wecomTestFrame{Command: "aibot_msg_callback", Headers: wecomTestFrameHeaders{RequestID: "callback-conformance"}, Body: body}
	data, err := json.Marshal(frame)
	if err != nil {
		return nil, err
	}
	result, err := a.hostStream.HandleStreamFrame(ctx, wecomRuntime(account, a.endpoint), account, wecomsdk.StreamFrameRequest{
		MessageType: wecomsdk.StreamMessageTypeText,
		Data:        data,
	})
	if err != nil || result == nil || result.EventResult == nil || result.EventResult.Ignored || result.EventResult.Inbound == nil {
		return nil, err
	}
	return []conformance.InboundMessage{convert[conformance.InboundMessage](a.t, result.EventResult.Inbound)}, nil
}

func (a *wecomAdapter) Acknowledge(ctx context.Context, req conformance.OutboundAck) (*conformance.AckResult, error) {
	result, err := a.connector.Acknowledge(ctx, wecomsdk.Runtime{}, convert[wecomsdk.OutboundAck](a.t, req))
	if result == nil {
		return nil, err
	}
	out := convert[conformance.AckResult](a.t, result)
	return &out, err
}

func (a *wecomAdapter) Send(ctx context.Context, req conformance.OutboundMessage) (*conformance.SendResult, error) {
	account := wecomAccount(req.AccountUUID, req.WorkspaceUUID, req.ChannelUUID, wecomCredential(), map[string]any{})
	store := &wecomStore{values: map[string]map[string]any{}}
	var transport wecomsdk.StreamTransport = wecomSendTransport{t: a.t}
	if req.Raw["conformance_scenario"] == "multipart_resume" {
		store = a.sendStore
		transport = a.retrySend
	}
	result, err := a.connector.Send(ctx, wecomsdk.Runtime{
		Account: account, Accounts: []wecomsdk.ChannelAccount{account},
		AccountStore: store,
		Stream:       transport,
	}, convert[wecomsdk.OutboundMessage](a.t, req))
	if result == nil {
		return nil, err
	}
	out := convert[conformance.SendResult](a.t, result)
	return &out, err
}

func (a *wecomAdapter) ConnectStream(ctx context.Context, req conformance.HostStreamConnectRequest) (*conformance.StreamConnectResult, error) {
	account := wecomAccount(req.Account.UUID, req.WorkspaceUUID, req.ChannelUUID, req.Account.Credential, req.Account.State)
	result, err := a.hostStream.ConnectStream(ctx, wecomRuntime(account, a.endpoint), account)
	if result == nil {
		return nil, err
	}
	if len(result.InitialFrames) > 0 {
		frame, parseErr := parseWeComTestFrame(result.InitialFrames[0].Data)
		if parseErr != nil {
			return nil, parseErr
		}
		a.mu.Lock()
		a.authID = frame.Headers.RequestID
		a.mu.Unlock()
	}
	initialFrames := make([]conformance.StreamFrame, 0, len(result.InitialFrames))
	for _, frame := range result.InitialFrames {
		initialFrames = append(initialFrames, conformance.StreamFrame{MessageType: frame.MessageType, Data: frame.Data})
	}
	return &conformance.StreamConnectResult{
		URL:             result.URL,
		Headers:         result.Headers,
		ServiceID:       result.ServiceID,
		ReadMessageType: result.ReadMessageType,
		InitialFrames:   initialFrames,
		WaitForReady:    result.WaitForReady,
		PingInterval:    result.PingInterval,
		PongTimeout:     result.PongTimeout,
		State:           result.State,
		HealthUpdates:   result.HealthUpdates,
	}, err
}

func (a *wecomAdapter) BuildStreamPing(ctx context.Context, req conformance.StreamPingRequest) (*conformance.StreamFrame, error) {
	result, err := a.hostStream.BuildStreamPing(ctx, wecomsdk.StreamPingRequest{ServiceID: req.ServiceID, State: req.State})
	if result == nil {
		return nil, err
	}
	frame, parseErr := parseWeComTestFrame(result.Data)
	if parseErr != nil {
		return nil, parseErr
	}
	a.mu.Lock()
	a.pingID = frame.Headers.RequestID
	a.mu.Unlock()
	return &conformance.StreamFrame{MessageType: result.MessageType, Data: result.Data}, err
}

func (a *wecomAdapter) HandleStreamFrame(ctx context.Context, req conformance.StreamFrameRequest) (*conformance.StreamFrameResult, error) {
	account := wecomAccount(req.Account.UUID, req.WorkspaceUUID, req.ChannelUUID, req.Account.Credential, req.Account.State)
	data := req.Data
	a.mu.Lock()
	authID, pingID := a.authID, a.pingID
	a.mu.Unlock()
	switch string(req.Data) {
	case wecomAuthOKMarker:
		data = mustWeComFrame(a.t, wecomTestFrame{Headers: wecomTestFrameHeaders{RequestID: authID}, ErrCode: 0})
	case wecomPongMarker:
		data = mustWeComFrame(a.t, wecomTestFrame{Headers: wecomTestFrameHeaders{RequestID: pingID}, ErrCode: 0})
	case wecomSendAckMarker:
		data = mustWeComFrame(a.t, wecomTestFrame{Headers: wecomTestFrameHeaders{RequestID: "send-request-conformance"}, ErrCode: 0})
	case wecomEventMarker:
		data = mustWeComFrame(a.t, wecomTestFrame{
			Command: "aibot_msg_callback",
			Headers: wecomTestFrameHeaders{RequestID: "callback-stream"},
			Body: map[string]any{
				"msgid":    "msg-stream-1",
				"aibotid":  "bot-conformance",
				"chattype": "single",
				"from":     map[string]any{"userid": "user-stream"},
				"msgtype":  "text",
				"text":     map[string]any{"content": "stream hello"},
			},
		})
	case wecomStopMarker:
		data = mustWeComFrame(a.t, wecomTestFrame{
			Command: "aibot_event_callback",
			Headers: wecomTestFrameHeaders{RequestID: "event-disconnected"},
			Body: map[string]any{
				"msgid":    "event-1",
				"aibotid":  "bot-conformance",
				"msgtype":  "event",
				"event":    map[string]any{"eventtype": "disconnected_event"},
				"from":     map[string]any{"userid": "system"},
				"chattype": "single",
			},
		})
	}
	result, err := a.hostStream.HandleStreamFrame(ctx, wecomRuntime(account, a.endpoint), account, wecomsdk.StreamFrameRequest{
		MessageType: req.MessageType,
		Data:        data,
		ServiceID:   req.ServiceID,
		State:       req.State,
	})
	if result == nil {
		return nil, err
	}
	out := &conformance.StreamFrameResult{
		HealthUpdates: result.HealthUpdates,
		CloseReason:   result.CloseReason,
		State:         result.State,
		ResponseTo:    result.ResponseTo,
		Ready:         result.Ready,
		Terminal:      result.Terminal,
	}
	for _, frame := range result.ResponseFrames {
		out.ResponseFrames = append(out.ResponseFrames, conformance.StreamFrame{MessageType: frame.MessageType, Data: frame.Data})
	}
	if result.EventResult != nil {
		event := convert[conformance.StreamEventResult](a.t, result.EventResult)
		out.EventResult = &event
	}
	return out, err
}

func wecomCredential() map[string]any {
	return map[string]any{"bot_id": "bot-conformance", "secret": "secret-conformance", "account_id": "bot-conformance"}
}

func wecomAccount(accountUUID, workspaceUUID, channelUUID string, credential, state map[string]any) wecomsdk.ChannelAccount {
	if strings.TrimSpace(accountUUID) == "" {
		accountUUID = "account-1"
	}
	if strings.TrimSpace(workspaceUUID) == "" {
		workspaceUUID = "workspace-1"
	}
	if strings.TrimSpace(channelUUID) == "" {
		channelUUID = "channel-1"
	}
	if len(credential) == 0 {
		credential = wecomCredential()
	}
	return wecomsdk.ChannelAccount{UUID: accountUUID, WorkspaceUUID: workspaceUUID, ChannelUUID: channelUUID, Platform: beakwecom.Platform, Credential: credential, State: state}
}

func wecomRuntime(account wecomsdk.ChannelAccount, endpoint string) wecomsdk.Runtime {
	gateway := &wecomGateway{}
	return wecomsdk.Runtime{
		WorkspaceUUID: account.WorkspaceUUID,
		Channel:       wecomsdk.Channel{UUID: account.ChannelUUID, WorkspaceUUID: account.WorkspaceUUID, Platform: beakwecom.Platform},
		Account:       account,
		Accounts:      []wecomsdk.ChannelAccount{account},
		Gateway:       gateway,
		AccountStore:  &wecomStore{values: map[string]map[string]any{}},
		Native:        beakwecom.NativeRuntime{WebSocketURL: endpoint},
	}
}

type wecomGateway struct{}

func (*wecomGateway) EnsureChannel(context.Context, wecomsdk.EnsureChannelRequest) (string, error) {
	return "channel-1", nil
}

func (*wecomGateway) EnsureChannelLinkSession(context.Context, wecomsdk.EnsureChannelLinkSessionRequest) (string, error) {
	return "link-session", nil
}

func (*wecomGateway) EnsureChatSession(context.Context, wecomsdk.EnsureChatSessionRequest) (string, error) {
	return "chat-session", nil
}

func (*wecomGateway) CreateMessage(context.Context, wecomsdk.CreateMessageRequest) (string, error) {
	return "message-uuid", nil
}

func (*wecomGateway) StreamSession(context.Context, wecomsdk.StreamSessionRequest, func(wecomsdk.StreamEvent) error) error {
	return nil
}

func (*wecomGateway) AgentParticipantID() string { return "agent-1" }

func (*wecomGateway) BridgeParticipantID(platform string) string {
	return wecomsdk.BridgeParticipantID(platform)
}

type wecomStore struct {
	mu     sync.Mutex
	values map[string]map[string]any
}

type wecomSendTransport struct {
	t *testing.T
}

type wecomRetrySendTransport struct {
	t        *testing.T
	mu       sync.Mutex
	contents []string
}

func (s *wecomRetrySendTransport) Request(_ context.Context, req wecomsdk.StreamRequest) (*wecomsdk.StreamResponse, error) {
	frame, err := parseWeComTestFrame(req.Frame.Data)
	if err != nil {
		return nil, err
	}
	markdown, _ := frame.Body["markdown"].(map[string]any)
	content := stringValue(markdown["content"])
	s.mu.Lock()
	s.contents = append(s.contents, content)
	call := len(s.contents)
	contents := append([]string(nil), s.contents...)
	s.mu.Unlock()
	if call == 2 {
		return nil, errors.New("temporary stream failure")
	}
	if call == 3 && (contents[2] != contents[1] || contents[2] == contents[0]) {
		return nil, errors.New("multipart retry did not resume the failed chunk")
	}
	ack := mustWeComFrame(s.t, wecomTestFrame{
		Headers: wecomTestFrameHeaders{RequestID: req.CorrelationID},
		Body:    map[string]any{"msgid": "wecom-message-multipart"},
	})
	return &wecomsdk.StreamResponse{
		Frame:         wecomsdk.StreamFrame{MessageType: wecomsdk.StreamMessageTypeText, Data: ack},
		CorrelationID: req.CorrelationID,
	}, nil
}

func (s wecomSendTransport) Request(_ context.Context, req wecomsdk.StreamRequest) (*wecomsdk.StreamResponse, error) {
	frame, err := parseWeComTestFrame(req.Frame.Data)
	if err != nil {
		return nil, err
	}
	if frame.Headers.RequestID != req.CorrelationID {
		s.t.Fatalf("wecom send correlation id = %q, want %q", frame.Headers.RequestID, req.CorrelationID)
	}
	ack := mustWeComFrame(s.t, wecomTestFrame{
		Headers: wecomTestFrameHeaders{RequestID: req.CorrelationID},
		Body:    map[string]any{"msgid": "wecom-message-conformance"},
	})
	return &wecomsdk.StreamResponse{
		Frame:         wecomsdk.StreamFrame{MessageType: wecomsdk.StreamMessageTypeText, Data: ack},
		CorrelationID: req.CorrelationID,
	}, nil
}

func (s *wecomStore) LoadChannelAccountState(_ context.Context, accountUUID string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneAnyMap(s.values[accountUUID]), nil
}

func (s *wecomStore) SaveChannelAccountState(_ context.Context, accountUUID string, state map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.values[accountUUID] == nil {
		s.values[accountUUID] = map[string]any{}
	}
	for key, value := range state {
		s.values[accountUUID][key] = value
	}
	return nil
}

func cloneAnyMap(values map[string]any) map[string]any {
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func newWeComCredentialServer(t *testing.T) (string, func()) {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		frame, err := parseWeComTestFrame(data)
		if err != nil {
			return
		}
		code, message := 0, "ok"
		if frame.Body["secret"] == "bad" {
			code, message = 40001, "invalid secret"
		}
		_ = conn.WriteMessage(websocket.TextMessage, mustWeComFrame(t, wecomTestFrame{
			Headers: wecomTestFrameHeaders{RequestID: frame.Headers.RequestID},
			ErrCode: code,
			ErrMsg:  message,
		}))
	}))
	return "ws" + strings.TrimPrefix(server.URL, "http"), server.Close
}

type wecomTestFrame struct {
	Command string                `json:"cmd,omitempty"`
	Headers wecomTestFrameHeaders `json:"headers"`
	Body    map[string]any        `json:"body,omitempty"`
	ErrCode int                   `json:"errcode,omitempty"`
	ErrMsg  string                `json:"errmsg,omitempty"`
}

type wecomTestFrameHeaders struct {
	RequestID string `json:"req_id"`
}

func parseWeComTestFrame(data []byte) (wecomTestFrame, error) {
	var frame wecomTestFrame
	err := json.Unmarshal(data, &frame)
	return frame, err
}

func mustWeComFrame(t *testing.T, frame wecomTestFrame) []byte {
	t.Helper()
	data, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
