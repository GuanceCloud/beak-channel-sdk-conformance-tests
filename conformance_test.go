package conformancetests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	beakdingtalk "github.com/GuanceCloud/beak-agent-channel-dingtalk"
	dingtalksdk "github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
	beaklark "github.com/GuanceCloud/beak-agent-channel-lark"
	larksdk "github.com/GuanceCloud/beak-agent-channel-lark/sdk"
	beakweixin "github.com/GuanceCloud/beak-agent-channel-wechat"
	wechatsdk "github.com/GuanceCloud/beak-agent-channel-wechat/sdk"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
	dtpayload "github.com/open-dingtalk/dingtalk-stream-sdk-go/payload"
	dtutils "github.com/open-dingtalk/dingtalk-stream-sdk-go/utils"
	conformance "gitlab.jiagouyun.com/guance/beak-agent-channel-sdk/beak-channel-sdk-conformance"
)

func TestBeakSDKConformance(t *testing.T) {
	t.Run("dingtalk", func(t *testing.T) {
		runDingTalkConformance(t)
	})
	t.Run("lark", func(t *testing.T) {
		runLarkConformance(t)
	})
	t.Run("weixin", func(t *testing.T) {
		runWeixinConformance(t)
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func jsonResponse(body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func convert[T any](t *testing.T, value any) T {
	t.Helper()
	var out T
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal conformance value: %v", err)
	}
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal conformance value: %v", err)
	}
	return out
}

func firstString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return ""
	}
}

func runDingTalkConformance(t *testing.T) {
	adapter := newDingTalkAdapter(t)
	trueValue := true
	falseValue := false

	conformance.Run(t, conformance.Config{
		Platform:                 beakdingtalk.Platform,
		MetadataProvider:         adapter,
		CredentialSchemaProvider: adapter,
		CredentialValidator:      adapter,
		InboundParser:            adapter,
		Acknowledger:             adapter,
		HostStreamer:             adapter,
		HostStreamCases: []conformance.HostStreamCase{{
			Name: "connect, ping, and system frames",
			Request: conformance.HostStreamConnectRequest{
				Account: conformance.ChannelAccount{
					UUID:       "account-stream",
					Credential: dingtalkCredential("account-stream"),
					State:      map[string]any{},
				},
			},
			Expect: conformance.HostStreamConnectExpectation{
				URLContains:            "wss://dingtalk-stream.test/connect?ticket=ticket-conformance",
				ReadMessageType:        conformance.StreamMessageTypeText,
				RequirePingInterval:    true,
				RequirePongTimeout:     true,
				RequireConnectedHealth: true,
			},
			Ping: &conformance.HostStreamPingCase{
				Expect: conformance.HostStreamPingExpectation{MessageType: conformance.StreamMessageTypePing},
			},
			Frames: []conformance.HostStreamFrameCase{{
				Name: "system ping returns pong and records health",
				Request: conformance.StreamFrameRequest{
					MessageType: conformance.StreamMessageTypeText,
					Data:        dingtalkSystemFrame("ping", "dt-ping-conformance"),
				},
				Expect: conformance.HostStreamFrameExpectation{
					MinResponseFrames:   1,
					ResponseMessageType: conformance.StreamMessageTypeText,
					RuntimeHealth: conformance.RuntimeHealthExpectation{
						RequireLastActivityAt: true,
						RequireLastPingAt:     true,
					},
				},
			}, {
				Name: "system disconnect asks host to close",
				Request: conformance.StreamFrameRequest{
					MessageType: conformance.StreamMessageTypeText,
					Data:        dingtalkSystemFrame("disconnect", "dt-disconnect-conformance"),
				},
				Expect: conformance.HostStreamFrameExpectation{
					CloseReason: "disconnect",
					RuntimeHealth: conformance.RuntimeHealthExpectation{
						RequireDisconnectedAt: true,
					},
				},
			}},
		}},
		CredentialCases: []conformance.CredentialValidationCase{{
			Name: "valid credential exposes stable account identity",
			Request: conformance.CredentialValidationRequest{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				Credential: map[string]any{
					"client_id":     "client_conformance",
					"client_secret": "secret_conformance",
					"robot_code":    "robot_conformance",
				},
			},
			Expect: conformance.CredentialValidationExpectation{
				Valid:              true,
				AccountKey:         "robot_conformance",
				DisplayName:        "robot_conformance",
				RequireAccountID:   true,
				RequireBotIdentity: true,
			},
		}, {
			Name: "invalid credential fails without creating account",
			Request: conformance.CredentialValidationRequest{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				Credential: map[string]any{
					"client_id":     "client_bad",
					"client_secret": "bad",
					"robot_code":    "robot_bad",
				},
			},
			Expect: conformance.CredentialValidationExpectation{Valid: false},
		}},
		InboundCases: []conformance.InboundCase{{
			Name: "mention_all does not imply mentioned_me",
			Fixture: conformance.InboundFixture{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				AccountUUID:   "account-1",
				Credential:    dingtalkCredential("account-1"),
				Raw: json.RawMessage(`{
					"conversationType":"2",
					"conversationId":"cid-group",
					"conversationTitle":"Team",
					"senderStaffId":"staff-1",
					"senderNick":"Alice",
					"msgId":"msg-at-all-conformance",
					"msgtype":"text",
					"isInAtList":false,
					"text":{"content":"hello all","at":{"isAtAll":true}},
					"robotCode":"robot-1"
				}`),
			},
			Expect: conformance.InboundExpectation{
				ChatType:          conformance.ChatTypeGroup,
				ChatID:            "cid-group",
				ChatDisplayName:   "Team",
				ChatIdentityID:    "cid-group",
				SenderID:          "staff-1",
				SenderDisplayName: "Alice",
				Text:              "hello all",
				MentionedMe:       &falseValue,
				MentionAll:        &trueValue,
				RequireMessageID:  true,
			},
		}, {
			Name: "only bot mention follow up is delivered",
			Fixture: conformance.InboundFixture{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				AccountUUID:   "account-1",
				Credential:    dingtalkCredential("account-1"),
				Raw: json.RawMessage(`{
					"conversationType":"2",
					"conversationId":"cid-group",
					"conversationTitle":"Team",
					"senderStaffId":"staff-1",
					"senderNick":"Alice",
					"msgId":"msg-only-bot-conformance",
					"msgtype":"text",
					"isInAtList":true,
					"text":{"content":"","at":{}},
					"chatbotUserId":"bot-1",
					"robotCode":"robot-1"
				}`),
			},
			Expect: conformance.InboundExpectation{
				ChatType:          conformance.ChatTypeGroup,
				ChatID:            "cid-group",
				ChatDisplayName:   "Team",
				ChatIdentityID:    "cid-group",
				SenderID:          "staff-1",
				SenderDisplayName: "Alice",
				TextTrimmedEmpty:  &trueValue,
				MentionedMe:       &trueValue,
				RequireMessageID:  true,
			},
		}, {
			Name: "reply exposes referenced message",
			Fixture: conformance.InboundFixture{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				AccountUUID:   "account-1",
				Credential:    dingtalkCredential("account-1"),
				Raw: json.RawMessage(`{
					"conversationType":"2",
					"conversationId":"cid-group",
					"conversationTitle":"Team",
					"senderStaffId":"staff-1",
					"senderNick":"Alice",
					"msgId":"msg-reply-conformance",
					"msgtype":"reply",
					"isInAtList":false,
					"content":"current reply",
					"isReplyMsg":true,
					"repliedMsg":{
						"msgId":"quoted-dingtalk-conformance",
						"msgType":"text",
						"senderId":"staff-2",
						"senderNick":"Bob",
						"content":{"text":"quoted dingtalk"}
					},
					"robotCode":"robot-1"
				}`),
			},
			Expect: conformance.InboundExpectation{
				ChatType:          conformance.ChatTypeGroup,
				ChatID:            "cid-group",
				ChatDisplayName:   "Team",
				ChatIdentityID:    "cid-group",
				SenderID:          "staff-1",
				SenderDisplayName: "Alice",
				Text:              "current reply",
				ReferencedMessage: &conformance.ReferencedMessageExpectation{
					Platform:          beakdingtalk.Platform,
					MessageID:         "quoted-dingtalk-conformance",
					ChatType:          conformance.ChatTypeGroup,
					ChatID:            "cid-group",
					SenderID:          "staff-2",
					SenderDisplayName: "Bob",
					MessageType:       "text",
					Text:              "quoted dingtalk",
				},
				RequireMessageID: true,
			},
		}},
		AckCases: []conformance.AckCase{{
			Name: "processing ack is unsupported without sending a message",
			Request: conformance.OutboundAck{
				AccountUUID:     "account-1",
				ChatType:        conformance.ChatTypeGroup,
				ChatID:          "cid-group",
				TargetMessageID: "msg-conformance",
				Action:          "start",
			},
			Expect: conformance.AckExpectation{
				Status: "unsupported",
				Mode:   "unsupported",
			},
		}},
	})

	t.Run("runtime health start is host-owned and event updates activity", func(t *testing.T) {
		store := newDingTalkStore()
		account := dingtalksdk.ChannelAccount{
			UUID:          "account-health",
			WorkspaceUUID: "workspace-1",
			ChannelUUID:   "channel-1",
			Platform:      beakdingtalk.Platform,
			Credential:    dingtalkCredential("account-health"),
			State:         map[string]any{},
		}
		startErr := make(chan error, 1)
		go func() {
			startErr <- adapter.connector.Start(context.Background(), dingtalksdk.Runtime{
				WorkspaceUUID: "workspace-1",
				Channel:       dingtalksdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: beakdingtalk.Platform},
				Account:       account,
				Gateway:       &dingtalkGateway{},
				AccountStore:  store,
			})
		}()
		select {
		case err := <-startErr:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("dingtalk Start blocked; stream connection must stay host-owned")
		}
		_, err := adapter.event.HandleEvent(context.Background(), dingtalksdk.Runtime{
			WorkspaceUUID: "workspace-1",
			Channel:       dingtalksdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: beakdingtalk.Platform},
			Account:       account,
			Gateway:       &dingtalkGateway{},
			AccountStore:  store,
		}, account, []byte(`{
			"conversationType":"2",
			"conversationId":"cid-health",
			"conversationTitle":"Team",
			"senderStaffId":"staff-1",
			"senderNick":"Alice",
			"msgId":"msg-health",
			"msgtype":"text",
			"isInAtList":true,
			"text":{"content":"hello","at":{}},
			"chatbotUserId":"bot-1",
			"robotCode":"robot-1"
		}`))
		if err != nil {
			t.Fatal(err)
		}
		conformance.AssertRuntimeHealthState(t, store.state("account-health"), conformance.RuntimeHealthExpectation{
			RequireLastActivityAt: true,
			RequireLastEventAt:    true,
		})
	})
}

type dingtalkAdapter struct {
	t          *testing.T
	connector  dingtalksdk.Connector
	event      beakdingtalk.EventConnector
	hostStream dingtalksdk.HostStreamConnector
	httpClient *http.Client
}

func newDingTalkAdapter(t *testing.T) dingtalkAdapter {
	t.Helper()
	connector := beakdingtalk.NewConnector()
	event, ok := connector.(beakdingtalk.EventConnector)
	if !ok {
		t.Fatal("dingtalk connector should expose EventConnector")
	}
	hostStream, ok := connector.(dingtalksdk.HostStreamConnector)
	if !ok {
		t.Fatal("dingtalk connector should expose HostStreamConnector")
	}
	return dingtalkAdapter{
		t:          t,
		connector:  connector,
		event:      event,
		hostStream: hostStream,
		httpClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/v1.0/oauth2/accessToken":
				var body map[string]string
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body["appSecret"] == "bad" {
					return jsonResponse(map[string]any{"code": "InvalidAppSecret", "message": "bad secret"})
				}
				return jsonResponse(map[string]any{"accessToken": "access-token-conformance", "expireIn": 3600})
			case "/v1.0/gateway/connections/open":
				var body map[string]any
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body["clientId"] != "client-1" || body["clientSecret"] != "secret-1" {
					t.Fatalf("unexpected dingtalk stream credential body=%+v", body)
				}
				return jsonResponse(map[string]any{
					"endpoint": "wss://dingtalk-stream.test/connect",
					"ticket":   "ticket-conformance",
				})
			default:
				t.Fatalf("unexpected dingtalk request: %s %s", req.Method, req.URL.Path)
				return nil, nil
			}
		})},
	}
}

func (a dingtalkAdapter) Metadata() conformance.ConnectorMetadata {
	return convert[conformance.ConnectorMetadata](a.t, a.connector.Metadata())
}

func (a dingtalkAdapter) CredentialSchema(ctx context.Context) conformance.CredentialSchema {
	return convert[conformance.CredentialSchema](a.t, a.connector.CredentialSchema(ctx))
}

func (a dingtalkAdapter) ValidateCredential(ctx context.Context, req conformance.CredentialValidationRequest) (*conformance.CredentialValidationResult, error) {
	sdkReq := convert[dingtalksdk.CredentialValidationRequest](a.t, req)
	sdkReq.Runtime = dingtalksdk.Runtime{HTTPClient: a.httpClient}
	result, err := a.connector.ValidateCredential(ctx, sdkReq)
	if result == nil {
		return nil, err
	}
	out := convert[conformance.CredentialValidationResult](a.t, result)
	return &out, err
}

func (a dingtalkAdapter) ParseInbound(ctx context.Context, fixture conformance.InboundFixture) ([]conformance.InboundMessage, error) {
	account := dingtalkAccount(fixture)
	result, err := a.event.HandleEvent(ctx, dingtalksdk.Runtime{
		WorkspaceUUID: firstString(fixture.WorkspaceUUID, "workspace-1"),
		Channel: dingtalksdk.Channel{
			UUID:          firstString(fixture.ChannelUUID, "channel-1"),
			WorkspaceUUID: firstString(fixture.WorkspaceUUID, "workspace-1"),
			Platform:      beakdingtalk.Platform,
		},
		Account:      account,
		Gateway:      &dingtalkGateway{},
		AccountStore: newDingTalkStore(),
	}, account, fixture.Raw)
	if err != nil || result == nil || result.Ignored || result.Inbound == nil {
		return nil, err
	}
	return []conformance.InboundMessage{convert[conformance.InboundMessage](a.t, result.Inbound)}, nil
}

func (a dingtalkAdapter) Acknowledge(ctx context.Context, req conformance.OutboundAck) (*conformance.AckResult, error) {
	sdkReq := convert[dingtalksdk.OutboundAck](a.t, req)
	account := dingtalksdk.ChannelAccount{
		UUID:       firstString(req.AccountUUID, "account-1"),
		Platform:   beakdingtalk.Platform,
		Credential: dingtalkCredential(firstString(req.AccountUUID, "account-1")),
	}
	result, err := a.connector.Acknowledge(ctx, dingtalksdk.Runtime{
		Account:    account,
		HTTPClient: a.httpClient,
	}, sdkReq)
	if result == nil {
		return nil, err
	}
	out := convert[conformance.AckResult](a.t, result)
	return &out, err
}

func (a dingtalkAdapter) ConnectStream(ctx context.Context, req conformance.HostStreamConnectRequest) (*conformance.StreamConnectResult, error) {
	account := dingtalkStreamAccount(req)
	result, err := a.hostStream.ConnectStream(ctx, dingtalkRuntime(req.WorkspaceUUID, req.ChannelUUID, account, a.httpClient), account)
	if result == nil {
		return nil, err
	}
	return &conformance.StreamConnectResult{
		URL:             result.URL,
		Headers:         result.Headers,
		ServiceID:       result.ServiceID,
		ReadMessageType: result.ReadMessageType,
		PingInterval:    result.PingInterval,
		PongTimeout:     result.PongTimeout,
		State:           result.State,
		HealthUpdates:   result.HealthUpdates,
	}, err
}

func (a dingtalkAdapter) BuildStreamPing(ctx context.Context, req conformance.StreamPingRequest) (*conformance.StreamFrame, error) {
	result, err := a.hostStream.BuildStreamPing(ctx, dingtalksdk.StreamPingRequest{
		ServiceID: req.ServiceID,
		State:     req.State,
	})
	if result == nil {
		return nil, err
	}
	return &conformance.StreamFrame{MessageType: result.MessageType, Data: result.Data}, err
}

func (a dingtalkAdapter) HandleStreamFrame(ctx context.Context, req conformance.StreamFrameRequest) (*conformance.StreamFrameResult, error) {
	account := dingtalkFrameAccount(req)
	result, err := a.hostStream.HandleStreamFrame(ctx, dingtalkRuntime(req.WorkspaceUUID, req.ChannelUUID, account, a.httpClient), account, dingtalksdk.StreamFrameRequest{
		MessageType: req.MessageType,
		Data:        req.Data,
		ServiceID:   req.ServiceID,
		State:       req.State,
	})
	if result == nil {
		return nil, err
	}
	return dingtalkStreamFrameResult(a.t, result), err
}

func dingtalkAccount(fixture conformance.InboundFixture) dingtalksdk.ChannelAccount {
	credential := fixture.Credential
	if len(credential) == 0 {
		credential = dingtalkCredential(firstString(fixture.AccountUUID, "account-1"))
	}
	return dingtalksdk.ChannelAccount{
		UUID:          firstString(fixture.AccountUUID, "account-1"),
		WorkspaceUUID: firstString(fixture.WorkspaceUUID, "workspace-1"),
		ChannelUUID:   firstString(fixture.ChannelUUID, "channel-1"),
		Platform:      beakdingtalk.Platform,
		Credential:    credential,
		State:         fixture.AccountState,
	}
}

func dingtalkStreamAccount(req conformance.HostStreamConnectRequest) dingtalksdk.ChannelAccount {
	credential := req.Account.Credential
	if len(credential) == 0 {
		credential = req.Credential
	}
	if len(credential) == 0 {
		credential = dingtalkCredential(firstString(req.Account.UUID, "account-1"))
	}
	state := req.Account.State
	if state == nil {
		state = req.State
	}
	return dingtalksdk.ChannelAccount{
		UUID:          firstString(req.Account.UUID, "account-1"),
		WorkspaceUUID: firstString(req.Account.WorkspaceUUID, req.WorkspaceUUID, "workspace-1"),
		ChannelUUID:   firstString(req.Account.ChannelUUID, req.ChannelUUID, "channel-1"),
		Platform:      beakdingtalk.Platform,
		Credential:    credential,
		State:         state,
	}
}

func dingtalkFrameAccount(req conformance.StreamFrameRequest) dingtalksdk.ChannelAccount {
	connectReq := conformance.HostStreamConnectRequest{
		WorkspaceUUID: req.WorkspaceUUID,
		ChannelUUID:   req.ChannelUUID,
		Account:       req.Account,
		Credential:    req.Credential,
	}
	return dingtalkStreamAccount(connectReq)
}

func dingtalkRuntime(workspaceUUID, channelUUID string, account dingtalksdk.ChannelAccount, httpClient *http.Client) dingtalksdk.Runtime {
	workspaceUUID = firstString(workspaceUUID, account.WorkspaceUUID, "workspace-1")
	channelUUID = firstString(channelUUID, account.ChannelUUID, "channel-1")
	return dingtalksdk.Runtime{
		WorkspaceUUID: workspaceUUID,
		Channel:       dingtalksdk.Channel{UUID: channelUUID, WorkspaceUUID: workspaceUUID, Platform: beakdingtalk.Platform},
		Account:       account,
		Gateway:       &dingtalkGateway{},
		AccountStore:  newDingTalkStore(),
		HTTPClient:    httpClient,
	}
}

func dingtalkStreamFrameResult(t *testing.T, result *dingtalksdk.StreamFrameResult) *conformance.StreamFrameResult {
	t.Helper()
	out := &conformance.StreamFrameResult{
		HealthUpdates: result.HealthUpdates,
		CloseReason:   result.CloseReason,
		State:         result.State,
	}
	for _, frame := range result.ResponseFrames {
		out.ResponseFrames = append(out.ResponseFrames, conformance.StreamFrame{
			MessageType: frame.MessageType,
			Data:        frame.Data,
		})
	}
	if result.EventResult != nil {
		event := convert[conformance.StreamEventResult](t, result.EventResult)
		out.EventResult = &event
	}
	return out
}

func dingtalkSystemFrame(topic, messageID string) []byte {
	frame := &dtpayload.DataFrame{
		SpecVersion: "1.0",
		Type:        dtutils.SubscriptionTypeKSystem,
		Time:        time.Now().UnixMilli(),
		Headers: dtpayload.DataFrameHeader{
			dtpayload.DataFrameHeaderKTopic:     topic,
			dtpayload.DataFrameHeaderKMessageId: messageID,
		},
		Data: "{}",
	}
	return frame.Encode()
}

func dingtalkCredential(accountUUID string) map[string]any {
	return map[string]any{
		"account_id":      firstString(accountUUID, "account-1"),
		"client_id":       "client-1",
		"client_secret":   "secret-1",
		"robot_code":      "robot-1",
		"chatbot_user_id": "bot-1",
		"chatbot_corp_id": "corp-1",
		"base_url":        "https://dingtalk.test",
	}
}

type dingtalkGateway struct {
	mu       sync.Mutex
	messages []dingtalksdk.CreateMessageRequest
}

func (g *dingtalkGateway) EnsureChannel(context.Context, dingtalksdk.EnsureChannelRequest) (string, error) {
	return "channel-1", nil
}

func (g *dingtalkGateway) EnsureChannelLinkSession(context.Context, dingtalksdk.EnsureChannelLinkSessionRequest) (string, error) {
	return "link-1", nil
}

func (g *dingtalkGateway) EnsureChatSession(context.Context, dingtalksdk.EnsureChatSessionRequest) (string, error) {
	return "session-1", nil
}

func (g *dingtalkGateway) CreateMessage(_ context.Context, req dingtalksdk.CreateMessageRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.messages = append(g.messages, req)
	return "message-1", nil
}

func (g *dingtalkGateway) StreamSession(context.Context, dingtalksdk.StreamSessionRequest, func(dingtalksdk.StreamEvent) error) error {
	return nil
}

func (g *dingtalkGateway) AgentParticipantID() string {
	return "agent:agent-1"
}

func (g *dingtalkGateway) BridgeParticipantID(platform string) string {
	return "bridge:" + platform
}

type dingtalkStore struct {
	mu     sync.Mutex
	states map[string]map[string]any
}

func newDingTalkStore() *dingtalkStore {
	return &dingtalkStore{states: map[string]map[string]any{}}
}

func (s *dingtalkStore) LoadChannelAccountState(_ context.Context, accountUUID string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.states[accountUUID], nil
}

func (s *dingtalkStore) SaveChannelAccountState(_ context.Context, accountUUID string, state map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[accountUUID] = state
	return nil
}

func (s *dingtalkStore) state(accountUUID string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]any, len(s.states[accountUUID]))
	for key, value := range s.states[accountUUID] {
		out[key] = value
	}
	return out
}

func runLarkConformance(t *testing.T) {
	adapter := newLarkAdapter(t)
	trueValue := true
	falseValue := false

	conformance.Run(t, conformance.Config{
		Platform:                 beaklark.Platform,
		MetadataPlatform:         beaklark.Platform,
		MetadataProvider:         adapter,
		CredentialSchemaProvider: adapter,
		CredentialValidator:      adapter,
		InboundParser:            adapter,
		Acknowledger:             adapter,
		HostStreamer:             adapter,
		HostStreamCases: []conformance.HostStreamCase{{
			Name: "connect, ping, control pong, and event frame",
			Request: conformance.HostStreamConnectRequest{
				Account: conformance.ChannelAccount{
					UUID:       "account-stream",
					Credential: larkCredential("account-stream"),
					State:      map[string]any{},
				},
			},
			Expect: conformance.HostStreamConnectExpectation{
				URLContains:            "wss://lark-stream.test/ws",
				ReadMessageType:        conformance.StreamMessageTypeBinary,
				RequireServiceID:       true,
				RequirePingInterval:    true,
				RequirePongTimeout:     true,
				RequireState:           true,
				RequireConnectedHealth: true,
			},
			Ping: &conformance.HostStreamPingCase{
				Expect: conformance.HostStreamPingExpectation{
					MessageType: conformance.StreamMessageTypeBinary,
					RequireData: true,
				},
			},
			Frames: []conformance.HostStreamFrameCase{{
				Name: "control pong updates heartbeat health",
				Request: conformance.StreamFrameRequest{
					MessageType: conformance.StreamMessageTypeBinary,
					Data:        larkControlPongFrame(t),
				},
				Expect: conformance.HostStreamFrameExpectation{
					RequireFrameState: true,
					RuntimeHealth: conformance.RuntimeHealthExpectation{
						RequireLastActivityAt: true,
						RequireLastPongAt:     true,
					},
				},
			}, {
				Name: "event frame creates message and returns ack frame",
				Request: conformance.StreamFrameRequest{
					MessageType: conformance.StreamMessageTypeBinary,
					Data:        larkStreamEventFrame(t, "lark-stream-message-conformance"),
				},
				Expect: conformance.HostStreamFrameExpectation{
					MinResponseFrames:   1,
					ResponseMessageType: conformance.StreamMessageTypeBinary,
					RequireEventResult:  true,
					EventType:           "im.message.receive_v1",
					RuntimeHealth: conformance.RuntimeHealthExpectation{
						RequireLastActivityAt: true,
						RequireLastEventAt:    true,
					},
				},
			}},
		}},
		CredentialCases: []conformance.CredentialValidationCase{{
			Name: "valid credential exposes stable account identity",
			Request: conformance.CredentialValidationRequest{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				Credential: map[string]any{
					"app_id":     "cli_conformance",
					"app_secret": "secret_conformance",
					"brand":      "feishu",
				},
			},
			Expect: conformance.CredentialValidationExpectation{
				Valid:              true,
				AccountKey:         "cli_conformance",
				DisplayName:        "Beak Conformance Bot",
				RequireAccountID:   true,
				RequireBotIdentity: true,
			},
		}, {
			Name: "invalid credential fails without creating account",
			Request: conformance.CredentialValidationRequest{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				Credential: map[string]any{
					"app_id":     "cli_bad",
					"app_secret": "bad",
				},
			},
			Expect: conformance.CredentialValidationExpectation{Valid: false},
		}},
		InboundCases: []conformance.InboundCase{{
			Name: "mention_all does not imply mentioned_me",
			Fixture: conformance.InboundFixture{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				AccountUUID:   "account-1",
				Credential:    larkCredential("account-1"),
				Raw: json.RawMessage(`{
					"schema":"2.0",
					"header":{"event_id":"evt_at_all_conformance","event_type":"im.message.receive_v1","app_id":"cli_1","token":"verify-token"},
					"event":{
						"sender":{"sender_id":{"open_id":"ou_user"},"sender_type":"user"},
						"message":{"message_id":"om_at_all_conformance","chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"hello all\"}","create_time":"1770000000000","mentions":[{"key":"@_all","id":{},"name":"所有人"}]}
					}
				}`),
			},
			Expect: conformance.InboundExpectation{
				ChatType:          conformance.ChatTypeGroup,
				ChatID:            "oc_group",
				ChatDisplayName:   "Team",
				ChatAvatarURL:     "https://example.test/team.png",
				ChatIdentityID:    "oc_group",
				SenderID:          "ou_user",
				SenderDisplayName: "Alice",
				Text:              "hello all",
				MentionedMe:       &falseValue,
				MentionAll:        &trueValue,
				RequireMessageID:  true,
			},
		}, {
			Name: "only bot mention follow up is delivered",
			Fixture: conformance.InboundFixture{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				AccountUUID:   "account-1",
				Credential:    larkCredential("account-1"),
				Raw: json.RawMessage(`{
					"schema":"2.0",
					"header":{"event_id":"evt_only_bot_conformance","event_type":"im.message.receive_v1","app_id":"cli_1","token":"verify-token"},
					"event":{
						"sender":{"sender_id":{"open_id":"ou_user"},"sender_type":"user"},
						"message":{"message_id":"om_only_bot_conformance","chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"@_bot\"}","create_time":"1770000000000","mentions":[{"key":"@_bot","id":{"open_id":"ou_bot"},"name":"Beak Bot"}]}
					}
				}`),
			},
			Expect: conformance.InboundExpectation{
				ChatType:          conformance.ChatTypeGroup,
				ChatID:            "oc_group",
				ChatDisplayName:   "Team",
				ChatAvatarURL:     "https://example.test/team.png",
				ChatIdentityID:    "oc_group",
				SenderID:          "ou_user",
				SenderDisplayName: "Alice",
				TextTrimmedEmpty:  &trueValue,
				MentionedMe:       &trueValue,
				MentionIDs:        []string{"ou_bot"},
				RequireMessageID:  true,
			},
		}, {
			Name: "reply exposes referenced message",
			Fixture: conformance.InboundFixture{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				AccountUUID:   "account-1",
				Credential:    larkCredential("account-1"),
				Raw: json.RawMessage(`{
					"schema":"2.0",
					"header":{"event_id":"evt_reply_conformance","event_type":"im.message.receive_v1","app_id":"cli_1","token":"verify-token"},
					"event":{
						"sender":{"sender_id":{"open_id":"ou_user"},"sender_type":"user"},
						"message":{"message_id":"om_reply_conformance","chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"reply text\"}","create_time":"1770000000000","parent_id":"om_parent_conformance","root_id":"om_root_conformance","thread_id":"omt_conformance"}
					}
				}`),
			},
			Expect: conformance.InboundExpectation{
				ChatType:          conformance.ChatTypeGroup,
				ChatID:            "oc_group",
				ThreadID:          "omt_conformance",
				ChatDisplayName:   "Team",
				ChatAvatarURL:     "https://example.test/team.png",
				ChatIdentityID:    "oc_group",
				SenderID:          "ou_user",
				SenderDisplayName: "Alice",
				Text:              "reply text",
				ReferencedMessage: &conformance.ReferencedMessageExpectation{
					Platform:    beaklark.Platform,
					MessageID:   "om_parent_conformance",
					ChatType:    conformance.ChatTypeGroup,
					ChatID:      "oc_group",
					ThreadID:    "omt_conformance",
					RootID:      "om_root_conformance",
					SenderID:    "ou_parent",
					MessageType: "text",
					Text:        "quoted lark",
					CreatedAt:   "1770000001000",
				},
				RequireMessageID: true,
			},
		}},
		AckCases: []conformance.AckCase{{
			Name: "processing ack adds lark reaction",
			Request: conformance.OutboundAck{
				AccountUUID:     "account-1",
				ChatType:        conformance.ChatTypeGroup,
				ChatID:          "oc_group",
				TargetMessageID: "om_conformance",
				Action:          "start",
				Emoji:           "thinking",
			},
			Expect: conformance.AckExpectation{
				Status:     "sent",
				Mode:       "reaction",
				ReactionID: "reaction-conformance",
			},
		}},
	})

	t.Run("feishu runtime platform remains beak-facing", func(t *testing.T) {
		conformance.Run(t, conformance.Config{
			Platform:            "feishu",
			MetadataPlatform:    beaklark.Platform,
			MetadataProvider:    adapter,
			CredentialValidator: adapter,
			InboundParser:       adapter,
			Acknowledger:        adapter,
			HostStreamer:        adapter,
			CredentialCases: []conformance.CredentialValidationCase{{
				Name: "valid feishu credential exposes feishu metadata platform",
				Request: conformance.CredentialValidationRequest{
					WorkspaceUUID: "workspace-1",
					ChannelUUID:   "channel-1",
					Credential: map[string]any{
						"app_id":     "cli_feishu_conformance",
						"app_secret": "secret_feishu_conformance",
						"brand":      "feishu",
					},
				},
				Expect: conformance.CredentialValidationExpectation{
					Valid:              true,
					AccountKey:         "cli_feishu_conformance",
					DisplayName:        "Beak Conformance Bot",
					MetadataPlatform:   "feishu",
					RequireAccountID:   true,
					RequireBotIdentity: true,
				},
			}},
			HostStreamCases: []conformance.HostStreamCase{{
				Name: "event frame returns feishu inbound",
				Request: conformance.HostStreamConnectRequest{
					Account: conformance.ChannelAccount{
						UUID:       "account-stream-feishu",
						Credential: larkCredential("account-stream-feishu"),
						State:      map[string]any{},
					},
				},
				Expect: conformance.HostStreamConnectExpectation{
					URLContains:            "wss://lark-stream.test/ws",
					ReadMessageType:        conformance.StreamMessageTypeBinary,
					RequireServiceID:       true,
					RequirePingInterval:    true,
					RequirePongTimeout:     true,
					RequireState:           true,
					RequireConnectedHealth: true,
				},
				Frames: []conformance.HostStreamFrameCase{{
					Name: "event frame creates feishu message",
					Request: conformance.StreamFrameRequest{
						MessageType: conformance.StreamMessageTypeBinary,
						Data:        larkStreamEventFrame(t, "feishu-stream-message-conformance"),
					},
					Expect: conformance.HostStreamFrameExpectation{
						MinResponseFrames:   1,
						ResponseMessageType: conformance.StreamMessageTypeBinary,
						RequireEventResult:  true,
						EventType:           "im.message.receive_v1",
						RuntimeHealth: conformance.RuntimeHealthExpectation{
							RequireLastActivityAt: true,
							RequireLastEventAt:    true,
						},
					},
				}},
			}},
			InboundCases: []conformance.InboundCase{{
				Name: "webhook creates feishu inbound",
				Fixture: conformance.InboundFixture{
					WorkspaceUUID: "workspace-1",
					ChannelUUID:   "channel-1",
					AccountUUID:   "account-feishu",
					Credential:    larkCredential("account-feishu"),
					Raw: json.RawMessage(`{
						"schema":"2.0",
						"header":{"event_id":"evt_feishu_conformance","event_type":"im.message.receive_v1","app_id":"cli_1","token":"verify-token"},
						"event":{
							"sender":{"sender_id":{"open_id":"ou_user"},"sender_type":"user"},
							"message":{"message_id":"om_feishu_conformance","chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"hello feishu\"}","create_time":"1770000000000"}
						}
					}`),
				},
				Expect: conformance.InboundExpectation{
					ChatType:          conformance.ChatTypeGroup,
					ChatID:            "oc_group",
					ChatDisplayName:   "Team",
					ChatAvatarURL:     "https://example.test/team.png",
					ChatIdentityID:    "oc_group",
					SenderID:          "ou_user",
					SenderDisplayName: "Alice",
					Text:              "hello feishu",
					RequireMessageID:  true,
				},
			}, {
				Name: "webhook creates feishu referenced message",
				Fixture: conformance.InboundFixture{
					WorkspaceUUID: "workspace-1",
					ChannelUUID:   "channel-1",
					AccountUUID:   "account-feishu",
					Credential:    larkCredential("account-feishu"),
					Raw: json.RawMessage(`{
						"schema":"2.0",
						"header":{"event_id":"evt_feishu_reply_conformance","event_type":"im.message.receive_v1","app_id":"cli_1","token":"verify-token"},
						"event":{
							"sender":{"sender_id":{"open_id":"ou_user"},"sender_type":"user"},
							"message":{"message_id":"om_feishu_reply_conformance","chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"hello feishu reply\"}","create_time":"1770000000000","parent_id":"om_feishu_parent_conformance","root_id":"om_feishu_root_conformance","thread_id":"omt_feishu_conformance"}
						}
					}`),
				},
				Expect: conformance.InboundExpectation{
					ChatType:          conformance.ChatTypeGroup,
					ChatID:            "oc_group",
					ThreadID:          "omt_feishu_conformance",
					ChatDisplayName:   "Team",
					ChatAvatarURL:     "https://example.test/team.png",
					ChatIdentityID:    "oc_group",
					SenderID:          "ou_user",
					SenderDisplayName: "Alice",
					Text:              "hello feishu reply",
					ReferencedMessage: &conformance.ReferencedMessageExpectation{
						Platform:    "feishu",
						MessageID:   "om_feishu_parent_conformance",
						ChatType:    conformance.ChatTypeGroup,
						ChatID:      "oc_group",
						ThreadID:    "omt_feishu_conformance",
						RootID:      "om_feishu_root_conformance",
						SenderID:    "ou_parent",
						MessageType: "text",
						Text:        "quoted feishu",
						CreatedAt:   "1770000001000",
					},
					RequireMessageID: true,
				},
			}},
			AckCases: []conformance.AckCase{{
				Name: "feishu ack keeps runtime platform",
				Request: conformance.OutboundAck{
					AccountUUID:     "account-feishu",
					ChatType:        conformance.ChatTypeGroup,
					ChatID:          "oc_group",
					TargetMessageID: "om_conformance",
					Action:          "start",
					Emoji:           "thinking",
				},
				Expect: conformance.AckExpectation{
					Status:     "sent",
					Mode:       "reaction",
					ReactionID: "reaction-conformance",
				},
			}},
		})
	})

	t.Run("runtime health start is host-owned and event updates activity", func(t *testing.T) {
		store := newLarkStore()
		account := larksdk.ChannelAccount{
			UUID:          "account-health",
			WorkspaceUUID: "workspace-1",
			ChannelUUID:   "channel-1",
			Platform:      beaklark.Platform,
			Credential:    larkCredential("account-health"),
			State:         map[string]any{},
		}
		startErr := make(chan error, 1)
		go func() {
			startErr <- adapter.connector.Start(context.Background(), larksdk.Runtime{
				WorkspaceUUID: "workspace-1",
				Channel:       larksdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: beaklark.Platform},
				Account:       account,
				Gateway:       &larkGateway{},
				AccountStore:  store,
				HTTPClient:    adapter.httpClient,
			})
		}()
		select {
		case err := <-startErr:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("lark Start blocked; WebSocket client must stay host-owned")
		}
		_, err := adapter.event.HandleEvent(context.Background(), larksdk.Runtime{
			WorkspaceUUID: "workspace-1",
			Channel:       larksdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: beaklark.Platform},
			Account:       account,
			Gateway:       &larkGateway{},
			AccountStore:  store,
			HTTPClient:    adapter.httpClient,
		}, account, []byte(`{
			"schema":"2.0",
			"header":{"event_id":"evt_health","event_type":"im.message.receive_v1","app_id":"cli_1","token":"verify-token"},
			"event":{
				"sender":{"sender_id":{"open_id":"ou_user"},"sender_type":"user"},
				"message":{"message_id":"om_health","chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"@_bot hello\"}","create_time":"1770000000000","mentions":[{"key":"@_bot","id":{"open_id":"ou_bot"},"name":"Beak Bot"}]}
			}
		}`))
		if err != nil {
			t.Fatal(err)
		}
		conformance.AssertRuntimeHealthState(t, store.state("account-health"), conformance.RuntimeHealthExpectation{
			RequireLastActivityAt: true,
			RequireLastEventAt:    true,
		})
	})
}

type larkAdapter struct {
	t          *testing.T
	connector  larksdk.Connector
	webhook    beaklark.WebhookConnector
	event      beaklark.EventConnector
	hostStream larksdk.HostStreamConnector
	httpClient *http.Client
}

func newLarkAdapter(t *testing.T) larkAdapter {
	t.Helper()
	connector := beaklark.NewConnector()
	webhook, ok := connector.(beaklark.WebhookConnector)
	if !ok {
		t.Fatal("lark connector should expose WebhookConnector")
	}
	event, ok := connector.(beaklark.EventConnector)
	if !ok {
		t.Fatal("lark connector should expose EventConnector")
	}
	hostStream, ok := connector.(larksdk.HostStreamConnector)
	if !ok {
		t.Fatal("lark connector should expose HostStreamConnector")
	}
	return larkAdapter{
		t:          t,
		connector:  connector,
		webhook:    webhook,
		event:      event,
		hostStream: hostStream,
		httpClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/open-apis/auth/v3/tenant_access_token/internal":
				var body map[string]string
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body["app_secret"] == "bad" {
					return jsonResponse(map[string]any{"code": 999, "msg": "bad secret"})
				}
				return jsonResponse(map[string]any{"code": 0, "tenant_access_token": "tenant-token-conformance", "expire": 3600})
			case "/callback/ws/endpoint":
				var body map[string]string
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body["AppID"] != "cli_1" || body["AppSecret"] != "secret_1" {
					t.Fatalf("unexpected lark stream credential body=%+v", body)
				}
				return jsonResponse(map[string]any{
					"code": 0,
					"msg":  "ok",
					"data": map[string]any{
						"URL": "wss://lark-stream.test/ws?service_id=42",
						"ClientConfig": map[string]any{
							"PingInterval": 1,
						},
					},
				})
			case "/open-apis/bot/v3/info":
				return jsonResponse(map[string]any{
					"code": 0,
					"bot": map[string]any{
						"open_id":         "ou_bot_conformance",
						"app_name":        "Beak Conformance Bot",
						"activate_status": 1,
					},
				})
			case "/open-apis/contact/v3/users/ou_user":
				if got := req.URL.Query().Get("user_id_type"); got != "open_id" {
					t.Fatalf("lark user_id_type = %q, want open_id", got)
				}
				return jsonResponse(map[string]any{
					"code": 0,
					"data": map[string]any{
						"user": map[string]any{
							"open_id": "ou_user",
							"name":    "Alice",
						},
					},
				})
			case "/open-apis/im/v1/chats/oc_group":
				return jsonResponse(map[string]any{
					"code": 0,
					"data": map[string]any{
						"chat_id":    "oc_group",
						"name":       "Team",
						"avatar_url": "https://example.test/team.png",
					},
				})
			case "/open-apis/im/v1/messages/om_parent_conformance", "/open-apis/im/v1/messages/om_feishu_parent_conformance":
				if got := req.URL.Query().Get("card_msg_content_type"); got != "raw_card_content" {
					t.Fatalf("lark card_msg_content_type = %q, want raw_card_content", got)
				}
				messageID := strings.TrimPrefix(req.URL.Path, "/open-apis/im/v1/messages/")
				rootID := "om_root_conformance"
				threadID := "omt_conformance"
				text := "quoted lark"
				if strings.Contains(messageID, "feishu") {
					rootID = "om_feishu_root_conformance"
					threadID = "omt_feishu_conformance"
					text = "quoted feishu"
				}
				return jsonResponse(map[string]any{
					"code": 0,
					"msg":  "ok",
					"data": map[string]any{
						"items": []map[string]any{{
							"message_id":  messageID,
							"root_id":     rootID,
							"thread_id":   threadID,
							"chat_id":     "oc_group",
							"chat_type":   "group",
							"msg_type":    "text",
							"content":     `{"text":"` + text + `"}`,
							"create_time": "1770000001000",
							"sender":      map[string]any{"sender_id": map[string]any{"open_id": "ou_parent"}, "sender_type": "user"},
						}},
					},
				})
			case "/open-apis/im/v1/messages/om_conformance/reactions":
				var body struct {
					ReactionType struct {
						EmojiType string `json:"emoji_type"`
					} `json:"reaction_type"`
				}
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body.ReactionType.EmojiType != "THINKING" {
					t.Fatalf("lark reaction emoji=%q", body.ReactionType.EmojiType)
				}
				return jsonResponse(map[string]any{
					"code": 0,
					"data": map[string]any{
						"reaction_id": "reaction-conformance",
						"reaction_type": map[string]any{
							"emoji_type": "THINKING",
						},
						"action_time": "1770000000000",
					},
				})
			default:
				t.Fatalf("unexpected lark request: %s %s", req.Method, req.URL.Path)
				return nil, nil
			}
		})},
	}
}

func (a larkAdapter) Metadata() conformance.ConnectorMetadata {
	return convert[conformance.ConnectorMetadata](a.t, a.connector.Metadata())
}

func (a larkAdapter) CredentialSchema(ctx context.Context) conformance.CredentialSchema {
	return convert[conformance.CredentialSchema](a.t, a.connector.CredentialSchema(ctx))
}

func (a larkAdapter) ValidateCredential(ctx context.Context, req conformance.CredentialValidationRequest) (*conformance.CredentialValidationResult, error) {
	sdkReq := convert[larksdk.CredentialValidationRequest](a.t, req)
	sdkReq.Runtime = larksdk.Runtime{HTTPClient: a.httpClient}
	result, err := a.connector.ValidateCredential(ctx, sdkReq)
	if result == nil {
		return nil, err
	}
	out := convert[conformance.CredentialValidationResult](a.t, result)
	return &out, err
}

func (a larkAdapter) ParseInbound(ctx context.Context, fixture conformance.InboundFixture) ([]conformance.InboundMessage, error) {
	account := larkAccount(fixture)
	platform := firstString(fixture.Platform, account.Platform, beaklark.Platform)
	result, err := a.webhook.HandleWebhook(ctx, larksdk.Runtime{
		WorkspaceUUID: firstString(fixture.WorkspaceUUID, "workspace-1"),
		Channel: larksdk.Channel{
			UUID:          firstString(fixture.ChannelUUID, "channel-1"),
			WorkspaceUUID: firstString(fixture.WorkspaceUUID, "workspace-1"),
			Platform:      platform,
		},
		Account:      account,
		Gateway:      &larkGateway{},
		AccountStore: newLarkStore(),
		HTTPClient:   a.httpClient,
	}, account, fixture.Raw)
	if err != nil || result == nil || result.Ignored || result.Inbound == nil {
		return nil, err
	}
	return []conformance.InboundMessage{convert[conformance.InboundMessage](a.t, result.Inbound)}, nil
}

func (a larkAdapter) Acknowledge(ctx context.Context, req conformance.OutboundAck) (*conformance.AckResult, error) {
	sdkReq := convert[larksdk.OutboundAck](a.t, req)
	account := larksdk.ChannelAccount{
		UUID:       firstString(req.AccountUUID, "account-1"),
		Platform:   firstString(req.Platform, beaklark.Platform),
		Credential: larkCredential(firstString(req.AccountUUID, "account-1")),
	}
	result, err := a.connector.Acknowledge(ctx, larksdk.Runtime{
		Account:    account,
		HTTPClient: a.httpClient,
	}, sdkReq)
	if result == nil {
		return nil, err
	}
	out := convert[conformance.AckResult](a.t, result)
	return &out, err
}

func (a larkAdapter) ConnectStream(ctx context.Context, req conformance.HostStreamConnectRequest) (*conformance.StreamConnectResult, error) {
	account := larkStreamAccount(req)
	result, err := a.hostStream.ConnectStream(ctx, larkRuntime(req.WorkspaceUUID, req.ChannelUUID, account, a.httpClient), account)
	if result == nil {
		return nil, err
	}
	return &conformance.StreamConnectResult{
		URL:             result.URL,
		Headers:         result.Headers,
		ServiceID:       result.ServiceID,
		ReadMessageType: result.ReadMessageType,
		PingInterval:    result.PingInterval,
		PongTimeout:     result.PongTimeout,
		State:           result.State,
		HealthUpdates:   result.HealthUpdates,
	}, err
}

func (a larkAdapter) BuildStreamPing(ctx context.Context, req conformance.StreamPingRequest) (*conformance.StreamFrame, error) {
	result, err := a.hostStream.BuildStreamPing(ctx, larksdk.StreamPingRequest{
		ServiceID: req.ServiceID,
		State:     req.State,
	})
	if result == nil {
		return nil, err
	}
	return &conformance.StreamFrame{MessageType: result.MessageType, Data: result.Data}, err
}

func (a larkAdapter) HandleStreamFrame(ctx context.Context, req conformance.StreamFrameRequest) (*conformance.StreamFrameResult, error) {
	account := larkFrameAccount(req)
	result, err := a.hostStream.HandleStreamFrame(ctx, larkRuntime(req.WorkspaceUUID, req.ChannelUUID, account, a.httpClient), account, larksdk.StreamFrameRequest{
		MessageType: req.MessageType,
		Data:        req.Data,
		ServiceID:   req.ServiceID,
		State:       req.State,
	})
	if result == nil {
		return nil, err
	}
	return larkStreamFrameResult(a.t, result), err
}

func larkAccount(fixture conformance.InboundFixture) larksdk.ChannelAccount {
	credential := fixture.Credential
	if len(credential) == 0 {
		credential = larkCredential(firstString(fixture.AccountUUID, "account-1"))
	}
	return larksdk.ChannelAccount{
		UUID:          firstString(fixture.AccountUUID, "account-1"),
		WorkspaceUUID: firstString(fixture.WorkspaceUUID, "workspace-1"),
		ChannelUUID:   firstString(fixture.ChannelUUID, "channel-1"),
		Platform:      firstString(fixture.Platform, larkPlatformFromCredential(credential), beaklark.Platform),
		Credential:    credential,
		State:         fixture.AccountState,
	}
}

func larkStreamAccount(req conformance.HostStreamConnectRequest) larksdk.ChannelAccount {
	credential := req.Account.Credential
	if len(credential) == 0 {
		credential = req.Credential
	}
	if len(credential) == 0 {
		credential = larkCredential(firstString(req.Account.UUID, "account-1"))
	}
	state := req.Account.State
	if state == nil {
		state = req.State
	}
	return larksdk.ChannelAccount{
		UUID:          firstString(req.Account.UUID, "account-1"),
		WorkspaceUUID: firstString(req.Account.WorkspaceUUID, req.WorkspaceUUID, "workspace-1"),
		ChannelUUID:   firstString(req.Account.ChannelUUID, req.ChannelUUID, "channel-1"),
		Platform:      firstString(req.Account.Platform, larkPlatformFromCredential(credential), beaklark.Platform),
		Credential:    credential,
		State:         state,
	}
}

func larkFrameAccount(req conformance.StreamFrameRequest) larksdk.ChannelAccount {
	connectReq := conformance.HostStreamConnectRequest{
		WorkspaceUUID: req.WorkspaceUUID,
		ChannelUUID:   req.ChannelUUID,
		Account:       req.Account,
		Credential:    req.Credential,
	}
	return larkStreamAccount(connectReq)
}

func larkRuntime(workspaceUUID, channelUUID string, account larksdk.ChannelAccount, httpClient *http.Client) larksdk.Runtime {
	workspaceUUID = firstString(workspaceUUID, account.WorkspaceUUID, "workspace-1")
	channelUUID = firstString(channelUUID, account.ChannelUUID, "channel-1")
	platform := firstString(account.Platform, larkPlatformFromCredential(account.Credential), beaklark.Platform)
	return larksdk.Runtime{
		WorkspaceUUID: workspaceUUID,
		Channel:       larksdk.Channel{UUID: channelUUID, WorkspaceUUID: workspaceUUID, Platform: platform},
		Account:       account,
		Gateway:       &larkGateway{},
		AccountStore:  newLarkStore(),
		HTTPClient:    httpClient,
	}
}

func larkPlatformFromCredential(credential map[string]any) string {
	platform := strings.TrimSpace(stringValue(credential["platform"]))
	if platform != "" {
		return platform
	}
	switch strings.ToLower(strings.TrimSpace(stringValue(credential["brand"]))) {
	case "feishu":
		return "feishu"
	case "lark":
		return "lark"
	}
	baseURL := strings.ToLower(strings.TrimSpace(stringValue(credential["base_url"])))
	switch {
	case strings.Contains(baseURL, "open.feishu.cn"):
		return "feishu"
	case strings.Contains(baseURL, "open.larksuite.com"):
		return "lark"
	default:
		return ""
	}
}

func larkStreamFrameResult(t *testing.T, result *larksdk.StreamFrameResult) *conformance.StreamFrameResult {
	t.Helper()
	out := &conformance.StreamFrameResult{
		HealthUpdates: result.HealthUpdates,
		CloseReason:   result.CloseReason,
		State:         result.State,
	}
	for _, frame := range result.ResponseFrames {
		out.ResponseFrames = append(out.ResponseFrames, conformance.StreamFrame{
			MessageType: frame.MessageType,
			Data:        frame.Data,
		})
	}
	if result.EventResult != nil {
		event := convert[conformance.StreamEventResult](t, result.EventResult)
		out.EventResult = &event
	}
	return out
}

func larkControlPongFrame(t *testing.T) []byte {
	t.Helper()
	frame := larkws.Frame{
		Method: int32(larkws.FrameTypeControl),
		Headers: []larkws.Header{{
			Key:   larkws.HeaderType,
			Value: string(larkws.MessageTypePong),
		}},
		Payload: []byte(`{"PingInterval":1}`),
	}
	data, err := frame.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func larkStreamEventFrame(t *testing.T, messageID string) []byte {
	t.Helper()
	payload := []byte(`{
		"schema":"2.0",
		"header":{"event_id":"evt_stream_conformance","event_type":"im.message.receive_v1","app_id":"cli_1","token":"verify-token"},
		"event":{
			"sender":{"sender_id":{"open_id":"ou_user"},"sender_type":"user"},
			"message":{"message_id":"om_stream_conformance","chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"@_bot hello from stream\"}","create_time":"1770000000000","mentions":[{"key":"@_bot","id":{"open_id":"ou_bot"},"name":"Beak Bot"}]}
		}
	}`)
	frame := larkws.Frame{
		Method: int32(larkws.FrameTypeData),
		Headers: []larkws.Header{{
			Key:   larkws.HeaderType,
			Value: string(larkws.MessageTypeEvent),
		}, {
			Key:   larkws.HeaderMessageID,
			Value: messageID,
		}, {
			Key:   larkws.HeaderSum,
			Value: "1",
		}, {
			Key:   larkws.HeaderSeq,
			Value: "0",
		}},
		Payload: payload,
	}
	data, err := frame.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func larkCredential(accountUUID string) map[string]any {
	return map[string]any{
		"account_id":         firstString(accountUUID, "account-1"),
		"app_id":             "cli_1",
		"app_secret":         "secret_1",
		"verification_token": "verify-token",
		"brand":              "feishu",
		"bot_open_id":        "ou_bot",
	}
}

type larkGateway struct{}

func (g *larkGateway) EnsureChannel(context.Context, larksdk.EnsureChannelRequest) (string, error) {
	return "channel-1", nil
}

func (g *larkGateway) EnsureChannelLinkSession(context.Context, larksdk.EnsureChannelLinkSessionRequest) (string, error) {
	return "link-1", nil
}

func (g *larkGateway) EnsureChatSession(context.Context, larksdk.EnsureChatSessionRequest) (string, error) {
	return "session-1", nil
}

func (g *larkGateway) CreateMessage(context.Context, larksdk.CreateMessageRequest) (string, error) {
	return "message-1", nil
}

func (g *larkGateway) StreamSession(context.Context, larksdk.StreamSessionRequest, func(larksdk.StreamEvent) error) error {
	return nil
}

func (g *larkGateway) AgentParticipantID() string {
	return "agent:agent-1"
}

func (g *larkGateway) BridgeParticipantID(platform string) string {
	return "bridge:" + platform
}

type larkStore struct {
	mu     sync.Mutex
	states map[string]map[string]any
}

func newLarkStore() *larkStore {
	return &larkStore{states: map[string]map[string]any{}}
}

func (s *larkStore) LoadChannelAccountState(_ context.Context, accountUUID string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.states[accountUUID], nil
}

func (s *larkStore) SaveChannelAccountState(_ context.Context, accountUUID string, state map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[accountUUID] = state
	return nil
}

func (s *larkStore) state(accountUUID string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]any, len(s.states[accountUUID]))
	for key, value := range s.states[accountUUID] {
		out[key] = value
	}
	return out
}

func runWeixinConformance(t *testing.T) {
	adapter := newWeixinAdapter(t)
	defer adapter.loginSrv.Close()

	trueValue := true
	falseValue := false

	conformance.Run(t, conformance.Config{
		Platform:                 beakweixin.Platform,
		MetadataProvider:         adapter,
		CredentialSchemaProvider: adapter,
		CredentialValidator:      adapter,
		LoginPoller:              adapter,
		InboundParser:            adapter,
		Acknowledger:             adapter,
		CredentialCases: []conformance.CredentialValidationCase{{
			Name: "valid credential exposes stable account identity",
			Request: conformance.CredentialValidationRequest{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				Credential: map[string]any{
					"bot_token":     "token-conformance",
					"ilink_bot_id":  "ilink-bot-conformance",
					"ilink_user_id": "ilink-user-conformance",
					"base_url":      "https://ilinkai.weixin.qq.com",
				},
			},
			Expect: conformance.CredentialValidationExpectation{
				Valid:              true,
				AccountKey:         "ilink-user-conformance",
				DisplayName:        "ilink-user-conformance",
				RequireAccountID:   true,
				RequireBotIdentity: true,
			},
		}},
		LoginPollCases: []conformance.LoginPollCase{{
			Name: "approved qr login exposes stable account identity",
			Request: conformance.LoginPollRequest{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				ChallengeCode: "qr-conformance",
			},
			Expect: conformance.LoginPollExpectation{
				Approved:           true,
				AccountKey:         "ilink-user-conformance",
				RequireAccountID:   true,
				RequireBotIdentity: true,
			},
		}},
		InboundCases: []conformance.InboundCase{{
			Name: "mention_all does not imply mentioned_me",
			Fixture: conformance.InboundFixture{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				AccountUUID:   "account-1",
				Raw: json.RawMessage(`{
					"ret":0,
					"get_updates_buf":"buf-conformance-all",
					"msgs":[{
						"message_id":301,
						"from_user_id":"user-1",
						"to_user_id":"bot-1",
						"group_id":"group-1",
						"message_type":1,
						"message_state":2,
						"mention_all":true,
						"item_list":[{"type":1,"text_item":{"text":"hello all"}}]
					}]
				}`),
			},
			Expect: conformance.InboundExpectation{
				ChatType:         conformance.ChatTypeGroup,
				ChatID:           "group-1",
				ChatIdentityID:   "group-1",
				SenderID:         "user-1",
				Text:             "hello all",
				MentionedMe:      &falseValue,
				MentionAll:       &trueValue,
				RequireMessageID: true,
				RequireDedupeKey: true,
			},
		}, {
			Name: "only bot mention follow up is delivered",
			Fixture: conformance.InboundFixture{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				AccountUUID:   "account-1",
				Raw: json.RawMessage(`{
					"ret":0,
					"get_updates_buf":"buf-conformance-bot",
					"msgs":[{
						"message_id":302,
						"from_user_id":"user-1",
						"to_user_id":"bot-1",
						"group_id":"group-1",
						"message_type":1,
						"message_state":2,
						"mentioned_me":true,
						"mentions":[{"id":"ilink-bot-conformance","id_type":"ilink_bot_id","display_name":"Beak Bot"}],
						"item_list":[{"type":1,"text_item":{"text":""}}]
					}]
				}`),
			},
			Expect: conformance.InboundExpectation{
				ChatType:         conformance.ChatTypeGroup,
				ChatID:           "group-1",
				ChatIdentityID:   "group-1",
				SenderID:         "user-1",
				TextTrimmedEmpty: &trueValue,
				MentionedMe:      &trueValue,
				MentionIDs:       []string{"ilink-bot-conformance"},
				RequireMessageID: true,
				RequireDedupeKey: true,
			},
		}, {
			Name: "ref_msg exposes referenced message",
			Fixture: conformance.InboundFixture{
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				AccountUUID:   "account-1",
				Raw: json.RawMessage(`{
					"ret":0,
					"get_updates_buf":"buf-conformance-ref",
					"msgs":[{
						"message_id":303,
						"from_user_id":"user-1",
						"to_user_id":"bot-1",
						"group_id":"group-1",
						"message_type":1,
						"message_state":2,
						"item_list":[
							{"type":1,"text_item":{"text":"reply text"}},
							{"type":1,"ref_msg":{"title":"引用标题","message_id":99,"message_item":{"type":1,"text_item":{"text":"quoted weixin"}}}}
						]
					}]
				}`),
			},
			Expect: conformance.InboundExpectation{
				ChatType:         conformance.ChatTypeGroup,
				ChatID:           "group-1",
				ChatIdentityID:   "group-1",
				SenderID:         "user-1",
				Text:             "reply text",
				RequireMessageID: true,
				RequireDedupeKey: true,
				ReferencedMessage: &conformance.ReferencedMessageExpectation{
					Platform:    beakweixin.Platform,
					MessageID:   "99",
					ChatType:    conformance.ChatTypeGroup,
					ChatID:      "group-1",
					MessageType: "text",
					Text:        "引用标题 | quoted weixin",
				},
			},
		}},
		AckCases: []conformance.AckCase{{
			Name: "processing ack sends weixin typing",
			Request: conformance.OutboundAck{
				AccountUUID: "account-1",
				ChatType:    conformance.ChatTypeGroup,
				ChatID:      "group-conformance",
				Action:      "start",
			},
			Expect: conformance.AckExpectation{
				Status: "sent",
				Mode:   "typing",
			},
		}},
	})

	t.Run("runtime health records poll errors and recovered activity", func(t *testing.T) {
		var getUpdatesCalls int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/ilink/bot/msg/notifystart", "/ilink/bot/msg/notifystop":
				_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
			case "/ilink/bot/getupdates":
				getUpdatesCalls++
				if getUpdatesCalls == 1 {
					_ = json.NewEncoder(w).Encode(map[string]any{"ret": 1, "errmsg": "temporary getupdates failure"})
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ret":             0,
					"get_updates_buf": "buf-health",
					"msgs": []map[string]any{{
						"message_id":    401,
						"from_user_id":  "user-1",
						"to_user_id":    "bot-1",
						"message_type":  1,
						"message_state": 2,
						"context_token": "ctx-health",
						"item_list": []map[string]any{{
							"type":      1,
							"text_item": map[string]any{"text": "hello health"},
						}},
					}},
				})
			case "/ilink/bot/getconfig":
				_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0, "typing_ticket": "typing-ticket-conformance"})
			case "/ilink/bot/sendtyping":
				_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
			default:
				t.Fatalf("unexpected weixin request: %s %s", r.Method, r.URL.Path)
			}
		}))
		defer server.Close()

		store := newWeixinStore()
		gateway := &weixinGateway{
			streamErrs:  []error{errors.New("temporary stream failure")},
			streamBlock: make(chan struct{}),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		err := adapter.connector.Start(ctx, wechatsdk.Runtime{
			WorkspaceUUID: "workspace-1",
			Channel:       wechatsdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: beakweixin.Platform},
			Account: wechatsdk.ChannelAccount{
				UUID:          "account-health",
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				Platform:      beakweixin.Platform,
				Credential: map[string]any{
					"account_id":    "account-health",
					"bot_token":     "token-conformance",
					"base_url":      server.URL,
					"ilink_user_id": "ilink-user-conformance",
					"ilink_bot_id":  "ilink-bot-conformance",
				},
				State: map[string]any{},
			},
			Gateway:         gateway,
			AccountStore:    store,
			PollInterval:    time.Millisecond,
			StreamReconnect: time.Millisecond,
		})
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		conformance.AssertRuntimeHealthState(t, store.state("account-health"), conformance.RuntimeHealthExpectation{
			ConnectionState:       conformance.RuntimeHealthStateConnected,
			RequireConnectedAt:    true,
			RequireLastActivityAt: true,
			RequireLastEventAt:    true,
			RequireLastError:      true,
			RequireLastErrorAt:    true,
		})
	})

	t.Run("runtime health records expired poll session", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/ilink/bot/msg/notifystart", "/ilink/bot/msg/notifystop":
				_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
			case "/ilink/bot/getupdates":
				_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0, "errcode": -14, "errmsg": "expired"})
			default:
				t.Fatalf("unexpected weixin request: %s %s", r.Method, r.URL.Path)
			}
		}))
		defer server.Close()

		store := newWeixinStore()
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		err := adapter.connector.Start(ctx, wechatsdk.Runtime{
			WorkspaceUUID: "workspace-1",
			Channel:       wechatsdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: beakweixin.Platform},
			Account: wechatsdk.ChannelAccount{
				UUID:          "account-expired",
				WorkspaceUUID: "workspace-1",
				ChannelUUID:   "channel-1",
				Platform:      beakweixin.Platform,
				Credential: map[string]any{
					"account_id":    "account-expired",
					"bot_token":     "token-conformance",
					"base_url":      server.URL,
					"ilink_user_id": "ilink-user-conformance",
					"ilink_bot_id":  "ilink-bot-conformance",
				},
				State: map[string]any{},
			},
			Gateway:      &weixinGateway{},
			AccountStore: store,
		})
		if err == nil || !strings.Contains(err.Error(), "session expired") {
			t.Fatalf("expected expired session error, got %v", err)
		}
		trueValue := true
		conformance.AssertRuntimeHealthState(t, store.state("account-expired"), conformance.RuntimeHealthExpectation{
			ConnectionState:    conformance.RuntimeHealthStateExpired,
			RequireLastError:   true,
			RequireLastErrorAt: true,
			SessionExpired:     &trueValue,
		})
	})
}

type weixinAdapter struct {
	t          *testing.T
	connector  wechatsdk.Connector
	httpClient *http.Client
	loginSrv   *httptest.Server
}

func newWeixinAdapter(t *testing.T) weixinAdapter {
	t.Helper()
	var loginSrv *httptest.Server
	loginSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ilink/bot/get_qrcode_status":
			if got := r.URL.Query().Get("qrcode"); got != "qr-conformance" {
				t.Fatalf("qrcode=%q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":        "confirmed",
				"bot_token":     "token-conformance",
				"ilink_bot_id":  "ilink-bot-conformance",
				"ilink_user_id": "ilink-user-conformance",
				"baseurl":       loginSrv.URL,
			})
		default:
			t.Fatalf("unexpected weixin login request: %s %s", r.Method, r.URL.Path)
		}
	}))
	targetURL, err := url.Parse(loginSrv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return weixinAdapter{
		t:         t,
		connector: beakweixin.NewConnector(),
		httpClient: &http.Client{
			Transport: rewriteTransport{target: targetURL, base: http.DefaultTransport},
		},
		loginSrv: loginSrv,
	}
}

func (a weixinAdapter) Metadata() conformance.ConnectorMetadata {
	return convert[conformance.ConnectorMetadata](a.t, a.connector.Metadata())
}

func (a weixinAdapter) CredentialSchema(ctx context.Context) conformance.CredentialSchema {
	return convert[conformance.CredentialSchema](a.t, a.connector.CredentialSchema(ctx))
}

func (a weixinAdapter) ValidateCredential(ctx context.Context, req conformance.CredentialValidationRequest) (*conformance.CredentialValidationResult, error) {
	sdkReq := convert[wechatsdk.CredentialValidationRequest](a.t, req)
	result, err := a.connector.ValidateCredential(ctx, sdkReq)
	if result == nil {
		return nil, err
	}
	out := convert[conformance.CredentialValidationResult](a.t, result)
	return &out, err
}

func (a weixinAdapter) PollLogin(ctx context.Context, req conformance.LoginPollRequest) (*conformance.LoginStatus, error) {
	sdkReq := convert[wechatsdk.LoginPollRequest](a.t, req)
	sdkReq.Runtime = wechatsdk.Runtime{HTTPClient: a.httpClient}
	result, err := a.connector.PollLogin(ctx, sdkReq)
	if result == nil {
		return nil, err
	}
	out := convert[conformance.LoginStatus](a.t, result)
	return &out, err
}

func (a weixinAdapter) ParseInbound(ctx context.Context, fixture conformance.InboundFixture) ([]conformance.InboundMessage, error) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ilink/bot/msg/notifystart", "/ilink/bot/msg/notifystop":
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
		case "/ilink/bot/getupdates":
			_, _ = w.Write(fixture.Raw)
		case "/ilink/bot/getconfig":
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0, "typing_ticket": "typing-ticket-conformance"})
		case "/ilink/bot/sendtyping":
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
		default:
			a.t.Fatalf("unexpected weixin request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	gateway := &weixinGateway{}
	ctx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancel()
	err := a.connector.Start(ctx, wechatsdk.Runtime{
		WorkspaceUUID: firstString(fixture.WorkspaceUUID, "workspace-1"),
		Channel: wechatsdk.Channel{
			UUID:          firstString(fixture.ChannelUUID, "channel-1"),
			WorkspaceUUID: firstString(fixture.WorkspaceUUID, "workspace-1"),
			Platform:      beakweixin.Platform,
		},
		Account: wechatsdk.ChannelAccount{
			UUID:          firstString(fixture.AccountUUID, "account-1"),
			WorkspaceUUID: firstString(fixture.WorkspaceUUID, "workspace-1"),
			ChannelUUID:   firstString(fixture.ChannelUUID, "channel-1"),
			Platform:      beakweixin.Platform,
			Credential: map[string]any{
				"account_id":    firstString(fixture.AccountUUID, "account-1"),
				"bot_token":     "token-conformance",
				"base_url":      server.URL,
				"ilink_user_id": "ilink-user-conformance",
				"ilink_bot_id":  "ilink-bot-conformance",
			},
			State: map[string]any{},
		},
		Gateway:         gateway,
		AccountStore:    newWeixinStore(),
		PollInterval:    time.Millisecond,
		StreamReconnect: time.Millisecond,
	})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}

	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	out := make([]conformance.InboundMessage, 0, len(gateway.messages))
	for _, message := range gateway.messages {
		inbound, ok := message.Metadata["inbound_message"].(wechatsdk.InboundMessage)
		if !ok {
			continue
		}
		out = append(out, convert[conformance.InboundMessage](a.t, inbound))
	}
	return out, nil
}

func (a weixinAdapter) Acknowledge(ctx context.Context, req conformance.OutboundAck) (*conformance.AckResult, error) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ilink/bot/getconfig":
			var body struct {
				ILinkUserID  string `json:"ilink_user_id"`
				ContextToken string `json:"context_token"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				a.t.Fatal(err)
			}
			if body.ILinkUserID != req.ChatID || body.ContextToken != "ctx-conformance" {
				a.t.Fatalf("weixin getconfig body=%+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0, "typing_ticket": "typing-ticket-conformance"})
		case "/ilink/bot/sendtyping":
			var body struct {
				ILinkUserID  string `json:"ilink_user_id"`
				TypingTicket string `json:"typing_ticket"`
				Status       int    `json:"status"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				a.t.Fatal(err)
			}
			if body.ILinkUserID != req.ChatID || body.TypingTicket != "typing-ticket-conformance" || body.Status != 1 {
				a.t.Fatalf("weixin sendtyping body=%+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ret": 0})
		default:
			a.t.Fatalf("unexpected weixin ack request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	sdkReq := convert[wechatsdk.OutboundAck](a.t, req)
	contextKey := req.ChatID
	if req.ChatType == conformance.ChatTypeGroup {
		contextKey = "group:" + req.ChatID
	}
	account := wechatsdk.ChannelAccount{
		UUID:     firstString(req.AccountUUID, "account-1"),
		Platform: beakweixin.Platform,
		Credential: map[string]any{
			"account_id":    firstString(req.AccountUUID, "account-1"),
			"bot_token":     "token-conformance",
			"base_url":      server.URL,
			"ilink_user_id": "ilink-user-conformance",
			"ilink_bot_id":  "ilink-bot-conformance",
		},
		State: map[string]any{
			"context_tokens": map[string]any{
				contextKey: "ctx-conformance",
			},
		},
	}
	result, err := a.connector.Acknowledge(ctx, wechatsdk.Runtime{Account: account}, sdkReq)
	if result == nil {
		return nil, err
	}
	out := convert[conformance.AckResult](a.t, result)
	return &out, err
}

type rewriteTransport struct {
	target *url.URL
	base   http.RoundTripper
}

func (t rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = t.target.Scheme
	clone.URL.Host = t.target.Host
	if t.base == nil {
		t.base = http.DefaultTransport
	}
	return t.base.RoundTrip(clone)
}

type weixinGateway struct {
	mu          sync.Mutex
	messages    []wechatsdk.CreateMessageRequest
	streamErrs  []error
	streamBlock <-chan struct{}
}

func (g *weixinGateway) EnsureChannel(context.Context, wechatsdk.EnsureChannelRequest) (string, error) {
	return "channel-1", nil
}

func (g *weixinGateway) EnsureChannelLinkSession(context.Context, wechatsdk.EnsureChannelLinkSessionRequest) (string, error) {
	return "link-1", nil
}

func (g *weixinGateway) EnsureChatSession(context.Context, wechatsdk.EnsureChatSessionRequest) (string, error) {
	return "session-1", nil
}

func (g *weixinGateway) CreateMessage(_ context.Context, req wechatsdk.CreateMessageRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.messages = append(g.messages, req)
	return "message-1", nil
}

func (g *weixinGateway) StreamSession(ctx context.Context, _ wechatsdk.StreamSessionRequest, _ func(wechatsdk.StreamEvent) error) error {
	g.mu.Lock()
	if len(g.streamErrs) > 0 {
		err := g.streamErrs[0]
		g.streamErrs = g.streamErrs[1:]
		g.mu.Unlock()
		return err
	}
	block := g.streamBlock
	g.mu.Unlock()
	if block != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-block:
			return nil
		}
	}
	return nil
}

func (g *weixinGateway) AgentParticipantID() string {
	return "agent:agent-1"
}

func (g *weixinGateway) BridgeParticipantID(platform string) string {
	return "bridge:" + platform
}

type weixinStore struct {
	mu     sync.Mutex
	states map[string]map[string]any
}

func newWeixinStore() *weixinStore {
	return &weixinStore{states: map[string]map[string]any{}}
}

func (s *weixinStore) LoadChannelAccountState(_ context.Context, accountUUID string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.states[accountUUID], nil
}

func (s *weixinStore) SaveChannelAccountState(_ context.Context, accountUUID string, state map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[accountUUID] = state
	return nil
}

func (s *weixinStore) state(accountUUID string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]any, len(s.states[accountUUID]))
	for key, value := range s.states[accountUUID] {
		out[key] = value
	}
	return out
}
