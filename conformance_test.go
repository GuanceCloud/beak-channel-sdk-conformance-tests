package conformancetests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
				ChatType:         conformance.ChatTypeGroup,
				ChatID:           "cid-group",
				ChatDisplayName:  "Team",
				ChatIdentityID:   "cid-group",
				SenderID:         "staff-1",
				Text:             "hello all",
				MentionedMe:      &falseValue,
				MentionAll:       &trueValue,
				RequireMessageID: true,
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
				ChatType:         conformance.ChatTypeGroup,
				ChatID:           "cid-group",
				ChatDisplayName:  "Team",
				ChatIdentityID:   "cid-group",
				SenderID:         "staff-1",
				TextTrimmedEmpty: &trueValue,
				MentionedMe:      &trueValue,
				RequireMessageID: true,
			},
		}},
	})
}

type dingtalkAdapter struct {
	t          *testing.T
	connector  dingtalksdk.Connector
	event      beakdingtalk.EventConnector
	httpClient *http.Client
}

func newDingTalkAdapter(t *testing.T) dingtalkAdapter {
	t.Helper()
	connector := beakdingtalk.NewConnector()
	event, ok := connector.(beakdingtalk.EventConnector)
	if !ok {
		t.Fatal("dingtalk connector should expose EventConnector")
	}
	return dingtalkAdapter{
		t:         t,
		connector: connector,
		event:     event,
		httpClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != "/v1.0/oauth2/accessToken" {
				t.Fatalf("unexpected dingtalk request: %s %s", req.Method, req.URL.Path)
			}
			var body map[string]string
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["appSecret"] == "bad" {
				return jsonResponse(map[string]any{"code": "InvalidAppSecret", "message": "bad secret"})
			}
			return jsonResponse(map[string]any{"accessToken": "access-token-conformance", "expireIn": 3600})
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

func dingtalkCredential(accountUUID string) map[string]any {
	return map[string]any{
		"account_id":      firstString(accountUUID, "account-1"),
		"client_id":       "client-1",
		"client_secret":   "secret-1",
		"robot_code":      "robot-1",
		"chatbot_user_id": "bot-1",
		"chatbot_corp_id": "corp-1",
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

func runLarkConformance(t *testing.T) {
	adapter := newLarkAdapter(t)
	trueValue := true
	falseValue := false

	conformance.Run(t, conformance.Config{
		Platform:                 beaklark.Platform,
		MetadataProvider:         adapter,
		CredentialSchemaProvider: adapter,
		CredentialValidator:      adapter,
		InboundParser:            adapter,
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
				ChatType:         conformance.ChatTypeGroup,
				ChatID:           "oc_group",
				ChatIdentityID:   "oc_group",
				SenderID:         "ou_user",
				Text:             "hello all",
				MentionedMe:      &falseValue,
				MentionAll:       &trueValue,
				RequireMessageID: true,
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
				ChatType:         conformance.ChatTypeGroup,
				ChatID:           "oc_group",
				ChatIdentityID:   "oc_group",
				SenderID:         "ou_user",
				TextTrimmedEmpty: &trueValue,
				MentionedMe:      &trueValue,
				MentionIDs:       []string{"ou_bot"},
				RequireMessageID: true,
			},
		}},
	})
}

type larkAdapter struct {
	t          *testing.T
	connector  larksdk.Connector
	webhook    beaklark.WebhookConnector
	httpClient *http.Client
}

func newLarkAdapter(t *testing.T) larkAdapter {
	t.Helper()
	connector := beaklark.NewConnector()
	webhook, ok := connector.(beaklark.WebhookConnector)
	if !ok {
		t.Fatal("lark connector should expose WebhookConnector")
	}
	return larkAdapter{
		t:         t,
		connector: connector,
		webhook:   webhook,
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
			case "/open-apis/bot/v3/info":
				return jsonResponse(map[string]any{
					"code": 0,
					"bot": map[string]any{
						"open_id":         "ou_bot_conformance",
						"app_name":        "Beak Conformance Bot",
						"activate_status": 1,
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
	result, err := a.webhook.HandleWebhook(ctx, larksdk.Runtime{
		WorkspaceUUID: firstString(fixture.WorkspaceUUID, "workspace-1"),
		Channel: larksdk.Channel{
			UUID:          firstString(fixture.ChannelUUID, "channel-1"),
			WorkspaceUUID: firstString(fixture.WorkspaceUUID, "workspace-1"),
			Platform:      beaklark.Platform,
		},
		Account:      account,
		Gateway:      &larkGateway{},
		AccountStore: newLarkStore(),
	}, account, fixture.Raw)
	if err != nil || result == nil || result.Ignored || result.Inbound == nil {
		return nil, err
	}
	return []conformance.InboundMessage{convert[conformance.InboundMessage](a.t, result.Inbound)}, nil
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
		Platform:      beaklark.Platform,
		Credential:    credential,
		State:         fixture.AccountState,
	}
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
		}},
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
	mu       sync.Mutex
	messages []wechatsdk.CreateMessageRequest
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

func (g *weixinGateway) StreamSession(context.Context, wechatsdk.StreamSessionRequest, func(wechatsdk.StreamEvent) error) error {
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
