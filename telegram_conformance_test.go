package conformancetests

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	beaktelegram "github.com/TrueWatchTech/truewatch-beak-agent-channel-telegram"
	telegramsdk "github.com/TrueWatchTech/truewatch-beak-agent-channel-telegram/sdk"
	conformance "gitlab.jiagouyun.com/guance/beak-agent-channel-sdk/beak-channel-sdk-conformance"
)

func runTelegramConformance(t *testing.T) {
	adapter := &telegramConformanceAdapter{conn: beaktelegram.NewConnector(), raw: beaktelegram.Connector{}}
	trueValue := true
	conformance.Run(t, conformance.Config{
		Platform:                 beaktelegram.Platform,
		MetadataProvider:         adapter,
		CredentialSchemaProvider: adapter,
		CredentialValidator:      adapter,
		InboundParser:            adapter,
		Acknowledger:             adapter,
		CredentialCases: []conformance.CredentialValidationCase{{
			Name: "valid bot token",
			Request: conformance.CredentialValidationRequest{
				WorkspaceUUID: "ws-1", ChannelUUID: "channel-1", Platform: beaktelegram.Platform,
				Credential: map[string]any{"bot_token": "123456:ABCdef-test-token"},
			},
			Expect: conformance.CredentialValidationExpectation{
				Valid: true, AccountKey: "123456", DisplayName: "@testbot", MetadataPlatform: beaktelegram.Platform,
				RequireAccountID: true, RequireBotIdentity: true, RequiredCredentialKeys: []string{"webhook_secret"},
			},
		}},
		InboundCases: []conformance.InboundCase{
			{
				Name: "UTF-16 mention and topic reply expose common fields",
				Fixture: telegramFixture(`{
					"update_id":101,
					"message":{
						"message_id":201,"message_thread_id":77,"date":1720000000,
						"from":{"id":999,"first_name":"Alice","last_name":"Wu"},
						"chat":{"id":-200,"type":"supergroup","title":"Ops"},
						"text":"😀 hi @testbot",
						"entities":[{"type":"mention","offset":6,"length":8}],
						"reply_to_message":{
							"message_id":199,"message_thread_id":77,
							"from":{"id":998,"first_name":"Bob"},
							"chat":{"id":-200,"type":"supergroup","title":"Ops"},
							"text":"original"
						}
					}
				}`),
				Expect: conformance.InboundExpectation{
					ChatType: "group", ChatID: "-200", ThreadID: "77", ChatDisplayName: "Ops", ChatIdentityID: "-200",
					SenderID: "999", SenderDisplayName: "Alice Wu", Text: "😀 hi", MentionedMe: &trueValue,
					MentionIDs: []string{"testbot"}, RequireMessageID: true, RequireDedupeKey: true,
					ReferencedMessage: &conformance.ReferencedMessageExpectation{
						Platform: beaktelegram.Platform, MessageID: "199", ChatType: "group", ChatID: "-200",
						ThreadID: "77", SenderID: "998", SenderDisplayName: "Bob", MessageType: "text", Text: "original", RequireText: true,
					},
				},
			},
			{
				Name: "pure bot mention remains a follow-up event",
				Fixture: telegramFixture(`{
					"update_id":102,
					"message":{
						"message_id":202,"from":{"id":999,"first_name":"Alice"},
						"chat":{"id":-200,"type":"group","title":"Ops"},
						"text":"@testbot","entities":[{"type":"mention","offset":0,"length":8}]
					}
				}`),
				Expect: conformance.InboundExpectation{
					ChatType: "group", ChatID: "-200", SenderID: "999", TextTrimmedEmpty: &trueValue,
					MentionedMe: &trueValue, MentionIDs: []string{"testbot"}, RequireDedupeKey: true,
				},
			},
			{
				Name: "captioned media exposes caption text",
				Fixture: telegramFixture(`{
					"update_id":103,
					"message":{
						"message_id":203,"from":{"id":997,"first_name":"Carol"},
						"chat":{"id":-201,"type":"group","title":"Images"},
						"caption":"inspect this","photo":[{"file_id":"photo-1","width":10,"height":10}]
					}
				}`),
				Expect: conformance.InboundExpectation{
					ChatType: "group", ChatID: "-201", SenderID: "997", Text: "inspect this",
					RequireMessageID: true, RequireDedupeKey: true,
				},
			},
			{
				Name: "business message keeps an isolated reply target",
				Fixture: telegramFixture(`{
					"update_id":104,
					"business_message":{
						"message_id":204,"business_connection_id":"bc:1",
						"from":{"id":996,"first_name":"Dana"},
						"chat":{"id":4242,"type":"private","first_name":"Dana"},
						"text":"business question"
					}
				}`),
				Expect: conformance.InboundExpectation{
					ChatType: "direct", ChatID: "business:YmM6MQ:4242", ChatIdentityID: "business:YmM6MQ:4242",
					SenderID: "996", SenderDisplayName: "Dana", Text: "business question",
					RequireMessageID: true, RequireDedupeKey: true,
				},
			},
			{
				Name: "direct messages topic exposes an opaque direct chat and common thread",
				Fixture: telegramFixture(`{
					"update_id":105,
					"message":{
						"message_id":205,
						"from":{"id":995,"first_name":"Erin"},
						"chat":{"id":-900,"type":"channel","title":"Support","is_direct_messages":true},
						"direct_messages_topic":{"topic_id":88,"user":{"id":995,"first_name":"Erin"}},
						"text":"direct topic question"
					}
				}`),
				Expect: conformance.InboundExpectation{
					ChatType: "direct", ChatID: "direct_messages:88:-900", ThreadID: "direct_messages:88",
					ChatDisplayName: "Support", ChatIdentityID: "direct_messages:88:-900",
					SenderID: "995", SenderDisplayName: "Erin", Text: "direct topic question",
					RequireMessageID: true, RequireDedupeKey: true,
				},
			},
			{
				Name: "edited message is explicitly ignored until Beak exposes message updates",
				Fixture: telegramFixture(`{
					"update_id":106,
					"edited_message":{
						"message_id":206,
						"from":{"id":994,"first_name":"Frank"},
						"chat":{"id":-202,"type":"group","title":"Ops"},
						"text":"edited text"
					}
				}`),
				Expect: conformance.InboundExpectation{ExpectNoMessages: true},
			},
		},
		AckCases: []conformance.AckCase{{
			Name: "processing reaction",
			Request: conformance.OutboundAck{
				Platform: beaktelegram.Platform, AccountUUID: "123456", ChatType: "group", ChatID: "-200",
				TargetMessageID: "201", Mode: "reaction", Action: "start", Intent: "processing",
			},
			Expect: conformance.AckExpectation{Status: "sent", Mode: "reaction", ReactionID: "👀"},
		}},
	})
}

func TestTelegramWebhookErrorContract(t *testing.T) {
	connector := beaktelegram.Connector{}
	account := telegramsdk.ChannelAccount{
		UUID:     "acct-1",
		Platform: beaktelegram.Platform,
		Credential: map[string]any{
			"account_id": "123456", "bot_id": "123456", "bot_token": "123456:ABCdef-test-token", "webhook_secret": "secret",
		},
	}
	tests := []struct {
		name       string
		body       string
		secret     string
		wantStatus int
		wantCode   string
	}{
		{name: "malformed payload", body: `{`, secret: "secret", wantStatus: http.StatusBadRequest, wantCode: "invalid_request_body"},
		{name: "authentication failure", body: `{"update_id":1}`, secret: "wrong", wantStatus: http.StatusForbidden, wantCode: "channel_webhook_auth_failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(tt.body))
			req.Header.Set("X-Telegram-Bot-Api-Secret-Token", tt.secret)
			_, err := connector.HandleWebhookRequest(context.Background(), telegramsdk.Runtime{}, account, req)
			conformance.AssertWebhookError(t, err, tt.wantStatus, tt.wantCode)
		})
	}
}

func TestTelegramTransientCredentialFailureRemainsAHostError(t *testing.T) {
	adapter := &telegramConformanceAdapter{
		conn: beaktelegram.NewConnector(),
		raw:  beaktelegram.Connector{},
		httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"ok":false,"error_code":503,"description":"temporary unavailable"}`)),
			}, nil
		})},
	}
	req := conformance.CredentialValidationRequest{
		Platform: beaktelegram.Platform,
		Credential: map[string]any{
			"bot_token":      "123456:ABCdef-test-token",
			"webhook_secret": "test-webhook-secret",
		},
	}
	result, err := adapter.ValidateCredential(context.Background(), req)
	conformance.AssertCredentialValidationResult(t, req, result, err, conformance.CredentialValidationExpectation{RequireGoError: true})
}

func TestTelegramStartRegistersHostWebhook(t *testing.T) {
	var payload map[string]any
	httpClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(req.URL.Path, "/setWebhook") {
			t.Fatalf("request path = %q, want setWebhook", req.URL.Path)
		}
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			t.Fatalf("decode setWebhook payload: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true,"result":true}`)),
		}, nil
	})}
	account := telegramsdk.ChannelAccount{
		UUID: "acct-1", WorkspaceUUID: "ws-1", ChannelUUID: "channel-1", Platform: beaktelegram.Platform,
		Credential: map[string]any{
			"account_id": "123456", "bot_id": "123456", "bot_token": "123456:ABCdef-test-token",
			"webhook_secret": "generated-secret",
		},
	}
	webhookURL := "https://beak.example.com/api/v1/channel-webhooks/telegram/acct-1"
	err := (beaktelegram.Connector{}).Start(context.Background(), telegramsdk.Runtime{
		WorkspaceUUID: "ws-1",
		Channel: telegramsdk.Channel{
			UUID: "channel-1", WorkspaceUUID: "ws-1", Platform: beaktelegram.Platform,
		},
		Account: account, Accounts: []telegramsdk.ChannelAccount{account},
		Webhook: &telegramsdk.WebhookEndpoint{URL: webhookURL},
		Gateway: &telegramConformanceGateway{}, AccountStore: newTelegramConformanceStore(), HTTPClient: httpClient,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if payload["url"] != webhookURL || payload["secret_token"] != "generated-secret" {
		t.Fatalf("setWebhook payload = %#v", payload)
	}
}

func telegramFixture(raw string) conformance.InboundFixture {
	return conformance.InboundFixture{
		WorkspaceUUID: "ws-1", ChannelUUID: "channel-1", AccountUUID: "acct-1", Platform: beaktelegram.Platform,
		Credential: map[string]any{
			"account_id": "123456", "bot_id": "123456", "bot_token": "123456:ABCdef-test-token",
		},
		AccountState: map[string]any{"bot_id": "123456", "bot_username": "testbot"},
		Raw:          json.RawMessage(raw),
	}
}

type telegramConformanceAdapter struct {
	conn       telegramsdk.Connector
	raw        beaktelegram.Connector
	httpClient *http.Client
}

func (a *telegramConformanceAdapter) Metadata() conformance.ConnectorMetadata {
	m := a.conn.Metadata()
	return conformance.ConnectorMetadata{
		ID: m.ID, Platform: m.Platform, Label: m.Label, Description: m.Description,
		Capabilities: conformance.Capabilities{
			LoginModes: m.Capabilities.LoginModes, Text: m.Capabilities.Text, Media: m.Capabilities.Media,
			GroupChat: m.Capabilities.GroupChat, DirectChat: m.Capabilities.DirectChat, Stream: m.Capabilities.Stream,
			Webhook: m.Capabilities.Webhook, WebhookRegistration: m.Capabilities.WebhookRegistration,
			BlockStreaming: m.Capabilities.BlockStreaming, AckModes: m.Capabilities.AckModes,
			RuntimeOwnership: m.Capabilities.RuntimeOwnership,
		},
	}
}

func (a *telegramConformanceAdapter) CredentialSchema(ctx context.Context) conformance.CredentialSchema {
	schema := a.conn.CredentialSchema(ctx)
	properties := make(map[string]conformance.CredentialField, len(schema.Properties))
	for key, field := range schema.Properties {
		properties[key] = conformance.CredentialField{Type: field.Type, Title: field.Title, Description: field.Description, Secret: field.Secret}
	}
	return conformance.CredentialSchema{
		Type: schema.Type, LoginModes: schema.LoginModes, Properties: properties, Required: schema.Required,
		AdditionalProperties: schema.AdditionalProperties,
	}
}

func (a *telegramConformanceAdapter) ValidateCredential(ctx context.Context, req conformance.CredentialValidationRequest) (*conformance.CredentialValidationResult, error) {
	httpClient := a.httpClient
	if httpClient == nil {
		httpClient = telegramConformanceHTTPClient()
	}
	result, err := a.conn.ValidateCredential(ctx, telegramsdk.CredentialValidationRequest{
		WorkspaceUUID: req.WorkspaceUUID, ChannelUUID: req.ChannelUUID, Platform: req.Platform,
		Credential: req.Credential, State: req.State, Runtime: telegramsdk.Runtime{HTTPClient: httpClient},
	})
	if err != nil || result == nil {
		return nil, err
	}
	return &conformance.CredentialValidationResult{
		Valid: result.Valid, AccountKey: result.AccountKey, DisplayName: result.DisplayName,
		Credential: result.Credential, State: result.State, Metadata: result.Metadata, Error: result.Error,
	}, nil
}

func (a *telegramConformanceAdapter) ParseInbound(ctx context.Context, fixture conformance.InboundFixture) ([]conformance.InboundMessage, error) {
	account := telegramsdk.ChannelAccount{
		UUID: fixture.AccountUUID, WorkspaceUUID: fixture.WorkspaceUUID, ChannelUUID: fixture.ChannelUUID,
		Platform: fixture.Platform, Credential: fixture.Credential, State: fixture.AccountState,
	}
	result, err := a.raw.HandleWebhook(ctx, telegramsdk.Runtime{
		WorkspaceUUID: fixture.WorkspaceUUID,
		Channel: telegramsdk.Channel{
			UUID: fixture.ChannelUUID, WorkspaceUUID: fixture.WorkspaceUUID, Platform: fixture.Platform,
		},
		Gateway: &telegramConformanceGateway{}, AccountStore: newTelegramConformanceStore(),
	}, account, fixture.Raw)
	if err != nil {
		return nil, err
	}
	if result == nil || result.Inbound == nil {
		return []conformance.InboundMessage{}, nil
	}
	in := result.Inbound
	return []conformance.InboundMessage{{
		WorkspaceUUID: in.WorkspaceUUID, Platform: in.Platform, AccountUUID: in.AccountUUID, ChannelUUID: in.ChannelUUID,
		ChatType: in.ChatType, ChatID: in.ChatID, ThreadID: in.ThreadID, ChatDisplayName: in.ChatDisplayName,
		ChatAvatarURL: in.ChatAvatarURL,
		ChatIdentity: conformance.ChatIdentity{
			ID: in.ChatIdentity.ID, IDType: in.ChatIdentity.IDType, Type: in.ChatIdentity.Type,
			DisplayName: in.ChatIdentity.DisplayName, AvatarURL: in.ChatIdentity.AvatarURL,
		},
		SenderID: in.SenderID, SenderDisplayName: in.SenderDisplayName, MessageID: in.MessageID, Text: in.Text,
		ReferencedMessage: telegramConformanceReference(in.ReferencedMessage), DedupeKey: in.DedupeKey,
		Mentions: telegramConformanceMentions(in.Mentions), MentionedMe: in.MentionedMe, MentionAll: in.MentionAll, Raw: in.Raw,
	}}, nil
}

func (a *telegramConformanceAdapter) Acknowledge(ctx context.Context, req conformance.OutboundAck) (*conformance.AckResult, error) {
	account := telegramsdk.ChannelAccount{
		UUID: req.AccountUUID, Platform: beaktelegram.Platform,
		Credential: map[string]any{"account_id": req.AccountUUID, "bot_id": req.AccountUUID, "bot_token": "123456:ABCdef-test-token"},
	}
	result, err := a.conn.Acknowledge(ctx, telegramsdk.Runtime{
		Account: account, Accounts: []telegramsdk.ChannelAccount{account}, HTTPClient: telegramConformanceHTTPClient(),
	}, telegramsdk.OutboundAck{
		WorkspaceUUID: req.WorkspaceUUID, Platform: req.Platform, AccountUUID: req.AccountUUID, ChannelUUID: req.ChannelUUID,
		SessionUUID: req.SessionUUID, SourceMessageUUID: req.SourceMessageUUID, ChatType: req.ChatType, ChatID: req.ChatID,
		TargetMessageID: req.TargetMessageID, Intent: req.Intent, Action: req.Action, Mode: req.Mode, Emoji: req.Emoji, Raw: req.Raw,
	})
	if err != nil || result == nil {
		return nil, err
	}
	return &conformance.AckResult{
		Platform: result.Platform, AccountUUID: result.AccountUUID, Mode: result.Mode,
		Status: result.Status, ReactionID: result.ReactionID, Raw: result.Raw,
	}, nil
}

func telegramConformanceReference(ref *telegramsdk.ReferencedMessage) *conformance.ReferencedMessage {
	if ref == nil {
		return nil
	}
	return &conformance.ReferencedMessage{
		Platform: ref.Platform, MessageID: ref.MessageID, ChatType: ref.ChatType, ChatID: ref.ChatID,
		ThreadID: ref.ThreadID, RootID: ref.RootID, SenderID: ref.SenderID, SenderDisplayName: ref.SenderDisplayName,
		MessageType: ref.MessageType, Text: ref.Text, CreatedAt: ref.CreatedAt, Raw: ref.Raw,
	}
}

func telegramConformanceMentions(mentions []telegramsdk.MentionIdentity) []conformance.MentionIdentity {
	if len(mentions) == 0 {
		return nil
	}
	out := make([]conformance.MentionIdentity, len(mentions))
	for i, mention := range mentions {
		out[i] = conformance.MentionIdentity{ID: mention.ID, IDType: mention.IDType, DisplayName: mention.DisplayName}
	}
	return out
}

func telegramConformanceHTTPClient() *http.Client {
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := `{"ok":true,"result":{"id":123456,"is_bot":true,"first_name":"Test Bot","username":"testbot"}}`
		if strings.HasSuffix(req.URL.Path, "/setMessageReaction") {
			body = `{"ok":true,"result":true}`
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
}

type telegramConformanceGateway struct{}

func (*telegramConformanceGateway) EnsureChannel(context.Context, telegramsdk.EnsureChannelRequest) (string, error) {
	return "channel-1", nil
}
func (*telegramConformanceGateway) EnsureChannelLinkSession(_ context.Context, req telegramsdk.EnsureChannelLinkSessionRequest) (string, error) {
	return "link-" + req.AccountUUID, nil
}
func (*telegramConformanceGateway) EnsureChatSession(_ context.Context, req telegramsdk.EnsureChatSessionRequest) (string, error) {
	return "session-" + req.AccountUUID + "-" + req.ChatType + "-" + req.ChatID, nil
}
func (*telegramConformanceGateway) CreateMessage(context.Context, telegramsdk.CreateMessageRequest) (string, error) {
	return "message-1", nil
}
func (*telegramConformanceGateway) StreamSession(context.Context, telegramsdk.StreamSessionRequest, func(telegramsdk.StreamEvent) error) error {
	return nil
}
func (*telegramConformanceGateway) AgentParticipantID() string { return "agent:agent-1" }
func (*telegramConformanceGateway) BridgeParticipantID(platform string) string {
	return telegramsdk.BridgeParticipantID(platform)
}

type telegramConformanceStore struct {
	mu     sync.Mutex
	states map[string]map[string]any
}

func newTelegramConformanceStore() *telegramConformanceStore {
	return &telegramConformanceStore{states: make(map[string]map[string]any)}
}
func (s *telegramConformanceStore) LoadChannelAccountState(_ context.Context, accountUUID string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.states[accountUUID], nil
}
func (s *telegramConformanceStore) SaveChannelAccountState(_ context.Context, accountUUID string, state map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[accountUUID] = state
	return nil
}
