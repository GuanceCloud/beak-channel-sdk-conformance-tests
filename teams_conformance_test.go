package conformancetests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	beakteams "github.com/TrueWatchTech/truewatch-beak-agent-channel-teams"
	teamssdk "github.com/TrueWatchTech/truewatch-beak-agent-channel-teams/sdk"
	conformance "gitlab.jiagouyun.com/guance/beak-agent-channel-sdk/beak-channel-sdk-conformance"
)

func runTeamsConformance(t *testing.T) {
	adapter := &teamsConformanceAdapter{conn: beakteams.NewConnector(), raw: beakteams.Connector{}}
	trueValue := true
	falseValue := false
	conformance.Run(t, conformance.Config{
		Platform:                 beakteams.Platform,
		MetadataProvider:         adapter,
		CredentialSchemaProvider: adapter,
		CredentialValidator:      adapter,
		InboundParser:            adapter,
		Acknowledger:             adapter,
		CredentialCases: []conformance.CredentialValidationCase{{
			Name: "valid client credentials",
			Request: conformance.CredentialValidationRequest{
				WorkspaceUUID: "ws-1", ChannelUUID: "channel-1", Platform: beakteams.Platform,
				Credential: map[string]any{"client_id": "app-id", "client_secret": "secret"},
			},
			Expect: conformance.CredentialValidationExpectation{
				Valid: true, AccountKey: "app-id", DisplayName: "app-id", MetadataPlatform: beakteams.Platform,
				RequireAccountID: true, RequireBotIdentity: true,
			},
		}},
		InboundCases: []conformance.InboundCase{
			{
				Name: "real Teams bot mention uses recipient identity",
				Fixture: teamsFixture(`{
					"type":"message","id":"mention-1","channelId":"msteams",
					"serviceUrl":"https://smba.trafficmanager.net/amer/","text":"<at>Beak Bot</at> hello",
					"conversation":{"id":"C1","conversationType":"channel"},
					"from":{"id":"29:alice","name":"Alice"},"recipient":{"id":"28:app-id","name":"Beak Bot"},
					"entities":[{"type":"mention","text":"<at>Beak Bot</at>","mentioned":{"id":"28:app-id","name":"Beak Bot"}}]
				}`),
				Expect: conformance.InboundExpectation{
					ChatType: "group", ChatID: "C1", SenderID: "29:alice", SenderDisplayName: "Alice", Text: "hello",
					MentionedMe: &trueValue, MentionIDs: []string{"28:app-id"}, RequireMessageID: true, RequireDedupeKey: true,
				},
			},
			{
				Name: "pure bot mention remains a follow-up event",
				Fixture: teamsFixture(`{
					"type":"message","id":"mention-only-1","serviceUrl":"https://smba.trafficmanager.net/amer/","text":"<at>Beak Bot</at>",
					"conversation":{"id":"C1","conversationType":"channel"},
					"from":{"id":"29:alice"},"recipient":{"id":"28:app-id"},
					"entities":[{"type":"mention","text":"<at>Beak Bot</at>","mentioned":{"id":"28:app-id","name":"Beak Bot"}}]
				}`),
				Expect: conformance.InboundExpectation{
					ChatType: "group", ChatID: "C1", SenderID: "29:alice", TextTrimmedEmpty: &trueValue,
					MentionedMe: &trueValue, MentionIDs: []string{"28:app-id"}, RequireDedupeKey: true,
				},
			},
			{
				Name: "other mention does not imply mentioned me",
				Fixture: teamsFixture(`{
					"type":"message","id":"mention-other-1","serviceUrl":"https://smba.trafficmanager.net/amer/","text":"<at>Bob</at> check",
					"conversation":{"id":"C1","conversationType":"channel"},
					"from":{"id":"29:alice"},"recipient":{"id":"28:app-id"},
					"entities":[{"type":"mention","text":"<at>Bob</at>","mentioned":{"id":"29:bob","name":"Bob"}}]
				}`),
				Expect: conformance.InboundExpectation{
					ChatType: "group", ChatID: "C1", SenderID: "29:alice", Text: "<at>Bob</at> check",
					MentionedMe: &falseValue, MentionIDs: []string{"29:bob"},
				},
			},
			{
				Name: "reply and adaptive card expose common fields",
				Fixture: teamsFixture(`{
					"type":"message","id":"card-1","serviceUrl":"https://smba.trafficmanager.net/amer/","replyToId":"parent-1",
					"conversation":{"id":"C1","name":"Operations","conversationType":"channel"},
					"channelData":{"channel":{"id":"channel-1","name":"Alerts"}},
					"from":{"id":"29:alice","name":"Alice"},"recipient":{"id":"28:app-id"},
					"attachments":[{"contentType":"application/vnd.microsoft.card.adaptive","content":{"type":"AdaptiveCard","body":[{"type":"TextBlock","text":"CPU alert"}]}}]
				}`),
				Expect: conformance.InboundExpectation{
					ChatType: "group", ChatID: "C1", ThreadID: "parent-1", ChatDisplayName: "Alerts", ChatIdentityID: "C1",
					SenderID: "29:alice", SenderDisplayName: "Alice", Text: "CPU alert", RequireMessageID: true,
					ReferencedMessage: &conformance.ReferencedMessageExpectation{
						Platform: beakteams.Platform, MessageID: "parent-1", ChatType: "group", ChatID: "C1", ThreadID: "parent-1", MessageType: "message",
					},
				},
			},
		},
		AckCases: []conformance.AckCase{{
			Name: "unsupported reaction",
			Request: conformance.OutboundAck{
				Platform: beakteams.Platform, AccountUUID: "acct-1", ChatType: "group", ChatID: "C1", Mode: "reaction", Emoji: "eyes",
			},
			Expect: conformance.AckExpectation{Status: "unsupported", Mode: "reaction"},
		}},
	})
}

func TestTeamsWebhookErrorContract(t *testing.T) {
	connector := beakteams.Connector{}
	account := teamssdk.ChannelAccount{
		UUID:       "acct-1",
		Platform:   beakteams.Platform,
		Credential: map[string]any{"client_id": "app-id", "client_secret": "secret"},
	}
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{name: "malformed payload", body: `{`, wantStatus: http.StatusBadRequest, wantCode: "invalid_request_body"},
		{
			name:       "authentication failure",
			body:       `{"type":"message","id":"activity-1","serviceUrl":"https://smba.trafficmanager.net/amer/","channelId":"msteams","conversation":{"id":"C1"},"from":{"id":"29:user"},"recipient":{"id":"28:app-id"}}`,
			wantStatus: http.StatusForbidden,
			wantCode:   "channel_webhook_auth_failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(tt.body))
			_, err := connector.HandleWebhookRequest(context.Background(), teamssdk.Runtime{}, account, req)
			conformance.AssertWebhookError(t, err, tt.wantStatus, tt.wantCode)
		})
	}
}

func teamsFixture(raw string) conformance.InboundFixture {
	return conformance.InboundFixture{
		WorkspaceUUID: "ws-1", ChannelUUID: "channel-1", AccountUUID: "acct-1", Platform: beakteams.Platform,
		Credential:   map[string]any{"account_id": "app-id", "bot_id": "app-id", "client_id": "app-id", "client_secret": "secret"},
		AccountState: map[string]any{"bot_id": "app-id"}, Raw: json.RawMessage(raw),
	}
}

type teamsConformanceAdapter struct {
	conn teamssdk.Connector
	raw  beakteams.Connector
}

func (a *teamsConformanceAdapter) Metadata() conformance.ConnectorMetadata {
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

func (a *teamsConformanceAdapter) CredentialSchema(ctx context.Context) conformance.CredentialSchema {
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

func (a *teamsConformanceAdapter) ValidateCredential(ctx context.Context, req conformance.CredentialValidationRequest) (*conformance.CredentialValidationResult, error) {
	result, err := a.conn.ValidateCredential(ctx, teamssdk.CredentialValidationRequest{
		WorkspaceUUID: req.WorkspaceUUID, ChannelUUID: req.ChannelUUID, Platform: req.Platform,
		Credential: req.Credential, State: req.State,
		Runtime: teamssdk.Runtime{HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(map[string]any{"access_token": "token", "expires_in": 3600})
		})}},
	})
	if err != nil || result == nil {
		return nil, err
	}
	return &conformance.CredentialValidationResult{
		Valid: result.Valid, AccountKey: result.AccountKey, DisplayName: result.DisplayName,
		Credential: result.Credential, State: result.State, Metadata: result.Metadata, Error: result.Error,
	}, nil
}

func (a *teamsConformanceAdapter) ParseInbound(ctx context.Context, fixture conformance.InboundFixture) ([]conformance.InboundMessage, error) {
	result, err := a.raw.HandleWebhook(ctx, teamssdk.Runtime{
		WorkspaceUUID: fixture.WorkspaceUUID,
		Channel:       teamssdk.Channel{UUID: fixture.ChannelUUID, WorkspaceUUID: fixture.WorkspaceUUID, Platform: fixture.Platform},
		Gateway:       &teamsConformanceGateway{}, AccountStore: newTeamsConformanceStore(),
	}, teamssdk.ChannelAccount{
		UUID: fixture.AccountUUID, WorkspaceUUID: fixture.WorkspaceUUID, ChannelUUID: fixture.ChannelUUID,
		Platform: fixture.Platform, Credential: fixture.Credential, State: fixture.AccountState,
	}, fixture.Raw)
	if err != nil {
		return nil, err
	}
	if result == nil || result.Inbound == nil {
		return nil, nil
	}
	in := result.Inbound
	message := conformance.InboundMessage{
		WorkspaceUUID: in.WorkspaceUUID, Platform: in.Platform, AccountUUID: in.AccountUUID, ChannelUUID: in.ChannelUUID,
		ChatType: in.ChatType, ChatID: in.ChatID, ThreadID: in.ThreadID, ChatDisplayName: in.ChatDisplayName,
		ChatAvatarURL: in.ChatAvatarURL,
		ChatIdentity: conformance.ChatIdentity{
			ID: in.ChatIdentity.ID, IDType: in.ChatIdentity.IDType, Type: in.ChatIdentity.Type,
			DisplayName: in.ChatIdentity.DisplayName, AvatarURL: in.ChatIdentity.AvatarURL,
		},
		SenderID: in.SenderID, SenderDisplayName: in.SenderDisplayName, MessageID: in.MessageID, Text: in.Text,
		DedupeKey: in.DedupeKey, MentionedMe: in.MentionedMe, MentionAll: in.MentionAll, Raw: in.Raw,
	}
	for _, mention := range in.Mentions {
		message.Mentions = append(message.Mentions, conformance.MentionIdentity{ID: mention.ID, IDType: mention.IDType, DisplayName: mention.DisplayName})
	}
	if ref := in.ReferencedMessage; ref != nil {
		message.ReferencedMessage = &conformance.ReferencedMessage{
			Platform: ref.Platform, MessageID: ref.MessageID, ChatType: ref.ChatType, ChatID: ref.ChatID,
			ThreadID: ref.ThreadID, RootID: ref.RootID, SenderID: ref.SenderID, SenderDisplayName: ref.SenderDisplayName,
			MessageType: ref.MessageType, Text: ref.Text, CreatedAt: ref.CreatedAt, Raw: ref.Raw,
		}
	}
	return []conformance.InboundMessage{message}, nil
}

func (a *teamsConformanceAdapter) Acknowledge(ctx context.Context, req conformance.OutboundAck) (*conformance.AckResult, error) {
	result, err := a.conn.Acknowledge(ctx, teamssdk.Runtime{}, teamssdk.OutboundAck{
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

type teamsConformanceGateway struct{}

func (*teamsConformanceGateway) EnsureChannel(context.Context, teamssdk.EnsureChannelRequest) (string, error) {
	return "channel-1", nil
}
func (*teamsConformanceGateway) EnsureChannelLinkSession(_ context.Context, req teamssdk.EnsureChannelLinkSessionRequest) (string, error) {
	return "link-" + req.AccountUUID, nil
}
func (*teamsConformanceGateway) EnsureChatSession(_ context.Context, req teamssdk.EnsureChatSessionRequest) (string, error) {
	return "session-" + req.AccountUUID + "-" + req.ChatType + "-" + req.ChatID, nil
}
func (*teamsConformanceGateway) CreateMessage(context.Context, teamssdk.CreateMessageRequest) (string, error) {
	return "message-1", nil
}
func (*teamsConformanceGateway) StreamSession(context.Context, teamssdk.StreamSessionRequest, func(teamssdk.StreamEvent) error) error {
	return nil
}
func (*teamsConformanceGateway) AgentParticipantID() string { return "agent:agent-1" }
func (*teamsConformanceGateway) BridgeParticipantID(platform string) string {
	return teamssdk.BridgeParticipantID(platform)
}

type teamsConformanceStore struct {
	mu     sync.Mutex
	states map[string]map[string]any
}

func newTeamsConformanceStore() *teamsConformanceStore {
	return &teamsConformanceStore{states: make(map[string]map[string]any)}
}
func (s *teamsConformanceStore) LoadChannelAccountState(_ context.Context, accountUUID string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.states[accountUUID], nil
}
func (s *teamsConformanceStore) SaveChannelAccountState(_ context.Context, accountUUID string, state map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[accountUUID] = state
	return nil
}
