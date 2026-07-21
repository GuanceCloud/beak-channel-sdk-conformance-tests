package conformancetests

import (
	"reflect"
	"testing"

	dingtalksdk "github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
	larksdk "github.com/GuanceCloud/beak-agent-channel-lark/sdk"
	wechatsdk "github.com/GuanceCloud/beak-agent-channel-wechat/sdk"
	wecomsdk "github.com/GuanceCloud/beak-agent-wecom/sdk"
	slacksdk "github.com/TrueWatchTech/truewatch-beak-agent-channel-slack/sdk"
	teamssdk "github.com/TrueWatchTech/truewatch-beak-agent-channel-teams/sdk"
	telegramsdk "github.com/TrueWatchTech/truewatch-beak-agent-channel-telegram/sdk"
	conformance "gitlab.jiagouyun.com/guance/beak-agent-channel-sdk/beak-channel-sdk-conformance"
)

type runtimeHealthContract struct {
	keys   [13]string
	states [5]string
}

func TestSDKRuntimeHealthContractIsUniform(t *testing.T) {
	want := dingtalkRuntimeHealthContract()
	contracts := map[string]runtimeHealthContract{
		"dingtalk": want,
		"conformance": {
			keys: [13]string{
				conformance.RuntimeHealthKeyStreamConnectionState, conformance.RuntimeHealthKeyStreamConnectedAt,
				conformance.RuntimeHealthKeyStreamDisconnectedAt, conformance.RuntimeHealthKeyStreamLastActivityAt,
				conformance.RuntimeHealthKeyStreamLastPingAt, conformance.RuntimeHealthKeyStreamLastPongAt,
				conformance.RuntimeHealthKeyStreamLastEventAt, conformance.RuntimeHealthKeyStreamLastError,
				conformance.RuntimeHealthKeyStreamLastErrorAt, conformance.RuntimeHealthKeyStreamReconnectRequestedAt,
				conformance.RuntimeHealthKeyStreamReconnectError, conformance.RuntimeHealthKeyStreamReconnectErrorAt,
				conformance.RuntimeHealthKeyStreamSessionExpired,
			},
			states: [5]string{conformance.RuntimeHealthStateConnected, conformance.RuntimeHealthStateReconnecting, conformance.RuntimeHealthStateReconnectFailed, conformance.RuntimeHealthStateStopped, conformance.RuntimeHealthStateExpired},
		},
		"lark": {
			keys: [13]string{
				larksdk.RuntimeHealthKeyStreamConnectionState, larksdk.RuntimeHealthKeyStreamConnectedAt,
				larksdk.RuntimeHealthKeyStreamDisconnectedAt, larksdk.RuntimeHealthKeyStreamLastActivityAt,
				larksdk.RuntimeHealthKeyStreamLastPingAt, larksdk.RuntimeHealthKeyStreamLastPongAt,
				larksdk.RuntimeHealthKeyStreamLastEventAt, larksdk.RuntimeHealthKeyStreamLastError,
				larksdk.RuntimeHealthKeyStreamLastErrorAt, larksdk.RuntimeHealthKeyStreamReconnectRequestedAt,
				larksdk.RuntimeHealthKeyStreamReconnectError, larksdk.RuntimeHealthKeyStreamReconnectErrorAt,
				larksdk.RuntimeHealthKeyStreamSessionExpired,
			},
			states: [5]string{larksdk.RuntimeHealthStateConnected, larksdk.RuntimeHealthStateReconnecting, larksdk.RuntimeHealthStateReconnectFailed, larksdk.RuntimeHealthStateStopped, larksdk.RuntimeHealthStateExpired},
		},
		"weixin": {
			keys: [13]string{
				wechatsdk.RuntimeHealthKeyStreamConnectionState, wechatsdk.RuntimeHealthKeyStreamConnectedAt,
				wechatsdk.RuntimeHealthKeyStreamDisconnectedAt, wechatsdk.RuntimeHealthKeyStreamLastActivityAt,
				wechatsdk.RuntimeHealthKeyStreamLastPingAt, wechatsdk.RuntimeHealthKeyStreamLastPongAt,
				wechatsdk.RuntimeHealthKeyStreamLastEventAt, wechatsdk.RuntimeHealthKeyStreamLastError,
				wechatsdk.RuntimeHealthKeyStreamLastErrorAt, wechatsdk.RuntimeHealthKeyStreamReconnectRequestedAt,
				wechatsdk.RuntimeHealthKeyStreamReconnectError, wechatsdk.RuntimeHealthKeyStreamReconnectErrorAt,
				wechatsdk.RuntimeHealthKeyStreamSessionExpired,
			},
			states: [5]string{wechatsdk.RuntimeHealthStateConnected, wechatsdk.RuntimeHealthStateReconnecting, wechatsdk.RuntimeHealthStateReconnectFailed, wechatsdk.RuntimeHealthStateStopped, wechatsdk.RuntimeHealthStateExpired},
		},
		"wecom": {
			keys: [13]string{
				wecomsdk.RuntimeHealthKeyStreamConnectionState, wecomsdk.RuntimeHealthKeyStreamConnectedAt,
				wecomsdk.RuntimeHealthKeyStreamDisconnectedAt, wecomsdk.RuntimeHealthKeyStreamLastActivityAt,
				wecomsdk.RuntimeHealthKeyStreamLastPingAt, wecomsdk.RuntimeHealthKeyStreamLastPongAt,
				wecomsdk.RuntimeHealthKeyStreamLastEventAt, wecomsdk.RuntimeHealthKeyStreamLastError,
				wecomsdk.RuntimeHealthKeyStreamLastErrorAt, wecomsdk.RuntimeHealthKeyStreamReconnectRequestedAt,
				wecomsdk.RuntimeHealthKeyStreamReconnectError, wecomsdk.RuntimeHealthKeyStreamReconnectErrorAt,
				wecomsdk.RuntimeHealthKeyStreamSessionExpired,
			},
			states: [5]string{wecomsdk.RuntimeHealthStateConnected, wecomsdk.RuntimeHealthStateReconnecting, wecomsdk.RuntimeHealthStateReconnectFailed, wecomsdk.RuntimeHealthStateStopped, wecomsdk.RuntimeHealthStateExpired},
		},
		"slack": {
			keys: [13]string{
				slacksdk.RuntimeHealthKeyStreamConnectionState, slacksdk.RuntimeHealthKeyStreamConnectedAt,
				slacksdk.RuntimeHealthKeyStreamDisconnectedAt, slacksdk.RuntimeHealthKeyStreamLastActivityAt,
				slacksdk.RuntimeHealthKeyStreamLastPingAt, slacksdk.RuntimeHealthKeyStreamLastPongAt,
				slacksdk.RuntimeHealthKeyStreamLastEventAt, slacksdk.RuntimeHealthKeyStreamLastError,
				slacksdk.RuntimeHealthKeyStreamLastErrorAt, slacksdk.RuntimeHealthKeyStreamReconnectRequestedAt,
				slacksdk.RuntimeHealthKeyStreamReconnectError, slacksdk.RuntimeHealthKeyStreamReconnectErrorAt,
				slacksdk.RuntimeHealthKeyStreamSessionExpired,
			},
			states: [5]string{slacksdk.RuntimeHealthStateConnected, slacksdk.RuntimeHealthStateReconnecting, slacksdk.RuntimeHealthStateReconnectFailed, slacksdk.RuntimeHealthStateStopped, slacksdk.RuntimeHealthStateExpired},
		},
		"teams": {
			keys: [13]string{
				teamssdk.RuntimeHealthKeyStreamConnectionState, teamssdk.RuntimeHealthKeyStreamConnectedAt,
				teamssdk.RuntimeHealthKeyStreamDisconnectedAt, teamssdk.RuntimeHealthKeyStreamLastActivityAt,
				teamssdk.RuntimeHealthKeyStreamLastPingAt, teamssdk.RuntimeHealthKeyStreamLastPongAt,
				teamssdk.RuntimeHealthKeyStreamLastEventAt, teamssdk.RuntimeHealthKeyStreamLastError,
				teamssdk.RuntimeHealthKeyStreamLastErrorAt, teamssdk.RuntimeHealthKeyStreamReconnectRequestedAt,
				teamssdk.RuntimeHealthKeyStreamReconnectError, teamssdk.RuntimeHealthKeyStreamReconnectErrorAt,
				teamssdk.RuntimeHealthKeyStreamSessionExpired,
			},
			states: [5]string{teamssdk.RuntimeHealthStateConnected, teamssdk.RuntimeHealthStateReconnecting, teamssdk.RuntimeHealthStateReconnectFailed, teamssdk.RuntimeHealthStateStopped, teamssdk.RuntimeHealthStateExpired},
		},
		"telegram": {
			keys: [13]string{
				telegramsdk.RuntimeHealthKeyStreamConnectionState, telegramsdk.RuntimeHealthKeyStreamConnectedAt,
				telegramsdk.RuntimeHealthKeyStreamDisconnectedAt, telegramsdk.RuntimeHealthKeyStreamLastActivityAt,
				telegramsdk.RuntimeHealthKeyStreamLastPingAt, telegramsdk.RuntimeHealthKeyStreamLastPongAt,
				telegramsdk.RuntimeHealthKeyStreamLastEventAt, telegramsdk.RuntimeHealthKeyStreamLastError,
				telegramsdk.RuntimeHealthKeyStreamLastErrorAt, telegramsdk.RuntimeHealthKeyStreamReconnectRequestedAt,
				telegramsdk.RuntimeHealthKeyStreamReconnectError, telegramsdk.RuntimeHealthKeyStreamReconnectErrorAt,
				telegramsdk.RuntimeHealthKeyStreamSessionExpired,
			},
			states: [5]string{telegramsdk.RuntimeHealthStateConnected, telegramsdk.RuntimeHealthStateReconnecting, telegramsdk.RuntimeHealthStateReconnectFailed, telegramsdk.RuntimeHealthStateStopped, telegramsdk.RuntimeHealthStateExpired},
		},
	}
	for platform, got := range contracts {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s runtime health contract = %#v, want %#v", platform, got, want)
		}
	}
}

func dingtalkRuntimeHealthContract() runtimeHealthContract {
	return runtimeHealthContract{
		keys: [13]string{
			dingtalksdk.RuntimeHealthKeyStreamConnectionState, dingtalksdk.RuntimeHealthKeyStreamConnectedAt,
			dingtalksdk.RuntimeHealthKeyStreamDisconnectedAt, dingtalksdk.RuntimeHealthKeyStreamLastActivityAt,
			dingtalksdk.RuntimeHealthKeyStreamLastPingAt, dingtalksdk.RuntimeHealthKeyStreamLastPongAt,
			dingtalksdk.RuntimeHealthKeyStreamLastEventAt, dingtalksdk.RuntimeHealthKeyStreamLastError,
			dingtalksdk.RuntimeHealthKeyStreamLastErrorAt, dingtalksdk.RuntimeHealthKeyStreamReconnectRequestedAt,
			dingtalksdk.RuntimeHealthKeyStreamReconnectError, dingtalksdk.RuntimeHealthKeyStreamReconnectErrorAt,
			dingtalksdk.RuntimeHealthKeyStreamSessionExpired,
		},
		states: [5]string{dingtalksdk.RuntimeHealthStateConnected, dingtalksdk.RuntimeHealthStateReconnecting, dingtalksdk.RuntimeHealthStateReconnectFailed, dingtalksdk.RuntimeHealthStateStopped, dingtalksdk.RuntimeHealthStateExpired},
	}
}
