module gitlab.jiagouyun.com/guance/beak-agent-channel-sdk/beak-channel-sdk-conformance-tests

go 1.23

require (
	github.com/GuanceCloud/beak-agent-channel-dingtalk v0.0.0
	github.com/GuanceCloud/beak-agent-channel-lark v0.0.0
	github.com/GuanceCloud/beak-agent-channel-wechat v0.0.0
	github.com/TrueWatchTech/truewatch-beak-agent-channel-slack/conformance v0.0.0
	github.com/TrueWatchTech/truewatch-beak-agent-channel-teams v0.0.0
	github.com/TrueWatchTech/truewatch-beak-agent-channel-telegram v0.0.0
	github.com/larksuite/oapi-sdk-go/v3 v3.5.3
	github.com/open-dingtalk/dingtalk-stream-sdk-go v0.9.1
	gitlab.jiagouyun.com/guance/beak-agent-channel-sdk/beak-channel-sdk-conformance v0.0.0
)

require (
	github.com/TrueWatchTech/truewatch-beak-agent-channel-slack v0.0.0-00010101000000-000000000000 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
)

replace github.com/GuanceCloud/beak-agent-channel-dingtalk => ../beak-agent-dingtalk

replace github.com/GuanceCloud/beak-agent-channel-lark => ../beak-agent-lark

replace github.com/GuanceCloud/beak-agent-channel-wechat => ../beak-agent-weixin

replace github.com/TrueWatchTech/truewatch-beak-agent-channel-slack => ../truewatch-beak-agent-channel-slack

replace github.com/TrueWatchTech/truewatch-beak-agent-channel-slack/conformance => ../truewatch-beak-agent-channel-slack/conformance

replace github.com/TrueWatchTech/truewatch-beak-agent-channel-teams => ../truewatch-beak-agent-channel-teams

replace github.com/TrueWatchTech/truewatch-beak-agent-channel-telegram => ../truewatch-beak-agent-channel-telegram

replace gitlab.jiagouyun.com/guance/beak-agent-channel-sdk/beak-channel-sdk-conformance => ../beak-channel-sdk-conformance
