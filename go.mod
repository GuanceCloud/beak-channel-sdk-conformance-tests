module gitlab.jiagouyun.com/guance/beak-agent-channel-sdk/beak-channel-sdk-conformance-tests

go 1.22

require (
	github.com/GuanceCloud/beak-agent-channel-dingtalk v0.0.0
	github.com/GuanceCloud/beak-agent-channel-lark v0.0.0
	github.com/GuanceCloud/beak-agent-channel-wechat v0.0.0
	github.com/larksuite/oapi-sdk-go/v3 v3.5.3
	github.com/open-dingtalk/dingtalk-stream-sdk-go v0.9.1
	gitlab.jiagouyun.com/guance/beak-agent-channel-sdk/beak-channel-sdk-conformance v0.0.0
)

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
)

replace github.com/GuanceCloud/beak-agent-channel-dingtalk => ../beak-agent-dingtalk

replace github.com/GuanceCloud/beak-agent-channel-lark => ../beak-agent-lark

replace github.com/GuanceCloud/beak-agent-channel-wechat => ../beak-agent-weixin

replace gitlab.jiagouyun.com/guance/beak-agent-channel-sdk/beak-channel-sdk-conformance => ../beak-channel-sdk-conformance
