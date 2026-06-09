module gitlab.jiagouyun.com/guance/beak-agent-channel-sdk/beak-channel-sdk-conformance-tests

go 1.22

require (
	github.com/GuanceCloud/beak-agent-channel-dingtalk v0.0.0
	github.com/GuanceCloud/beak-agent-channel-lark v0.0.0
	github.com/GuanceCloud/beak-agent-channel-wechat v0.0.0
	gitlab.jiagouyun.com/guance/beak-agent-channel-sdk/beak-channel-sdk-conformance v0.0.0
)

replace github.com/GuanceCloud/beak-agent-channel-dingtalk => ../beak-agent-dingtalk

replace github.com/GuanceCloud/beak-agent-channel-lark => ../beak-agent-lark

replace github.com/GuanceCloud/beak-agent-channel-wechat => ../beak-agent-weixin

replace gitlab.jiagouyun.com/guance/beak-agent-channel-sdk/beak-channel-sdk-conformance => ../beak-channel-sdk-conformance
