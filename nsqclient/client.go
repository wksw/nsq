package nsqclient

import (
	"crypto/tls"
	"errors"

	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/nsqadmin"
)

type Client struct {
	ci  *clusterinfo.ClusterInfo
	opt *Options
}

type Options struct {
	AdminOpt *nsqadmin.Options
	Lg       func(lvl lg.LogLevel, f string, args ...interface{})
}

func New(opt *Options) (*Client, error) {
	client := http_api.NewClient(&tls.Config{
		InsecureSkipVerify: opt.AdminOpt.HTTPClientTLSInsecureSkipVerify,
	},
		opt.AdminOpt.HTTPClientConnectTimeout,
		opt.AdminOpt.HTTPClientRequestTimeout)

	return &Client{
		ci:  clusterinfo.New(opt.Lg, client),
		opt: opt,
	}, nil
}

func (c *Client) CreateTopicChannel(topic, channel string) error {
	if !protocol.IsValidTopicName(topic) {
		return errors.New("invalid topic name")
	}
	if len(channel) > 0 && !protocol.IsValidChannelName(channel) {
		return errors.New("invalid channel name")
	}
	if err := c.ci.CreateTopicChannel(topic, channel, c.opt.AdminOpt.NSQLookupdHTTPAddresses); err != nil {
		return err
	}
	return nil
}

func (c *Client) GetLookupdTopics() ([]string, error) {
	return c.ci.GetLookupdTopics(c.opt.AdminOpt.NSQLookupdHTTPAddresses)
}
