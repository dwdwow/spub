package spub

import "context"

type ChannelUtil[D any] interface {
	Marshal(D) (string, error)
	Unmarshal(string) (D, error)
}

type Subscription[D any] interface {
	Chan() <-chan D
	Receive(context.Context) (d D, closed bool, err error)
	Subscribe(ctx context.Context, channels ...string) error
	Close(ctx context.Context) error
}

type ConsumerService[D any] interface {
	ChannelUtil() ChannelUtil[D]
	Subscribe(ctx context.Context, channels ...string) (Subscription[D], error)
}

type PublisherManager[D any] interface {
	Start(ctx context.Context) error
	Close(ctx context.Context) error
}

type ProducerService[D any] interface {
	WatchChannels() (Subscription[[]string], error)
	ConsumerChannels(ctx context.Context) ([]string, error)
	ChannelUtil() ChannelUtil[D]
	Publish(ctx context.Context, channel string, d D) error
}

// Publisher
// if a producer is a publisher, can not implement Publish
type Publisher[D any] interface {
	ConsumerService[D]
	ProducerService[D]
	PublisherManager[D]
}
