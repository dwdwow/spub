package spub

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisSubsCfg struct {
	rcvCap int
}

type RedisSubscription[D any] struct {
	mux       sync.Mutex
	s         *redis.PubSub
	unmarshal func(string) (D, error)
	rcv       chan D
	err       chan error
	done      chan any
	closed    bool
}

func newRedisSubscription[D any](cfg redisSubsCfg, s *redis.PubSub, unmarshal func(string) (D, error)) *RedisSubscription[D] {
	return &RedisSubscription[D]{
		s:         s,
		unmarshal: unmarshal,
		rcv:       make(chan D, cfg.rcvCap),
		err:       make(chan error),
		done:      make(chan any, 1),
	}
}

func (s *RedisSubscription[D]) Chan() <-chan D {
	return s.rcv
}

func (s *RedisSubscription[D]) Receive(ctx context.Context) (d D, closed bool, err error) {
	s.mux.Lock()
	closed = s.closed
	s.mux.Unlock()
	if closed {
		return
	}
	var ok bool
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case d, ok = <-s.rcv:
		if ok {
			return
		}
		s.closed = true
		closed = true
		err = errors.New("simple subscription: rcv closed")
	case err = <-s.err:
		return
	}
	return
}

func (s *RedisSubscription[D]) Subscribe(ctx context.Context, channels ...string) error {
	s.mux.Lock()
	closed := s.closed
	s.mux.Unlock()
	if closed {
		return errors.New("redis subscription: closed")
	}
	return s.s.Subscribe(ctx, channels...)
}

func (s *RedisSubscription[D]) Close(ctx context.Context) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.closed = true
	closeRcv(s.rcv)
	select {
	case <-ctx.Done():
	case s.done <- 1:
	}
	return s.s.Close()
}

func (s *RedisSubscription[D]) start() {
	c := s.s.Channel()
	for {
		select {
		case <-s.done:
			return
		case msg, ok := <-c:
			if !ok {
				closeRcv(s.rcv)
				return
			}
			payload := msg.Payload
			d, err := s.unmarshal(payload)
			if err != nil {
				s.err <- err
			} else {
				s.rcv <- d
			}
		}
	}
}

type ChannelWatcher struct {
	mux     sync.Mutex
	rcv     chan []string
	doneToP chan<- *ChannelWatcher
	closed  bool
}

func newChannelWatcher(doneToP chan<- *ChannelWatcher) *ChannelWatcher {
	return &ChannelWatcher{
		rcv:     make(chan []string, 5),
		doneToP: doneToP,
	}
}

func (w *ChannelWatcher) Chan() <-chan []string {
	return w.rcv
}

func (w *ChannelWatcher) Receive(ctx context.Context) (d []string, closed bool, err error) {
	w.mux.Lock()
	closed = w.closed
	w.mux.Unlock()
	if closed {
		return
	}
	var ok bool
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case d, ok = <-w.rcv:
		if ok {
			return
		}
		w.closed = true
		closed = true
		err = errors.New("simple subscription: rcv closed")
	}
	return
}

func (w *ChannelWatcher) Subscribe(context.Context, ...string) error {
	return nil
}

func (w *ChannelWatcher) Close(ctx context.Context) error {
	w.mux.Lock()
	defer w.mux.Unlock()
	w.closed = true
	closeRcv(w.rcv)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.doneToP <- w:
	}
	return nil
}

type RedisPublisher[D any] struct {
	ropts         *redis.Options
	clt           *redis.Client
	scfg          redisSubsCfg
	cUtil         ChannelUtil[D]
	rMsgUnmarshal func(string) (D, error)
	// channel watchers mutex
	cwMux            sync.Mutex
	channelsWatchers []*ChannelWatcher
	watcherDone      chan *ChannelWatcher
	ctx              context.Context
	cancel           context.CancelFunc
	logger           *slog.Logger
}

type RedisPublisherOption[D any] func(*RedisPublisher[D])

func RedisRcvCapOption[D any](cap int) RedisPublisherOption[D] {
	return func(p *RedisPublisher[D]) {
		p.scfg.rcvCap = cap
	}
}

func NewRedisPublisher[D any](rOptions *redis.Options, channelKit ChannelUtil[D], redisMsgUnmarshal func(string) (D, error), logger *slog.Logger, optios ...RedisPublisherOption[D]) *RedisPublisher[D] {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
	}
	logger = logger.With("publisher", "redis")
	ctx, cancel := context.WithCancel(context.TODO())
	p := &RedisPublisher[D]{
		ropts:         rOptions,
		clt:           redis.NewClient(rOptions),
		cUtil:         channelKit,
		rMsgUnmarshal: redisMsgUnmarshal,
		watcherDone:   make(chan *ChannelWatcher, 100),
		ctx:           ctx,
		cancel:        cancel,
		logger:        logger,
	}
	for _, o := range optios {
		o(p)
	}
	return p
}

func (p *RedisPublisher[D]) ChannelUtil() ChannelUtil[D] {
	return p.cUtil
}

func (p *RedisPublisher[D]) Subscribe(ctx context.Context, channels ...string) (Subscription[D], error) {
	s := p.clt.Subscribe(ctx, channels...)
	sub := newRedisSubscription(p.scfg, s, p.rMsgUnmarshal)
	go sub.start()
	return sub, nil
}

func (p *RedisPublisher[D]) Start(ctx context.Context) error {
	go p.waitWatcherDone(ctx)
	go p.watchChannels(ctx)
	return nil
}

func (p *RedisPublisher[D]) waitWatcherDone(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.ctx.Done():
			return
		case w := <-p.watcherDone:
			p.removeWatcher(w)
		}
	}
}

func (p *RedisPublisher[D]) watchChannels(ctx context.Context) {
	old := map[string]bool{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.ctx.Done():
			return
		case <-time.After(time.Second):
			p.broadcastChannels(ctx, old)
		}
	}
}

func (p *RedisPublisher[D]) broadcastChannels(ctx context.Context, old map[string]bool) {
	cs, err := p.ConsumerChannels(ctx)
	if err != nil {
		p.logger.Error("Can not get all channels")
		return
	}
	var ncs []string
	for _, c := range cs {
		if old[c] {
			continue
		}
		ncs = append(ncs, c)
		old[c] = true
	}
	p.cwMux.Lock()
	defer p.cwMux.Unlock()
	wg := sync.WaitGroup{}
	wg.Add(len(p.channelsWatchers))
	for _, s := range p.channelsWatchers {
		s := s
		go func() {
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
				p.logger.Error("Suber can not receive all channels within 1 second")
			case s.rcv <- ncs:
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return
}

func (p *RedisPublisher[D]) removeWatcher(watcher *ChannelWatcher) {
	p.cwMux.Lock()
	defer p.cwMux.Unlock()
	watchers := p.channelsWatchers
	for i, w := range watchers {
		if w != watcher {
			continue
		}
		li := len(watchers) - 1
		watchers[i] = watchers[li]
		p.channelsWatchers = watchers[:li]
		return
	}
}

func (p *RedisPublisher[D]) Close(context.Context) error {
	p.cancel()
	return p.clt.Close()
}

func (p *RedisPublisher[D]) WatchChannels() (Subscription[[]string], error) {
	s := newChannelWatcher(p.watcherDone)
	p.cwMux.Lock()
	defer p.cwMux.Unlock()
	p.channelsWatchers = append(p.channelsWatchers, s)
	return s, nil
}

func (p *RedisPublisher[D]) ConsumerChannels(ctx context.Context) ([]string, error) {
	cmds := p.clt.PubSubChannels(ctx, "*")
	return cmds.Result()
}

func (p *RedisPublisher[D]) Publish(ctx context.Context, channel string, d D) error {
	data, err := json.Marshal(d)
	if err != nil {
		return err
	}
	cmd := p.clt.Publish(ctx, channel, data)
	return cmd.Err()
}

type RedisConsumer[D any] struct {
	ropts         *redis.Options
	clt           *redis.Client
	scfg          redisSubsCfg
	cUtil         ChannelUtil[D]
	rMsgUnmarshal func(string) (D, error)
	logger        *slog.Logger
}

type RedisConsumerOption[D any] func(*RedisConsumer[D])

func RedisConsumerRcvCapOption[D any](cap int) RedisConsumerOption[D] {
	return func(p *RedisConsumer[D]) {
		p.scfg.rcvCap = cap
	}
}

func NewRedisConsumer[D any](rOptions *redis.Options, channelKit ChannelUtil[D], redisMsgUnmarshal func(string) (D, error), logger *slog.Logger, optios ...RedisConsumerOption[D]) ConsumerService[D] {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
	}
	p := &RedisConsumer[D]{
		ropts:         rOptions,
		clt:           redis.NewClient(rOptions),
		cUtil:         channelKit,
		rMsgUnmarshal: redisMsgUnmarshal,
		logger:        logger,
	}
	for _, o := range optios {
		o(p)
	}
	return p
}

func (p *RedisConsumer[D]) ChannelUtil() ChannelUtil[D] {
	return p.cUtil
}

func (p *RedisConsumer[D]) Subscribe(ctx context.Context, channels ...string) (Subscription[D], error) {
	s := p.clt.Subscribe(ctx, channels...)
	sub := newRedisSubscription(p.scfg, s, p.rMsgUnmarshal)
	go sub.start()
	return sub, nil
}
