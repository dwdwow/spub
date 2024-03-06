package spub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type simpleNewChInfo[D any] struct {
	channel string
	s       *SimpleSubscription[D]
}

type simpleSubsCfg struct {
	rcvCap int
}

type SimpleSubscription[D any] struct {
	// channels and closed should be locked while changing
	mux      sync.RWMutex
	channels map[string]bool
	// if closed, this subscription can not be used
	closed bool
	// receive data from publisher
	// can set cap by publisher
	rcv chan D
	// new channel chan, write only
	// create by publisher
	nc chan<- []simpleNewChInfo[D]
	// tell publisher while closing, write only
	// create by publisher
	done chan<- *SimpleSubscription[D]
}

func newSimpleSubscription[D any](cfg simpleSubsCfg, nc chan<- []simpleNewChInfo[D], done chan<- *SimpleSubscription[D]) *SimpleSubscription[D] {
	return &SimpleSubscription[D]{
		channels: make(map[string]bool),
		rcv:      make(chan D, cfg.rcvCap),
		nc:       nc,
		done:     done,
	}
}

func (s *SimpleSubscription[D]) Chan() <-chan D {
	return s.rcv
}

func (s *SimpleSubscription[D]) Receive(ctx context.Context) (d D, closed bool, err error) {
	var ok bool
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case d, ok = <-s.rcv:
		if ok {
			return
		}
		s.closed = true
		s.channels = nil
		closed = true
		err = errors.New("simple subscription: rcv closed")
	}
	return
}

func (s *SimpleSubscription[D]) Subscribe(ctx context.Context, channels ...string) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.closed {
		return errors.New("simple subscription: closed")
	}
	var cs []string
	for _, c := range channels {
		if !s.channels[c] {
			cs = append(cs, c)
		}
	}
	var infos []simpleNewChInfo[D]
	for _, c := range cs {
		infos = append(infos, simpleNewChInfo[D]{c, s})
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.nc <- infos:
	}
	return nil
}

// Close
// if receive error, close again
func (s *SimpleSubscription[D]) Close(ctx context.Context) error {
	return s.close(ctx, false)
}

func (s *SimpleSubscription[D]) close(ctx context.Context, byPublisher bool) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.closed = true
	s.channels = nil
	if byPublisher {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.done <- s:
	}
	return nil
}

type SimplePublisher[D any] struct {
	mux           sync.RWMutex
	subsCfg       simpleSubsCfg
	cUtil         ChannelUtil[D]
	chCreatorOnce sync.Once
	ssByC         map[string][]*SimpleSubscription[D]
	// producers
	prds []*SimpleSubscription[[]string]
	// consumer new channel infos
	nc chan []simpleNewChInfo[D]
	// consumer done
	csmDone chan *SimpleSubscription[D]
	// producer done
	prdDone chan *SimpleSubscription[[]string]
	closed  bool
}

type SimplePublisherOption[D any] func(*SimplePublisher[D])

func SimpleRcvCapOption[D any](cap int) SimplePublisherOption[D] {
	return func(p *SimplePublisher[D]) {
		p.subsCfg.rcvCap = cap
	}
}

func NewSimplePublisher[D any](channelKit ChannelUtil[D], options ...SimplePublisherOption[D]) *SimplePublisher[D] {
	p := &SimplePublisher[D]{
		cUtil:   channelKit,
		ssByC:   make(map[string][]*SimpleSubscription[D]),
		nc:      make(chan []simpleNewChInfo[D]),
		csmDone: make(chan *SimpleSubscription[D]),
		prdDone: make(chan *SimpleSubscription[[]string]),
		closed:  true,
	}
	for _, o := range options {
		o(p)
	}
	return p
}

func (p *SimplePublisher[D]) Subscribe(ctx context.Context, channels ...string) (Subscription[D], error) {
	p.mux.Lock()
	closed := p.closed
	p.mux.Unlock()
	if closed {
		return nil, errors.New("simple publisher: closed")
	}
	s := newSimpleSubscription(p.subsCfg, p.nc, p.csmDone)
	if len(channels) > 0 {
		err := s.Subscribe(ctx, channels...)
		if err != nil {
			return nil, err
		}
	}
	for _, prd := range p.prds {
		prd := prd
		go func() {
			defer func() {
				recover()
			}()
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
			case prd.rcv <- channels:
			}
		}()
	}
	return s, nil
}

//	func (p *SimplePublisher[D]) SetChannelCreator(creator func(D) (string, error)) {
//		p.chCreatorOnce.Parse(func() {
//			p.chCreator = creator
//		})
//	}
func (p *SimplePublisher[D]) ChannelUtil() ChannelUtil[D] {
	return p.cUtil
}

// Start
// if closed, can start again
func (p *SimplePublisher[D]) Start(ctx context.Context) error {
	p.mux.Lock()
	defer p.mux.Unlock()
	if p.nc == nil {
		return errors.New("simple publisher: nc is nil")
	}
	if p.csmDone == nil {
		return errors.New("simple publisher: subs done is nil")
	}
	p.closed = false
	go p.receive(ctx)
	return nil
}

func (p *SimplePublisher[D]) Close(ctx context.Context) error {
	p.mux.Lock()
	defer p.mux.Unlock()
	p.closed = true
	for _, ss := range p.ssByC {
		for _, s := range ss {
			// if close by publisher, no error
			_ = s.close(ctx, true)
			// close to tell subscription finished
			closeRcv(s.rcv)
		}
	}
	for _, prd := range p.prds {
		_ = prd.close(ctx, true)
		closeRcv(prd.rcv)
	}
	p.ssByC = make(map[string][]*SimpleSubscription[D])
	return nil
}

func (p *SimplePublisher[D]) copyCsms(channel string) []*SimpleSubscription[D] {
	p.mux.Lock()
	defer p.mux.Unlock()
	// must copy
	var nss []*SimpleSubscription[D]
	for _, s := range p.ssByC[channel] {
		nss = append(nss, s)
	}
	return nss
}

func (p *SimplePublisher[D]) Publish(ctx context.Context, channel string, d D) error {
	p.mux.Lock()
	closed := p.closed
	p.mux.Unlock()
	if closed {
		return errors.New("simple publisher: closed")
	}
	ss := p.copyCsms(channel)
	if len(ss) == 0 {
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(len(ss))
	counter := 0
	cmux := sync.Mutex{}
	add := func() {
		cmux.Lock()
		counter++
		cmux.Unlock()
	}
	for _, s := range ss {
		s := s
		go func() {
			defer func() {
				// must recover
				// because sub rcv may be closed
				recover()
				wg.Done()
			}()
			select {
			case <-ctx.Done():
				add()
			case s.rcv <- d:
			}
		}()
	}
	wg.Wait()
	if counter > 0 {
		return fmt.Errorf("simple publisher: %v subscription have not received data", counter)
	}
	return nil
}

func (p *SimplePublisher[D]) WatchChannels() (Subscription[[]string], error) {
	p.mux.Lock()
	defer p.mux.Unlock()
	if p.closed {
		return nil, errors.New("simple publisher: closed")
	}
	prd := newSimpleSubscription(p.subsCfg, nil, p.prdDone)
	p.prds = append(p.prds, prd)
	return prd, nil
}

func (p *SimplePublisher[D]) ConsumerChannels(ctx context.Context) ([]string, error) {
	p.mux.Lock()
	defer p.mux.Unlock()
	var cs []string
	for c := range p.ssByC {
		cs = append(cs, c)
	}
	return cs, nil
}

func (p *SimplePublisher[D]) receive(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case infos := <-p.nc:
			p.newCsmHandler(infos)
		case s := <-p.csmDone:
			p.csmDoneHandler(s)
		case s := <-p.prdDone:
			p.prdDoneHandler(s)
		}
	}
}

func (p *SimplePublisher[D]) newCsmHandler(infos []simpleNewChInfo[D]) {
	p.mux.Lock()
	defer p.mux.Unlock()
	for _, info := range infos {
		p.ssByC[info.channel] = append(p.ssByC[info.channel], info.s)
	}
}

func (p *SimplePublisher[D]) csmDoneHandler(s *SimpleSubscription[D]) {
	p.mux.Lock()
	defer p.mux.Unlock()
	for c, ss := range p.ssByC {
		for i, v := range ss {
			if v != s {
				continue
			}
			closeRcv(s.rcv)
			li := len(ss) - 1
			ss[i] = ss[li]
			p.ssByC[c] = ss[:li]
			return
		}
	}
}

func (p *SimplePublisher[D]) prdDoneHandler(s *SimpleSubscription[[]string]) {
	p.mux.Lock()
	defer p.mux.Unlock()
	prds := p.prds
	for i, prd := range prds {
		if s != prd {
			continue
		}
		closeRcv(prd.rcv)
		li := len(prds) - 1
		prds[i] = prds[li]
		p.prds = prds[:li]
		return
	}
}

func closeRcv[D any](rcv chan D) {
	defer func() {
		recover()
	}()
	close(rcv)
}
