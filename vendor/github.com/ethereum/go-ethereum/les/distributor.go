// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package light implements on-demand retrieval capable state and chain objects
// for the Ethereum Light Client.
package les

import (
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
)

// ErrNoPeers is returned if no peers capable of serving a queued request are
// available for a certain amount of time (noPeersTimeout)
var (
	ErrNoPeers     = errors.New("no suitable peers available")
	noPeersTimeout = time.Second * 5
)

type distReq struct {
	getCost func(*peer) uint64
	canSend func(*peer) bool
	request func(uint64, *peer) // should not block

	reqID, reqOrder uint64
	// only for queued requests
	queueTime            mclock.AbsTime
	queuePrev, queueNext *distReq
	sentChn              chan *peer
}

type requestDistributor struct {
	queueFirst, queueLast *distReq
	lastReqOrder          uint64
	loopChn, stopChn      chan struct{}
	loopRunning           bool

	getAllPeers func() map[*peer]struct{}

	lock sync.Mutex
}

func newRequestDistributor(getAllPeers func() map[*peer]struct{}, stopChn chan struct{}) *requestDistributor {
	r := &requestDistributor{
		loopChn:     make(chan struct{}, 1),
		stopChn:     stopChn,
		getAllPeers: getAllPeers,
	}
	go r.loop()
	return r
}

func newDistReq(getCost func(*peer) uint64, canSend func(*peer) bool, request func(uint64, *peer)) *distReq {
	return &distReq{
		getCost: getCost,
		canSend: canSend,
		request: request,
		reqID:   getNextReqID(),
		//sentTo:  make(map[*peer]struct{}),
	}
}

const distMaxWait = time.Millisecond * 10

func (d *requestDistributor) loop() {
	for {
		select {
		case <-d.stopChn:
			d.lock.Lock()
			req := d.queueFirst
			for req != nil {
				close(req.sentChn)
				req = req.queueNext
			}
			d.lock.Unlock()
			return
		case <-d.loopChn:
			d.lock.Lock()
			d.loopRunning = false
		loop:
			for {
				peer, req, wait := d.nextRequest()
				if req != nil && wait == 0 {
					chn := req.sentChn
					d.remove(req)
					req.request(req.reqID, peer)
					chn <- peer
					close(chn)
				} else {
					if wait == 0 {
						if d.queueFirst == nil {
							break loop
						}
						// retry but do not set loopRunning, allow new requests to wake up the loop earlier
						wait = retryPeers
					} else {
						d.loopRunning = true
						if wait > distMaxWait {
							wait = distMaxWait
						}
					}
					go func() {
						time.Sleep(wait)
						d.loopChn <- struct{}{}
					}()
					break loop
				}
			}
			d.lock.Unlock()
		}
	}
}

type selectPeerItem struct {
	peer   *peer
	req    *distReq
	weight int64
}

func (sp selectPeerItem) Weight() int64 {
	return sp.weight
}

func (d *requestDistributor) nextRequest() (*peer, *distReq, time.Duration) {
	peers := d.getAllPeers()

	/*fmt.Println("len(peers) =", len(peers))
	rq := d.queueFirst
	fmt.Print("queue")
	for rq != nil {
		fmt.Print(" ", rq.reqOrder)
		rq = rq.queueNext
	}
	fmt.Println()*/

	req := d.queueFirst
	var (
		bestPeer *peer
		bestReq  *distReq
		bestWait time.Duration
		sel      *weightedRandomSelect
	)

	now := mclock.Now()

	for (len(peers) > 0 || req == d.queueFirst) && req != nil {
		//fmt.Println("req", req.reqOrder, "len", len(peers))
		canSend := false
		for peer, _ := range peers {
			if req.canSend(peer) {
				canSend = true
				cost := req.getCost(peer)
				wait, bufRemain := peer.fcServer.CanSend(cost)
				//fmt.Println("cs", wait, bufRemain)
				if wait == 0 {
					if sel == nil {
						sel = newWeightedRandomSelect()
					}
					sel.update(selectPeerItem{peer: peer, req: req, weight: int64(bufRemain*1000000) + 1})
				} else {
					if bestReq == nil || wait < bestWait {
						bestPeer = peer
						bestReq = req
						bestWait = wait
					}
				}
				delete(peers, peer)
			}
		}
		next := req.queueNext
		if req == d.queueFirst && !canSend && time.Duration(now-req.queueTime) > noPeersTimeout {
			//fmt.Println("remove", req.reqOrder)
			close(req.sentChn)
			d.remove(req)
		}
		req = next
	}

	if sel != nil {
		c := sel.choose().(selectPeerItem)
		//fmt.Println("choose", c.req.reqOrder)
		return c.peer, c.req, 0
	}
	if bestReq == nil {
		//fmt.Println("best nil")
	} else {
		//fmt.Println("best", bestReq.reqOrder)
	}
	return bestPeer, bestReq, bestWait
}

func (d *requestDistributor) queue(r *distReq) chan *peer {
	d.lock.Lock()
	defer d.lock.Unlock()

	if r.reqOrder == 0 {
		// queueing for the first time
		d.lastReqOrder++
		r.reqOrder = d.lastReqOrder
	}

	if d.queueLast == nil {
		d.queueFirst = r
		d.queueLast = r
	} else {
		if r.reqOrder > d.queueLast.reqOrder {
			d.queueLast.queueNext = r
			r.queuePrev = d.queueLast
			d.queueLast = r
		} else {
			// it's a resend, find its place (probably close to the front)
			before := d.queueFirst
			for before.reqOrder < r.reqOrder {
				before = before.queueNext
			}
			r.queueNext = before
			r.queuePrev = before.queuePrev
			r.queueNext.queuePrev = r
			if r.queuePrev == nil {
				d.queueFirst = r
			} else {
				r.queuePrev.queueNext = r
			}
		}
	}

	if !d.loopRunning {
		d.loopRunning = true
		d.loopChn <- struct{}{}
	}

	r.queueTime = mclock.Now()

	r.sentChn = make(chan *peer, 1)
	return r.sentChn
}

// it is guaranteed that the callback functions will not be called after
// cancel returns
func (d *requestDistributor) cancel(r *distReq) bool {
	d.lock.Lock()
	defer d.lock.Unlock()

	if r.sentChn == nil {
		return false
	}

	close(r.sentChn)
	d.remove(r)
	return true
}

func (d *requestDistributor) remove(r *distReq) {
	r.sentChn = nil
	if r.queueNext == nil {
		d.queueLast = r.queuePrev
	} else {
		r.queueNext.queuePrev = r.queuePrev
	}
	if r.queuePrev == nil {
		d.queueFirst = r.queueNext
	} else {
		r.queuePrev.queueNext = r.queueNext
	}
	r.queueNext = nil
	r.queuePrev = nil
}
