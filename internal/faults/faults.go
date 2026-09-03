// Package faults holds a broker's runtime fault-injection switches. The zero
// value is a no-op; the Track F chaos panel flips these over HTTP.
package faults

import (
	"sync"
	"time"
)

type Config struct {
	mu                sync.RWMutex
	slowFollowerDelay time.Duration
	dropPings         bool
	paused            bool
}

func New() *Config { return &Config{} }

// SlowFollowerDelay is an artificial pause added before every replica fetch on
// this broker. A value longer than replica.lag.time.max.ms drops the broker
// out of every ISR it is a follower in.
func (c *Config) SlowFollowerDelay() time.Duration {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.slowFollowerDelay
}

func (c *Config) SetSlowFollowerDelay(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.slowFollowerDelay = d
}

// DropPings makes this broker stop answering peer liveness pings, so the rest
// of the cluster treats it as down (and, if it was the controller, elects a
// new one).
func (c *Config) DropPings() bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dropPings
}

func (c *Config) SetDropPings(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropPings = v
}

// Paused halts this broker's replica fetch loop entirely.
func (c *Config) Paused() bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.paused
}

func (c *Config) SetPaused(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = v
}

type Snapshot struct {
	SlowFollowerDelayMS int64 `json:"slow_follower_delay_ms"`
	DropPings           bool  `json:"drop_pings"`
	Paused              bool  `json:"paused"`
}

func (c *Config) Snapshot() Snapshot {
	if c == nil {
		return Snapshot{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return Snapshot{
		SlowFollowerDelayMS: c.slowFollowerDelay.Milliseconds(),
		DropPings:           c.dropPings,
		Paused:              c.paused,
	}
}
