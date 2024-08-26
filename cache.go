package main

import (
	"context"
	"crypto"
	"fmt"
	"github.com/jmoiron/sqlx"
	"reflect"
	"sync"
	"time"
)

type (
	Cache interface {
		Start(ctx context.Context)
		DoContext(
			ctx context.Context,
			query func(ctx context.Context, args ...interface{}) (interface{}, error),
			args ...interface{}) (interface{}, error)
		Do(query func(args ...interface{}) (interface{}, error), args ...interface{}) (interface{}, error)
		hash(objs ...interface{}) (string, error)
		getOutdatedCache() []string
		flush(keys []string)
	}

	cache struct {
		db  *sqlx.DB
		ttl time.Duration

		mu sync.RWMutex

		data map[string]*cacheEntity
	}

	cacheEntity struct {
		lifetime int64
		value    interface{}
	}
)

func NewCache(ctx context.Context, db *sqlx.DB, ttl time.Duration) Cache {
	c := &cache{
		db:   db,
		ttl:  ttl,
		data: make(map[string]*cacheEntity),
	}
	c.Start(ctx)

	return c
}

func (c *cache) Start(ctx context.Context) {
	tt := time.NewTicker(c.ttl)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-tt.C:
				keys := c.getOutdatedCache()
				c.flush(keys)
			}
		}
	}()
}

func (c *cache) DoContext(ctx context.Context, query func(ctx context.Context, args ...interface{}) (interface{}, error), args ...interface{}) (interface{}, error) {
	h, err := c.hash(args)
	if err != nil {
		return nil, err
	}

	v, ok := c.data[h]
	if !ok {
		var nv interface{}
		if nv, err = query(ctx, args); err != nil {
			return nil, err
		}

		c.data[h] = &cacheEntity{
			lifetime: time.Now().Add(c.ttl).Unix(),
			value:    nv,
		}

		return nv, nil
	}

	return v.value, nil
}

func (c *cache) Do(query func(args ...interface{}) (interface{}, error), args ...interface{}) (interface{}, error) {
	h, err := c.hash(args)
	if err != nil {
		return nil, err
	}

	v, ok := c.data[h]
	if !ok {
		var nv interface{}
		if nv, err = query(args); err != nil {
			return nil, err
		}

		c.data[h] = &cacheEntity{
			lifetime: time.Now().Add(c.ttl).Unix(),
			value:    nv,
		}

		return nv, nil
	}

	return v.value, nil
}

func (c *cache) hash(objs ...interface{}) (string, error) {
	var (
		digester = crypto.MD5.New()
		err      error
	)
	for _, ob := range objs {
		if _, err = fmt.Fprint(digester, reflect.TypeOf(ob)); err != nil {
			return "", err
		}
		if _, err = fmt.Fprint(digester, ob); err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("%x\n", digester.Sum(nil)), nil
}

func (c *cache) getOutdatedCache() []string {
	defer c.mu.Unlock()
	c.mu.Lock()

	keys := make([]string, 0)
	for k, v := range c.data {
		if v.lifetime < time.Now().Unix() {
			keys = append(keys, k)
		}
	}

	return keys
}

func (c *cache) flush(keys []string) {
	defer c.mu.Unlock()
	c.mu.Lock()

	for _, key := range keys {
		delete(c.data, key)
	}
}
