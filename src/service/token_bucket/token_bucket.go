package token_bucket

import (
	"fmt"
	"nosqlEngine/src/config"
	"sync"
	"time"
)

var CONFIG = config.GetConfig()

type TokenBucket struct {
	mu             sync.Mutex
	currTokens     int
	lastRefillTime int64
}

func GetNewTokenBucket() *TokenBucket {
	return &TokenBucket{currTokens: CONFIG.MaxTokens, lastRefillTime: time.Now().Unix()}
}

func (tb *TokenBucket) CheckTokens() (bool, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now().Unix()
	elapsed := float64(now - tb.lastRefillTime)
	refilled := int(elapsed * CONFIG.TokenRefillRate)
	tokens := min(tb.currTokens+refilled, CONFIG.MaxTokens)

	if tokens < 1 {
		return false, fmt.Errorf("insufficient tokens")
	}
	tb.currTokens = tokens - 1
	tb.lastRefillTime = now
	return true, nil
}

