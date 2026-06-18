package user_limiter

import (
	"nosqlEngine/src/config"
	"nosqlEngine/src/service/token_bucket"
	"sync"
)

var CONFIG = config.GetConfig()

type UserLimiter struct {
	mu   sync.Mutex
	data map[string]*token_bucket.TokenBucket
}

func NewUserLimiter() *UserLimiter {
	return &UserLimiter{
		data: make(map[string]*token_bucket.TokenBucket),
	}
}

func (ul *UserLimiter) CheckUserTokens(user string) (bool, error) {
	ul.mu.Lock()
	bucket, exists := ul.data[user]
	if !exists {
		bucket = token_bucket.GetNewTokenBucket()
		ul.data[user] = bucket
	}
	ul.mu.Unlock()

	return bucket.CheckTokens()
}