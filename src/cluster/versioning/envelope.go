package versioning

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	EnvelopeVersion = 1
	MagicPrefix     = "NOSQLCLUSTER1:"
)

type Envelope struct {
	Version           int         `json:"version"`
	VectorClock       VectorClock `json:"vector_clock"`
	UpdatedAtUnixNano int64       `json:"updated_at_unix_nano"`
	Deleted           bool        `json:"deleted"`
	Value             string      `json:"value,omitempty"`
}

func NewPut(clock VectorClock, value string, now time.Time) Envelope {
	return Envelope{
		Version:           EnvelopeVersion,
		VectorClock:       clock.Clone(),
		UpdatedAtUnixNano: now.UnixNano(),
		Deleted:           false,
		Value:             value,
	}
}

func NewDelete(clock VectorClock, now time.Time) Envelope {
	return Envelope{
		Version:           EnvelopeVersion,
		VectorClock:       clock.Clone(),
		UpdatedAtUnixNano: now.UnixNano(),
		Deleted:           true,
	}
}

func Encode(e Envelope) (string, error) {
	e.normalize()
	if err := e.validate(); err != nil {
		return "", err
	}
	data, err := json.Marshal(e)
	if err != nil {
		return "", fmt.Errorf("encode envelope: %w", err)
	}
	return MagicPrefix + string(data), nil
}

func Decode(raw string) (Envelope, error) {
	if !IsEncoded(raw) {
		return Envelope{}, fmt.Errorf("decode envelope: missing magic prefix")
	}
	var e Envelope
	if err := json.Unmarshal([]byte(strings.TrimPrefix(raw, MagicPrefix)), &e); err != nil {
		return Envelope{}, fmt.Errorf("decode envelope json: %w", err)
	}
	e.normalize()
	if err := e.validate(); err != nil {
		return Envelope{}, fmt.Errorf("decode envelope: %w", err)
	}
	return e, nil
}

func IsEncoded(raw string) bool {
	return strings.HasPrefix(raw, MagicPrefix)
}

func (e *Envelope) normalize() {
	if e.VectorClock == nil {
		e.VectorClock = VectorClock{}
	} else {
		e.VectorClock = e.VectorClock.Clone()
	}
	if e.Deleted {
		e.Value = ""
	}
}

func (e Envelope) validate() error {
	if e.Version != EnvelopeVersion {
		return fmt.Errorf("unsupported envelope version %d", e.Version)
	}
	if e.UpdatedAtUnixNano == 0 {
		return fmt.Errorf("updated_at_unix_nano must be set")
	}
	return nil
}
