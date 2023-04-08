package encoder

import "github.com/hdt3213/rdb/core"

// Encoder is used to generate RDB file
type Encoder = core.Encoder

// NewEncoder creates an encoder instance
var NewEncoder = core.NewEncoder

// WithTTL specific expiration timestamp for object
var WithTTL = core.WithTTL
