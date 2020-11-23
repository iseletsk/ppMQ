package ppmq

import "errors"

// ErrNoSuchMessage is returned when underlying storage engine cannot find message with given MessageKey
var ErrNoSuchMessage = errors.New("no such message")

// ErrQueueEmpty is returned when Topic has no messages / is empty
var ErrQueueEmpty = errors.New("queue is empty")
