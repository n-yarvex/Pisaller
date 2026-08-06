package pool

import "sync"

var BufPool = sync.Pool{
    New: func() interface{} {
        b := make([]byte, 64*1024)
        return &b
    },
}
