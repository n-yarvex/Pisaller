package relay

import (
    "context"
    "fmt"
    "log"
    "net"
    "sync"
    "time"

    "proxy/internal/constants"
)

func DialTCPWithRace(addr string, strategy byte, concur int, timeout time.Duration) (net.Conn, error) {
    if concur <= 0 {
        return nil, fmt.Errorf("concur must be > 0")
    }
    if timeout <= 0 {
        return nil, fmt.Errorf("timeout must be > 0")
    }
    if concur <= 1 {
        return DialTCPWithStrategy(addr, strategy, timeout)
    }

    host, port, err := net.SplitHostPort(addr)
    if err != nil {
        return nil, err
    }

    ips := resolveIPsForDial(host, strategy, timeout)
    if len(ips) == 0 {
        return nil, fmt.Errorf("no IP addresses resolved for %s", host)
    }

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    resultCh := make(chan net.Conn, concur)
    var wg sync.WaitGroup

    for i := 0; i < concur; i++ {
        ip := ips[i%len(ips)]
        wg.Add(1)
        go func(ip net.IP) {
            defer wg.Done()
            target := net.JoinHostPort(ip.String(), port)
            dialer := net.Dialer{Timeout: timeout}
            conn, err := dialer.DialContext(ctx, "tcp", target)
            if err != nil {
                log.Printf("dial %s error: %v", target, err)
                return
            }
            select {
            case resultCh <- conn:
            case <-ctx.Done():
                conn.Close()
            }
        }(ip)
    }

    go func() {
        wg.Wait()
        close(resultCh)
    }()

    var winner net.Conn
    for conn := range resultCh {
        if winner == nil {
            winner = conn
            cancel()
        } else {
            conn.Close()
        }
    }

    if winner != nil {
        return winner, nil
    }
    return nil, fmt.Errorf("all connection attempts failed")
}

func resolveIPsForDial(host string, strategy byte, timeout time.Duration) []net.IP {
    if ip := net.ParseIP(host); ip != nil {
        switch strategy {
        case constants.IPStrategyIPv4Only:
            if ip.To4() == nil {
                return nil
            }
        case constants.IPStrategyIPv6Only:
            if ip.To4() != nil {
                return nil
            }
        }
        return []net.IP{ip}
    }

    var ips []net.IP
    resolver := &net.Resolver{}
    ctx, cancel := context.WithTimeout(context.Background(), timeout)
    defer cancel()

    addrs, err := resolver.LookupIPAddr(ctx, host)
    if err != nil {
        return nil
    }

    for _, addr := range addrs {
        ips = append(ips, addr.IP)
    }

    switch strategy {
    case constants.IPStrategyIPv4Only:
        return filterIPv4(ips)
    case constants.IPStrategyIPv6Only:
        return filterIPv6(ips)
    case constants.IPStrategyIPv4Prefer:
        v4 := filterIPv4(ips)
        v6 := filterIPv6(ips)
        if len(v4) > 0 {
            return append(v4, v6...)
        }
        return v6
    case constants.IPStrategyIPv6Prefer:
        v4 := filterIPv4(ips)
        v6 := filterIPv6(ips)
        if len(v6) > 0 {
            return append(v6, v4...)
        }
        return v4
    default:
        return ips
    }
}

func filterIPv4(ips []net.IP) []net.IP {
    var result []net.IP
    for _, ip := range ips {
        if ip.To4() != nil {
            result = append(result, ip)
        }
    }
    return result
}

func filterIPv6(ips []net.IP) []net.IP {
    var result []net.IP
    for _, ip := range ips {
        if ip.To4() == nil {
            result = append(result, ip)
        }
    }
    return result
}

func DialTCPWithStrategy(addr string, strategy byte, timeout time.Duration) (net.Conn, error) {
    host, port, err := net.SplitHostPort(addr)
    if err != nil {
        return net.DialTimeout("tcp", addr, timeout)
    }

    if ip := net.ParseIP(host); ip != nil {
        return net.DialTimeout("tcp", addr, timeout)
    }

    switch strategy {
    case constants.IPStrategyIPv4Only:
        return net.DialTimeout("tcp4", addr, timeout)
    case constants.IPStrategyIPv6Only:
        return net.DialTimeout("tcp6", addr, timeout)
    }

    if strategy == constants.IPStrategyIPv4Prefer || strategy == constants.IPStrategyIPv6Prefer {
        ips := resolveIPsForDial(host, strategy, timeout)
        for _, ip := range ips {
            target := net.JoinHostPort(ip.String(), port)
            conn, err := net.DialTimeout("tcp", target, timeout)
            if err == nil {
                return conn, nil
            }
            log.Printf("dial %s error: %v", target, err)
        }
        return nil, fmt.Errorf("no reachable IP for %s", host)
    }

    return net.DialTimeout("tcp", addr, timeout)
}
