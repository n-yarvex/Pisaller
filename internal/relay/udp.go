package relay

import (
    "fmt"
    "net"
    "strconv"
    "time"

    "proxy/internal/constants"
)

type UDPRelayer interface {
    Read(b []byte) (int, net.Addr, error)
    WriteTo(b []byte, addr net.Addr) (int, error)
    Close() error
    SetReadDeadline(t time.Time) error
    LocalAddr() net.Addr
}

type DirectUDPRelayer struct {
    conn *net.UDPConn
}

func NewDirectUDPRelayer() (*DirectUDPRelayer, error) {
    udpConn, err := net.ListenUDP("udp", nil)
    if err != nil {
        return nil, err
    }
    return &DirectUDPRelayer{conn: udpConn}, nil
}

func (r *DirectUDPRelayer) Read(b []byte) (int, net.Addr, error) {
    return r.conn.ReadFromUDP(b)
}

func (r *DirectUDPRelayer) WriteTo(b []byte, addr net.Addr) (int, error) {
    return r.conn.WriteToUDP(b, addr.(*net.UDPAddr))
}

func (r *DirectUDPRelayer) Close() error {
    return r.conn.Close()
}

func (r *DirectUDPRelayer) SetReadDeadline(t time.Time) error {
    return r.conn.SetReadDeadline(t)
}

func (r *DirectUDPRelayer) LocalAddr() net.Addr {
    return r.conn.LocalAddr()
}

func resolveUDPAddr(addr string, strategy byte, timeout time.Duration) (*net.UDPAddr, error) {
    host, portStr, err := net.SplitHostPort(addr)
    if err != nil {
        host = addr
        portStr = "0"
    }
    port, err := strconv.Atoi(portStr)
    if err != nil || port < 0 || port > 65535 {
        return nil, fmt.Errorf("invalid port: %s", portStr)
    }
    if ip := net.ParseIP(host); ip != nil {
        switch strategy {
        case constants.IPStrategyIPv4Only:
            if ip.To4() == nil {
                return nil, fmt.Errorf("IPv6 not allowed by strategy")
            }
        case constants.IPStrategyIPv6Only:
            if ip.To4() != nil {
                return nil, fmt.Errorf("IPv4 not allowed by strategy")
            }
        }
        return &net.UDPAddr{IP: ip, Port: port}, nil
    }
    ips := resolveIPsForDial(host, strategy, timeout)
    if len(ips) == 0 {
        return nil, fmt.Errorf("no IP resolved for %s", host)
    }
    return &net.UDPAddr{IP: ips[0], Port: port}, nil
}
