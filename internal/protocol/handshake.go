package protocol

import (
    "io"
    "net"
    "time"

    "proxy/internal/constants"
)

func WriteHandshakeRequest(conn net.Conn, uuid []byte, timeout time.Duration) error {
    conn.SetWriteDeadline(time.Now().Add(timeout))
    defer conn.SetWriteDeadline(time.Time{})
    buf := make([]byte, 1+len(uuid))
    buf[0] = constants.HandshakeReq
    copy(buf[1:], uuid)
    _, err := conn.Write(buf)
    return err
}

func ReadHandshakeRequest(conn net.Conn, timeout time.Duration) ([]byte, error) {
    conn.SetReadDeadline(time.Now().Add(timeout))
    defer conn.SetReadDeadline(time.Time{})
    var typ [1]byte
    if _, err := io.ReadFull(conn, typ[:]); err != nil {
        return nil, err
    }
    if typ[0] != constants.HandshakeReq {
        return nil, io.ErrUnexpectedEOF
    }
    uuid := make([]byte, constants.UUIDLength)
    if _, err := io.ReadFull(conn, uuid); err != nil {
        return nil, err
    }
    return uuid, nil
}

func WriteHandshakeResponse(conn net.Conn, code byte, timeout time.Duration) error {
    conn.SetWriteDeadline(time.Now().Add(timeout))
    defer conn.SetWriteDeadline(time.Time{})
    buf := []byte{constants.HandshakeResp, code}
    _, err := conn.Write(buf)
    return err
}

func ReadHandshakeResponse(conn net.Conn, timeout time.Duration) (byte, error) {
    conn.SetReadDeadline(time.Now().Add(timeout))
    defer conn.SetReadDeadline(time.Time{})
    var resp [2]byte
    if _, err := io.ReadFull(conn, resp[:]); err != nil {
        return 0, err
    }
    if resp[0] != constants.HandshakeResp {
        return 0, io.ErrUnexpectedEOF
    }
    return resp[1], nil
}
