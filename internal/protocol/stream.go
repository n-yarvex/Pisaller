package protocol

import (
    "encoding/binary"
    "io"
    "net"

    "github.com/xtaci/smux"
)

func ReadSmuxOpenHeader(stream *smux.Stream) (kind byte, strategy byte, target string, err error) {
    header := make([]byte, 4)
    if _, err = io.ReadFull(stream, header); err != nil {
        return
    }
    kind = header[0]
    strategy = header[1]
    tlen := int(binary.BigEndian.Uint16(header[2:4]))
    if tlen == 0 {
        target = ""
        return
    }
    buf := make([]byte, tlen)
    if _, err = io.ReadFull(stream, buf); err != nil {
        return
    }
    target = string(buf)
    return
}

func ReadChunk(stream *smux.Stream) ([]byte, error) {
    var size uint16
    if err := binary.Read(stream, binary.BigEndian, &size); err != nil {
        return nil, err
    }
    if size == 0 {
        return []byte{}, nil
    }
    data := make([]byte, size)
    if _, err := io.ReadFull(stream, data); err != nil {
        return nil, err
    }
    return data, nil
}

func WriteUDPReply(stream *smux.Stream, addr string, data []byte) error {
    addrBytes := []byte(addr)
    if len(addrBytes) > 65535 {
        addrBytes = addrBytes[:65535]
    }
    buf := make([]byte, 2+len(addrBytes)+len(data))
    binary.BigEndian.PutUint16(buf[0:2], uint16(len(addrBytes)))
    copy(buf[2:], addrBytes)
    copy(buf[2+len(addrBytes):], data)
    _, err := stream.Write(buf)
    return err
}

func WriteUDPChunk(stream *smux.Stream, addr string, data []byte) error {
    addrBytes := []byte(addr)
    if len(addrBytes) > 65535 {
        addrBytes = addrBytes[:65535]
    }
    buf := make([]byte, 2+2+len(addrBytes)+len(data))
    binary.BigEndian.PutUint16(buf[0:2], uint16(len(addrBytes)))
    copy(buf[2:], addrBytes)
    binary.BigEndian.PutUint16(buf[2+len(addrBytes):], uint16(len(data)))
    copy(buf[2+len(addrBytes)+2:], data)
    _, err := stream.Write(buf)
    return err
}

func ReadUDPChunk(stream *smux.Stream) (addr string, data []byte, err error) {
    var addrLen uint16
    if err = binary.Read(stream, binary.BigEndian, &addrLen); err != nil {
        return
    }
    addrBytes := make([]byte, addrLen)
    if _, err = io.ReadFull(stream, addrBytes); err != nil {
        return
    }
    addr = string(addrBytes)
    var dataLen uint16
    if err = binary.Read(stream, binary.BigEndian, &dataLen); err != nil {
        return
    }
    data = make([]byte, dataLen)
    if _, err = io.ReadFull(stream, data); err != nil {
        return
    }
    return
}
