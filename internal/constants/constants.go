package constants

const (
    IPStrategyIPv4Only   = 0x01
    IPStrategyIPv6Only   = 0x02
    IPStrategyIPv4Prefer = 0x03
    IPStrategyIPv6Prefer = 0x04

    StreamKindPing = 0x01
    StreamKindTCP  = 0x02
    StreamKindUDP  = 0x03

    HandshakeReq  = 0x10
    HandshakeResp = 0x11
    HandshakeOK   = 0x00
    HandshakeFail = 0xFF

    UUIDLength = 16
)
