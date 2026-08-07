package config

import (
    "encoding/hex"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "strings"
    "time"

    "proxy/internal/constants"
)

// CommonConfig 包含客户端和服务器共用的配置项。
// 注意：time.Duration 类型的字段在 JSON 中应使用字符串格式，如 "10s"。
type CommonConfig struct {
    DialTimeout     time.Duration `json:"dial_timeout"`
    KeepAlive       time.Duration `json:"keep_alive_interval"`
    UpPack          int           `json:"up_pack"`
    ChunkMs         int           `json:"chunk_ms"`
    Concur          int           `json:"concur"`
    MaxQueueBytes   int           `json:"max_queue_bytes"`
    ReadBuf         int           `json:"read_buf"`
    IPStrategy      byte
    UtlsFingerprint string        `json:"utls_fingerprint"`
    Fallback        bool          `json:"fallback"`
    DNSServer       string        `json:"dns_server"`
    ECHDomain       string        `json:"ech_domain"`
    Insecure        bool          `json:"insecure"`
    Token           string        `json:"token"`
    IPOverride      string        `json:"ip_override"`
}

type ClientConfig struct {
    CommonConfig
    UUID        string `json:"uuid"`
    ServerAddr  string `json:"server_addr"`
    LocalSocks5 string `json:"local_socks5"`
    LocalHTTP   string `json:"local_http"`
}

type ServerConfig struct {
    CommonConfig
    ListenAddr  string   `json:"listen_addr"`
    AuthUUIDs   []string `json:"auth_uuids"`
    AuthUUIDBin [][]byte `json:"-"`
    DnPack      int      `json:"dn_pack"`
    DnTail      int      `json:"dn_tail"`
    DnQr        int      `json:"dn_qr"`
}

var (
    ClientConf *ClientConfig
    ServerConf *ServerConfig
)

func LoadClientConfig(path string) error {
    f, err := os.Open(path)
    if err != nil {
        return err
    }
    defer f.Close()
    var c ClientConfig
    if err := json.NewDecoder(f).Decode(&c); err != nil {
        return err
    }
    setCommonDefaults(&c.CommonConfig)
    c.IPStrategy = parseStrategy(c.IPStrategy)
    ClientConf = &c
    return nil
}

func LoadServerConfig(path string) error {
    f, err := os.Open(path)
    if err != nil {
        return err
    }
    defer f.Close()
    var c ServerConfig
    if err := json.NewDecoder(f).Decode(&c); err != nil {
        return err
    }
    setCommonDefaults(&c.CommonConfig)
    c.IPStrategy = parseStrategy(c.IPStrategy)
    if c.DnPack <= 0 {
        c.DnPack = 32768
    }
    if c.DnTail <= 0 {
        c.DnTail = 512
    }
    if c.DnQr <= 0 {
        c.DnQr = 4
    }
    c.AuthUUIDBin = make([][]byte, 0, len(c.AuthUUIDs))
    for _, s := range c.AuthUUIDs {
        cleaned := strings.ReplaceAll(s, "-", "")
        if len(cleaned) != 32 {
            return fmt.Errorf("invalid UUID length: %s", s)
        }
        b, err := hex.DecodeString(cleaned)
        if err != nil {
            return fmt.Errorf("decode UUID %s: %w", s, err)
        }
        if len(b) != constants.UUIDLength {
            return fmt.Errorf("UUID %s length mismatch", s)
        }
        c.AuthUUIDBin = append(c.AuthUUIDBin, b)
    }
    if len(c.AuthUUIDBin) == 0 {
        log.Println("[警告] AuthUUIDs 为空，服务器将拒绝所有连接")
    }
    ServerConf = &c
    return nil
}

func setCommonDefaults(c *CommonConfig) {
    if c.DialTimeout == 0 {
        c.DialTimeout = 10 * time.Second
    }
    if c.KeepAlive == 0 {
        c.KeepAlive = 30 * time.Second
    }
    if c.UpPack == 0 {
        c.UpPack = 20 * 1024
    }
    if c.ChunkMs == 0 {
        c.ChunkMs = 20
    }
    if c.Concur == 0 {
        c.Concur = 2
    }
    if c.MaxQueueBytes == 0 {
        c.MaxQueueBytes = 128 * 1024
    }
    if c.ReadBuf == 0 {
        c.ReadBuf = 64 * 1024
    }
    if c.UtlsFingerprint == "" {
        c.UtlsFingerprint = "chrome_120"
    }
    if c.DNSServer == "" {
        c.DNSServer = "https://doh.pub/dns-query"
    }
    if c.ECHDomain == "" {
        c.ECHDomain = "cloudflare-ech.com"
    }
}

func parseStrategy(s string) byte {
    switch s {
    case "ipv4_only":
        return constants.IPStrategyIPv4Only
    case "ipv6_only":
        return constants.IPStrategyIPv6Only
    case "ipv4_prefer":
        return constants.IPStrategyIPv4Prefer
    case "ipv6_prefer":
        return constants.IPStrategyIPv6Prefer
    default:
        return constants.IPStrategyIPv4Prefer
    }
}
