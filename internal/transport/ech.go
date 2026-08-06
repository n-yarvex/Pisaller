package transport

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const typeHTTPS = 65

var (
	echList     []byte
	echListMu   sync.RWMutex
	echFallback bool
	echDomain   string
	dnsServer   string
)

func InitECH(dns, domain string, fallback bool) error {
	echFallback = fallback
	echDomain = domain
	dnsServer = dns
	if fallback {
		log.Println("[ECH] Fallback 模式，禁用 ECH")
		return nil
	}
	return RefreshECH()
}

func RefreshECH() error {
	echListMu.Lock()
	defer echListMu.Unlock()
	log.Printf("[ECH] 刷新 ECH 配置: %s -> %s", dnsServer, echDomain)
	raw, err := queryECH(dnsServer, echDomain)
	if err != nil {
		return err
	}
	echList = raw
	log.Printf("[ECH] ECHConfigList 长度: %d 字节", len(raw))
	return nil
}

func GetECHList() ([]byte, error) {
	if echFallback {
		return nil, nil
	}
	echListMu.RLock()
	defer echListMu.RUnlock()
	if len(echList) == 0 {
		return nil, fmt.Errorf("ECH 配置未加载")
	}
	return echList, nil
}

func queryECH(dnsServer, domain string) ([]byte, error) {
	if strings.HasPrefix(dnsServer, "http://") || strings.HasPrefix(dnsServer, "https://") {
		return queryDoH(domain, dnsServer)
	}
	return queryDNSUDP(domain, dnsServer)
}

func queryDNSUDP(domain, dnsServer string) ([]byte, error) {
	if !strings.Contains(dnsServer, ":") {
		dnsServer = dnsServer + ":53"
	}
	query := buildDNSQuery(domain, typeHTTPS)
	conn, err := net.DialTimeout("udp", dnsServer, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err = conn.Write(query); err != nil {
		return nil, err
	}
	resp := make([]byte, 4096)
	n, err := conn.Read(resp)
	if err != nil {
		return nil, err
	}
	return parseDNSResponse(resp[:n])
}

func queryDoH(domain, dohURL string) ([]byte, error) {
	u, err := url.Parse(dohURL)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	dnsQuery := buildDNSQuery(domain, typeHTTPS)
	dnsBase64 := base64.RawURLEncoding.EncodeToString(dnsQuery)
	q.Set("dns", dnsBase64)
	u.RawQuery = q.Encode()
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/dns-message")
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("DoH 状态码: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return parseDNSResponse(body)
}

func buildDNSQuery(domain string, qtype uint16) []byte {
	query := make([]byte, 0, 512)
	query = append(query, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
	for _, label := range strings.Split(domain, ".") {
		query = append(query, byte(len(label)))
		query = append(query, []byte(label)...)
	}
	query = append(query, 0x00)
	query = append(query, byte(qtype>>8), byte(qtype), 0x00, 0x01)
	return query
}

func parseDNSResponse(response []byte) ([]byte, error) {
	if len(response) < 12 {
		return nil, fmt.Errorf("响应过短")
	}
	ancount := binary.BigEndian.Uint16(response[6:8])
	if ancount == 0 {
		return nil, fmt.Errorf("无答案记录")
	}
	offset := 12
	for offset < len(response) && response[offset] != 0 {
		offset += int(response[offset]) + 1
	}
	offset += 5
	for i := 0; i < int(ancount); i++ {
		if offset >= len(response) {
			break
		}
		if response[offset]&0xC0 == 0xC0 {
			// 验证压缩指针指向的位置
			if offset+2 > len(response) {
				break
			}
			ptrOffset := int(binary.BigEndian.Uint16(response[offset:offset+2]) & 0x3FFF)
			if ptrOffset >= len(response) {
				return nil, fmt.Errorf("无效的DNS压缩指针")
			}
			offset += 2
		} else {
			for offset < len(response) && response[offset] != 0 {
				offset += int(response[offset]) + 1
			}
			offset++
		}
		if offset+10 > len(response) {
			break
		}
		rrType := binary.BigEndian.Uint16(response[offset : offset+2])
		offset += 8
		dataLen := binary.BigEndian.Uint16(response[offset : offset+2])
		offset += 2
		if offset+int(dataLen) > len(response) {
			break
		}
		data := response[offset : offset+int(dataLen)]
		offset += int(dataLen)
		if rrType == typeHTTPS {
			if ech := parseHTTPSRecord(data); ech != nil {
				return ech, nil
			}
		}
	}
	return nil, fmt.Errorf("未找到 ECH")
}

func parseHTTPSRecord(data []byte) []byte {
	if len(data) < 2 {
		return nil
	}
	offset := 2
	if offset < len(data) && data[offset] == 0 {
		offset++
	} else {
		for offset < len(data) && data[offset] != 0 {
			offset += int(data[offset]) + 1
		}
		offset++
	}
	for offset+4 <= len(data) {
		key := binary.BigEndian.Uint16(data[offset : offset+2])
		length := binary.BigEndian.Uint16(data[offset+2 : offset+4])
		offset += 4
		if offset+int(length) > len(data) {
			break
		}
		value := data[offset : offset+int(length)]
		offset += int(length)
		if key == 5 {
			return value
		}
	}
	return nil
}
