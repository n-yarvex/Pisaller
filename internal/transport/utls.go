package transport

import "github.com/refraction-networking/utls"

func GetClientHelloID(name string) *utls.ClientHelloID {
    switch name {
    case "chrome_120":
        return &utls.HelloChrome_120
    case "chrome_112":
        return &utls.HelloChrome_112
    case "firefox_120":
        return &utls.HelloFirefox_120
    case "firefox_115":
        return &utls.HelloFirefox_115
    case "ios_17":
        return &utls.HelloIOS_17_0
    case "ios_16":
        return &utls.HelloIOS_16_0
    case "edge_120":
        return &utls.HelloEdge_120
    case "edge_112":
        return &utls.HelloEdge_112
    case "safari_16":
        return &utls.HelloSafari_16_0
    case "safari_17":
        return &utls.HelloSafari_17_0
    default:
        return nil
    }
}
