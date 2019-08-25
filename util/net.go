package util

import (
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
)

//get_internal_ip 获取本机的内网IP
func GetInternalIp() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Oops:" + err.Error())
		return ""
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	os.Stderr.WriteString("can not get self ip")
	return ""
}

func GetSelfHost() string {
	if host, err := os.Hostname(); err == nil {
		return host
	} else {
		return ""
	}
}

func Ip2Int(ip string) (int32, error) {
	arr := strings.Split(ip, ".")
	if len(arr) == 4 {
		var part1 int64
		var part2 int64
		var part3 int64
		var part4 int64
		var err error
		if part1, err = strconv.ParseInt(arr[0], 10, 64); err != nil {
			return 0, err
		}
		if part2, err = strconv.ParseInt(arr[1], 10, 64); err != nil {
			return 0, err
		}
		if part3, err = strconv.ParseInt(arr[2], 10, 64); err != nil {
			return 0, err
		}
		if part4, err = strconv.ParseInt(arr[3], 10, 64); err != nil {
			return 0, err
		}
		if part1 >= 0 && part1 < 256 && part2 >= 0 && part2 < 256 && part3 >= 0 && part3 < 256 && part4 >= 0 && part4 < 256 {
			// 左移，正数左移之后有可能把最高位变为1，从而成为负数
			rect := part1 << 24
			rect += part2 << 16
			rect += part3 << 8
			rect += part4
			return int32(rect), nil
		} else {
			return 0, errors.New("ip value out of scope " + ip)
		}
	} else {
		return 0, errors.New("invalid ip " + ip)
	}
}
