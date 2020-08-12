package chord

import (
	"math/big"
	"net"
	"net/rpc"
	"time"
)

const (
	max_trial_time = 3
	waitTime = 500 * time.Millisecond
)

func GetLocalAddress() string {
	var localaddress string
	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}
	return localaddress
}

func jump(id *big.Int, num int) *big.Int {
	index := big.NewInt(int64(num) - 1)
	jump := new(big.Int).Exp(two, index, nil)
	sum := new(big.Int).Add(id, jump)
	return new(big.Int).Mod(sum, hashMod)
}

func between(start, elt, end *big.Int, inclusive bool) bool {
	if end.Cmp(start) > 0 {
		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
	} else {
		return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
	}
}

func  ping(addr string) bool {
	for i := 0; i < max_trial_time; i++ {
		ch := make(chan bool)
		go func() {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				ch <- false
				return
			} else{
				defer client.Close()
				var listening bool
				_ = client.Call("Node.Checkprocess", 0, &listening)
				ch <- listening
				return
			}
		}()
		var ch_info bool
		select {
		case ch_info = <-ch:
			if ch_info {
				return true
			} else {
				continue
			}
		case <-time.After(waitTime):
			continue
		}
	}
	return false
}

func dial(addr string)(*rpc.Client, error){
	var err error
	var client *rpc.Client
	for i := 0; i < max_trial_time; i++{
		client, err = rpc.Dial("tcp", addr)
		if err == nil{
			return client, err
		}else{
			time.Sleep(waitTime)
		}
	}
	return nil, err
}