package kademlia

import (
	"crypto/sha1"
	"math/big"
	"net"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

type Bucket struct {
	currentlength         int
	visitor          [20]ABpair
	permission        sync.Mutex
	visitTime time.Time
}

func (bucket *Bucket) checkexist(tmp ABpair) int{
	res := -1
	i := 0
	for i < bucket.currentlength {
		if Ping(bucket.visitor[i].A){
			if bucket.visitor[i].A == tmp.A {
				res = i
			}
			i++
			continue
		} else{
			for j := i; j < bucket.currentlength - 1; j++{
				bucket.visitor[j] = bucket.visitor[j+1]
			}
			bucket.currentlength--
		}
	}
	return  res
}
type ABpair struct {
	A string
	B *big.Int
}

type KVpair struct {
	K string
	V string
}

type VF_R struct {
	V ABpair
	F bool
}

type VDT_R struct {
	V ABpair
	D KVpair
	T time.Time
}

type VS_R struct {
	V ABpair
	S bool
}

type VB_R struct {
	V ABpair
	B *big.Int
}

type VN_R struct {
	V  ABpair
	N []ABpair
}

type VBK_R struct {
	V ABpair
	B *big.Int
	K string
}


type VNV_R struct {
	Vis  ABpair
	N []ABpair
	Val string
}

type VTT struct {
	V string
	T1 time.Time
	T2 time.Time
}

type DMpair struct {
	D  map[string]VTT
	M sync.Mutex
}

type Node struct {
	Node_info ABpair
	Nodes   [160]Bucket
	Info       DMpair
	Source DMpair
	Inprocess bool
}


func find_bucket_pos(a *big.Int, b * big.Int) int{
	tmp := new(big.Int).Xor(a, b)
	res := tmp.BitLen() - 1
	return res
}

func cmp(a *big.Int, b *big.Int, c *big.Int) bool{
	cmp1 := new(big.Int).Xor(a, c)
	cmp2 := new(big.Int).Xor(b, c)
	return cmp1.Cmp(cmp2) < 0
}

func HashString(A string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(A))
	return new(big.Int).SetBytes(hash.Sum(nil))
}

func GetLocalAddress() string {
	var localaddress string

	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				ipnet, ok := addr.(*net.IPNet)
				if ok {
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

func Dial(A string) (*rpc.Client, error) {
	var err error
	var client *rpc.Client
	for i := 0; i < 3; i++ {
		client, err = rpc.Dial("tcp", A)
		if err == nil {
			return client, err
		} else {
			time.Sleep(time.Millisecond * 500)
		}
	}
	return nil, err
}

func Ping(A string) bool {
	for i := 0; i < 3; i++ {
		ch := make(chan bool)
		go func() {
			client, err := rpc.Dial("tcp", A)
			if err == nil {
				err = client.Close()
				ch <- true
				return
			} else {
				ch <- false
				return
			}
		}()
		select {
		case ok := <-ch:
			if ok {
				return true
			} else {
				continue
			}
		case <-time.After(time.Millisecond * 500):
			break
		}
	}
	return false
}


func (bucket *Bucket) visit(new ABpair) {
	bucket.permission.Lock()
	tmp := bucket.checkexist(new)
	if tmp != -1{
		for i := tmp; i < bucket.currentlength - 1; i++{
			bucket.visitor[i] = bucket.visitor[i + 1]
		}
		bucket.visitor[bucket.currentlength - 1] = new
		bucket.permission.Unlock()
		bucket.visitTime = time.Now()
		return
	}
	if bucket.currentlength < 20 {
		bucket.visitor[bucket.currentlength] = new
		bucket.currentlength++
		bucket.permission.Unlock()
		bucket.visitTime = time.Now()
		return
	} else if Ping(bucket.visitor[0].A){
		tmp1 := bucket.visitor[0]
		for i := 0; i < bucket.currentlength-1; i++ {
			bucket.visitor[i] = bucket.visitor[i+1]
		}
		bucket.visitor[bucket.currentlength-1] = tmp1
	} else {
		for i := 0; i < bucket.currentlength-1; i++ {
			bucket.visitor[i] = bucket.visitor[i+1]
		}
		bucket.visitor[bucket.currentlength-1] = new
	}
	bucket.permission.Unlock()
	bucket.visitTime = time.Now()
}

func (node *Node) Ping(A string) bool {
	success := Ping(A)
	if !success {
		return false
	}
	client, err := Dial(A)
	if err != nil {
		return false
	} else{
		defer client.Close()
	}
	var res VF_R
	err = client.Call("Node.Callback", ABpair{node.Node_info.A, node.Node_info.B}, &res)
	if err != nil {
		return false
	}
	go func(){
		if node.Node_info.B.Cmp(res.V.B) != 0{
			node.Nodes[find_bucket_pos(node.Node_info.B, res.V.B)].visit(res.V)
		}
	}()
	return true
}

func (node *Node) Init(port string) {
	node.Node_info.A = GetLocalAddress() + ":" + port
	node.Node_info.B = HashString(node.Node_info.A)
	node.Source.D = make(map[string]VTT)
	node.Info.D = make(map[string]VTT)
}

func (node *Node) Checkprocess (no_use_one int, process *bool) error{
	*process = node.Inprocess
	return nil
}

func (node *Node) Join(A string) {
	tmp := ABpair{A, HashString(A)}
	if node.Node_info.B.Cmp(tmp.B) != 0{
		node.Nodes[find_bucket_pos(node.Node_info.B, tmp.B)].visit(tmp)
	}
	node.MuitiGetNeighbours(tmp.B)
}
func (node *Node) search(k string) (string, bool) {
	node.Info.M.Lock()
	v, ok := node.Info.D[k]
	node.Info.M.Unlock()
	return v.V, ok
}
func (node *Node) bucket_sort(b *big.Int) []ABpair{
	var res []ABpair
	for i := 0; i < 160; i++ {
		node.Nodes[i].permission.Lock()
		for j := 0; j < node.Nodes[i].currentlength; j++ {
			res = append(res, node.Nodes[i].visitor[j])
		}
		node.Nodes[i].permission.Unlock()
	}
	sort.Slice(res, func(i, j int) bool {
		return cmp(res[i].B, res[j].B, b)
	})
	return res
}
func (node *Node) findNeighbours(b *big.Int, num int) []ABpair {
	var res []ABpair
    distance := find_bucket_pos(node.Node_info.B, b)
    if node.Node_info.B.Cmp(b) == 0{
    	distance = 0
	}
	node.Nodes[distance].permission.Lock()
	if node.Nodes[distance].currentlength >= num {
		for i := 0; i < num; i++ {
			res = append(res, node.Nodes[distance].visitor[i])
		}
		node.Nodes[distance].permission.Unlock()
		return res
	}
	node.Nodes[distance].permission.Unlock()
    all := node.bucket_sort(b)
	length := len(all)
	if length >= num {
		for i := 0; i < num; i++ {
			res = append(res, all[i])
		}
	} else {
		res = all
	}
	return res
}

func (node *Node) MuitiGetNeighbours(b *big.Int) []ABpair {
	var tmp []ABpair
	VIS := make(map[string]bool)
	Neighbours := node.findNeighbours(b, 3)
	pos := 0
	for pos < len(Neighbours) {
		if VIS[Neighbours[pos].A]{
			pos++
			continue
		}
		if node.Ping(Neighbours[pos].A){
			VIS[Neighbours[pos].A] = true
			tmp = append(tmp, Neighbours[pos])

			client, err := Dial(Neighbours[pos].A)
			if err != nil {
				continue
			}
			var res VN_R
			err = client.Call("Node.NN", VB_R{node.Node_info, b}, &res)
			_ = client.Close()
			if err != nil {
				continue
			}
			go func(){
				if node.Node_info.B.Cmp(res.V.B) != 0{
					node.Nodes[find_bucket_pos(node.Node_info.B, res.V.B)].visit(res.V)
				}
			}()
			for _, v := range res.N {
				Neighbours = append(Neighbours, v)
			}
		}
		pos++
	}
	sort.Slice(tmp, func(i, j int) bool {
		return cmp(tmp[i].B, tmp[j].B, b)
	})
	if len(tmp) >= 20 {
		var res []ABpair
		for i := 0; i < 20; i++ {
			res = append(res, tmp[i])
		}
		return res
	} else {
		return tmp
	}
}

func (node *Node)findVal_around_neighbour(info VBK_R) (string, bool) {
	var tmp []ABpair
	VIS := make(map[string]bool)
	Neighbours := node.findNeighbours(info.B, 3)
	pos := 0
	for pos < len(Neighbours) {
		if VIS[Neighbours[pos].A] {
			pos++
			continue
		}
		if node.Ping(Neighbours[pos].A){
			VIS[Neighbours[pos].A] = true
			client, err := Dial(Neighbours[pos].A)
			if err != nil {
				continue
			}
			var res VNV_R
			err = client.Call("Node.NV", VBK_R{node.Node_info, info.B,info.K}, &res)
			_ = client.Close()
			if err != nil {
				continue
			}
			go func(){
				if node.Node_info.B.Cmp(res.Vis.B) != 0{
					node.Nodes[find_bucket_pos(node.Node_info.B, res.Vis.B)].visit(res.Vis)
				}
			}()
			if res.N == nil {
				sort.Slice(tmp, func(i, j int) bool {
					return cmp(tmp[i].B, tmp[j].B, info.B)
				})
				if len(tmp) > 0 {
					go func() {
						client, _ := Dial(tmp[0].A)
						var temp VS_R
						_ = client.Call("Node.Save", VDT_R{node.Node_info, KVpair{info.K, res.Val}, time.Now().Add(time.Minute )}, &temp)
						_ = client.Close()
						go func(){
							if node.Node_info.B.Cmp(temp.V.B) != 0{
								node.Nodes[find_bucket_pos(node.Node_info.B, temp.V.B)].visit(temp.V)
							}
						}()
					}()
				}
				return res.Val, true
			} else {
				for _, v := range res.N {
					Neighbours = append(Neighbours, v)
				}
				tmp = append(tmp, Neighbours[pos])
			}
		}
		pos++
	}
	return "", false
}

func (node *Node) save_info(info VDT_R) bool {
	Neighbours := node.MuitiGetNeighbours(HashString(info.D.K))
	success := false
	for _, t := range Neighbours {
		client, err := Dial(t.A)
		if err != nil {
			continue
		}
		var res VS_R
		err = client.Call("Node.Save", VDT_R{node.Node_info, info.D, time.Now().Add(time.Minute)}, &res)
		_ = client.Close()
		if err != nil {
			continue
		}
		go func(){
			if node.Node_info.B.Cmp(res.V.B) != 0{
				node.Nodes[find_bucket_pos(node.Node_info.B, res.V.B)].visit(res.V)
			}
		}()
		if res.S {
			success = true
		}
	}
	return success
}

func (node *Node) PutToDHT(k, v string, flag bool) bool {
	node.save_info(VDT_R{node.Node_info, KVpair{k, v}, time.Now().Add(time.Minute )})
	if flag{
		node.Source.M.Lock()
		node.Source.D[k] = VTT{v, time.Now().Add(time.Minute), time.Time{}}
		node.Source.M.Unlock()
	}
	return true
}

func (node *Node) GetFromDHT(k string) (string, bool) {
	node.Info.M.Lock()
	v, ok := node.Info.D[k]
	if ok {
		node.Info.M.Unlock()
		return v.V, true
	}
	node.Info.M.Unlock()
	node.Source.M.Lock()
	v, ok = node.Source.D[k]
	if ok {
		node.Source.M.Unlock()
		return v.V, true
	}
	node.Source.M.Unlock()
	return node.findVal_around_neighbour(VBK_R{node.Node_info, HashString(k), k})
}

func (node *Node) Republish() {
	node.Source.M.Lock()
	for k, v := range node.Source.D {
		if time.Now().After(v.T1) {
			node.PutToDHT(k, v.V, false)
			v.T1 = time.Now().Add(time.Minute)
		}
	}
	node.Source.M.Unlock()
}

func (node *Node) Expire() {
	node.Info.M.Lock()
	for k, v := range node.Info.D {
		if time.Now().After(v.T1) {
			delete(node.Info.D, k)
		}
	}
	node.Info.M.Unlock()
}

func (node *Node) Repliacte(){
	tmp := make([]VDT_R, 0)
	node.Info.M.Lock()
	for k, v := range node.Info.D{
		if !v.T2.IsZero() && time.Now().After(v.T2) {
			tmp = append(tmp, VDT_R{node.Node_info, KVpair{k, v.V}, v.T1})
		}
	}
	node.Info.M.Unlock()
	for _, v := range tmp {
		node.save_info(v)
		node.Info.D[v.D.K] = VTT{v.D.V, v.T, time.Time{}}
	}
}

func (node *Node) Refresh() {
	for i := 0; i < 160; i++ {
		if node.Nodes[i].visitTime.Add(30 * time.Second).Before(time.Now()) {
			node.MuitiGetNeighbours(new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(i)), nil))
		}
	}
}

func (node *Node) Callback(info ABpair, res *VF_R) error {
	go func(){
		if node.Node_info.B.Cmp(info.B) != 0{
			node.Nodes[find_bucket_pos(node.Node_info.B, info.B)].visit(info)
		}
	}()
	*res = VF_R{node.Node_info,true}
	return nil
}

func (node *Node) Save(info VDT_R, res *VS_R) error {
	go func(){
		if node.Node_info.B.Cmp(info.V.B) != 0{
			node.Nodes[find_bucket_pos(node.Node_info.B, info.V.B)].visit(info.V)
		}
	}()
	node.Info.M.Lock()
	node.Info.D[info.D.K] = VTT{info.D.V, info.T, time.Now().Add(30 * time.Second)}
	node.Info.M.Unlock()
	*res = VS_R{node.Node_info, true}
	return nil
}

func (node *Node) NN(info VB_R, res *VN_R) error {
	go func(){
		if node.Node_info.B.Cmp(info.V.B) != 0{
			node.Nodes[find_bucket_pos(node.Node_info.B, info.V.B)].visit(info.V)
		}
	}()
	res.V = ABpair{node.Node_info.A, node.Node_info.B}
	res.N = make([]ABpair, 0)
	tmp := node.findNeighbours(info.B, 20)
	res.N = tmp
	return nil
}

func (node *Node) NV(info VBK_R, res *VNV_R) error {
	go func(){
		if node.Node_info.B.Cmp(info.V.B) != 0{
			node.Nodes[find_bucket_pos(node.Node_info.B, info.V.B)].visit(info.V)
		}
	}()
	v, ok := node.search(info.K)
	if ok {
		*res = VNV_R{node.Node_info, nil, v}
		return nil
	}
	res.Vis = node.Node_info
	res.N = make([]ABpair, 0)
	res.Val = ""
	tmp := node.findNeighbours(info.B, 20)
	res.N = tmp
	return nil
}
