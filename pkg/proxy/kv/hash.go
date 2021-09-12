package kv

import (
    "golang.org/x/net/context"
    "google.golang.org/grpc"
	"fmt"
    "errors"
    "strings"
    "bytes"
	"strconv"

	"git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/utils/log"
    pb "git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/proxy/kv/phxkv"
)
const (
	HASH_REQ_TYPE =2
)

type KvHash struct {
}

func (cmd *KvHash) KvCmdType(cmdStr string) (int) {
    var cmdType int
	//log.Printf("KvCmdType %s", cmdStr)
    switch cmdStr {
        case "HashDel":
            cmdType = 1
        case "HashGet":
            cmdType = 2
        case "HashGetAll":
            cmdType = 3
        case "HashSet":
            cmdType = 4
        case "HashExists":
            cmdType = 5
        case "HashIncrInt":
            cmdType = 6
        case "HashIncrFloat":
            cmdType = 7
        case "HashKeys":
            cmdType = 8
        case "HashLen":
            cmdType = 9
        case "HashMGet":
            cmdType = 10
        case "HashMSet":
            cmdType = 11
        case "HashSetnx":
            cmdType = 12
        case "HashValues":
            cmdType = 13
    }
	return cmdType
}

func (cmd *KvHash) OpParams(cmdStr string, key string, fields []string, vals interface{}, groupId int) (*pb.Request) {
	var valsSlice []string
	cmdType := cmd.KvCmdType(cmdStr)

	hashReq := new(pb.HashRequest)
	hashReq.ReqType=pb.HashRequestEnumReq(cmdType)
    
    variType := fmt.Sprintf("%T", vals)
	log.Printf("EW deeee %s\n", variType)
    if strings.Compare(variType, "int") == 0 {
        log.Printf("OpParams %d", vals.(int))
        hashReq.IntValue = int32(vals.(int))
    } else if strings.Compare(variType, "float32") == 0 {
        hashReq.FloatValue = vals.(float32)
    }

	for k, field := range fields {
        hashField := new(pb.HashField)
        if strings.Compare(variType, "string") == 0 {
            if vals != "" {
                hashField.FieldValue = []byte(vals.(string))
            }
        } else if strings.Compare(variType, "[]string") == 0 {
            for _, v := range vals.([]string) {
                valsSlice = append(valsSlice, v)
            }	
        }

		buf := bytes.NewBuffer([]byte{})
		buf.WriteByte(byte(HASH_REQ_TYPE))  
		buf.WriteByte(byte(len(key))) 
		buf.WriteString(key)  
		buf.WriteString(field)  
		field = buf.String()

		hashField.FieldKey = []byte(field)
		if strings.Compare(variType, "[]string") == 0 {
			hashField.FieldValue = []byte(valsSlice[k])
		}
		hashReq.Field = append(hashReq.Field, hashField)
		log.Printf("OpParams aaa %d %+v %d", len(field), buf, len(field))
	}

	op := new(pb.Request)
    op.Key=[]byte(key)
	op.DataType = 0
	op.Groupid = uint32(groupId)
	op.HashReq = hashReq
    return op
}

func (cmd *KvHash) ReConn(r *pb.Response, conn **grpc.ClientConn, groupId int) (error) {
    var directIp string
    var directPort int
    group :=r.GetSubMap().SubGroup
    for _, val:= range group {
        if int(val.Groupid) == groupId {
            directIp = string(val.Masterip)
            directPort = int(val.Masterport)
            break
        }
    }

    newConn, err := grpc.Dial(fmt.Sprintf("%s:%d", directIp, directPort), grpc.WithInsecure())
    if err != nil {
        log.Printf("did not connect: %v", err)
        return err
    }
    
    (*conn).Close()
    (*conn) = newConn
    log.Printf("directIp %s, directPort %d", directIp, directPort)
    return nil
}

func (cmd *KvHash) BaseOp(conn **grpc.ClientConn, cmdStr string, key string, field []string, val interface{}, groupId int) (*pb.Response, error) {
	var ret int
    op := cmd.OpParams(cmdStr, key, field, val, groupId)
	for i:=0; i<3; i++ {
		client := pb.NewPhxKVServerClient(*conn)
		r, err := client.HashOperate(context.Background(), op)
		if err != nil {
			log.Printf("could not greet: %v", err)
			return nil, err
		}
        
		ret = int(r.GetRetCode())
        log.Printf("BaseOp ret %d", ret)
		if ret == 0 {
			return r, nil
		} else if ret == 10 {
            err := cmd.ReConn(r, conn, groupId)
            if err != nil {
				return nil, err
            }
		} else if ret == 1{
			return nil, errors.New(fmt.Sprintf("key %s not exists", key))
		}
	}
	return nil, errors.New(fmt.Sprintf("key %s error", key))
}

func (cmd *KvHash) ReadBaseOp(r *pb.Response) ([]string, []string, error) {
	var keys []string
	var values []string

	if r.GetHashResponse() != nil {
		if res := r.GetHashResponse().GetField(); res != nil {
			for _, val := range res {
				if len(val.GetFieldKey()) > 0 {
					tmp := val.GetFieldKey()
					tmp2 := []byte(tmp)
					//log.Printf("ccc  %s %+v", string(tmp), tmp2)

					keyLen := int(tmp2[1])
					keyTmp :=string(tmp2[2+keyLen: ])
					//log.Printf("%s", keyTmp)
					keys = append(keys, keyTmp)
				}
			
				if len(val.GetFieldValue()) > 0 {
					values = append(values, string(val.GetFieldValue()))
				}
			}
			return keys, values, nil
		}
	}
	return keys, values, errors.New("error")
}


func (cmd *KvHash) HIncrByBase(conn **grpc.ClientConn, cmdStr string, key string, fields []string, num interface{}, groupId int) (string, error) {
	var value string

	r, err := cmd.BaseOp(conn, "HashIncrFloat", key, fields, num, groupId)
	if err !=nil {
		return value, err
	}

    log.Printf("EEEEEEEE %+v", r)
	if r.GetHashResponse() != nil {
		if r.GetHashResponse().GetField() != nil {
			tmp := r.GetHashResponse().GetField()[0].GetFieldValue()
			if len(tmp) <= 0 {
				return value, errors.New(fmt.Sprintf("key %s error", key))
			}
			value = string(tmp)
		}
	}
	return value, nil
}

func (cmd *KvHash) HDel(conn **grpc.ClientConn, key string, field string, groupId int) (int, error) {
	fields := []string{
		field,
	}
	val := ""

	_, err := cmd.BaseOp(conn, "HashDel", key, fields, val, groupId)
	if err !=nil {
		return 0, err
	}
	return 1, nil
}

func (cmd *KvHash) HSetNX(conn **grpc.ClientConn, key string, field string, val string, groupId int) (int, error) {
	fields := []string{
		field,
	}
	_, err := cmd.BaseOp(conn, "HashSetnx", key, fields, val, groupId)
	if err !=nil {
		return 0, err
	}
	return 1, nil
}

func (cmd *KvHash) HSet(conn **grpc.ClientConn, key string, field string, val string, groupId int) (int, error) {
	fields := []string{
		field,
	}
	_, err := cmd.BaseOp(conn, "HashSet", key, fields, val, groupId)
	if err !=nil {
		return 0, err
	}
	return 1, nil
}

func (cmd *KvHash) HMSet(conn **grpc.ClientConn, key string, fields []string, vals []string, groupId int) ([]string, error) {
	var ret []string

	_, err := cmd.BaseOp(conn, "HashMSet", key, fields, vals, groupId)
	if err !=nil {
		return ret, err
	}

	return []string{}, nil
}

func (cmd *KvHash) HMGet(conn **grpc.ClientConn, key string, fields []string) ([]string, error) {
	var ret []string
	val := ""
	groupId := 0

	r, err := cmd.BaseOp(conn, "HashMGet", key, fields, val, groupId)
	if err !=nil {
		return ret, err
	}

	_, vals, err := cmd.ReadBaseOp(r)
	if err != nil {
		return ret, err
	}
	return vals, nil
}

func (cmd *KvHash) HGet(conn **grpc.ClientConn, key string, field string) (string, error) {
	var ret string
	fields := []string{
		field,
	}
	value := ""
	groupId := 0
	
	r, err := cmd.BaseOp(conn, "HashGet", key, fields, value, groupId)
	if err !=nil {
		return ret, err
	}
	
	_, vals, err := cmd.ReadBaseOp(r)
	if err != nil {
		return ret, err
	}
	return vals[0], nil
}

func (cmd *KvHash) HGetAll(conn **grpc.ClientConn, key string) (map[string]string, error) {
	ret := make(map[string]string)
	fields := []string{}
	value := ""
	groupId := 0
	
	r, err := cmd.BaseOp(conn, "HashGetAll", key, fields, value, groupId)
	if err !=nil {
		return ret, err
	}

	keys, vals, err := cmd.ReadBaseOp(r)
	if err != nil {
		return ret, err
	}
	
	for k, v := range keys {
		ret[v] = vals[k]
	}
	return ret, nil
}

func (cmd *KvHash) HKeys(conn **grpc.ClientConn, key string) ([]string, error) {
	var ret []string

	fields := []string{}
	value := ""
	groupId := 0
	
	r, err := cmd.BaseOp(conn, "HashKeys", key, fields, value, groupId)
	if err !=nil {
		return ret, err
	}
	
	keys, _, err := cmd.ReadBaseOp(r)
	if err != nil {
		return ret, err
	}
	return keys, nil
}

func (cmd *KvHash) HVals(conn **grpc.ClientConn, key string) ([]string, error) {
	var ret []string

	fields := []string{}
	value := ""
	groupId := 0
	
	r, err := cmd.BaseOp(conn, "HashValues", key, fields, value, groupId)
	if err !=nil {
		return ret, err
	}
	
	_, vals, err := cmd.ReadBaseOp(r)
	if err != nil {
		return ret, err
	}
	return vals, nil
}

func (cmd *KvHash) HLen(conn **grpc.ClientConn, key string) (int, error) {
	fields := []string{}
	value := ""
	groupId := 0
	
	r, err := cmd.BaseOp(conn, "HashExists", key, fields, value, groupId)
	if err !=nil {
		return 0, err
	}

	ret := r.GetLength()
	return int(ret), nil
}

func (cmd *KvHash) HExists(conn **grpc.ClientConn, key string, field string) (int, error) {
	var ret int

	fields := []string{
		field,
	}
	value := ""
	groupId := 0
	
	r, err := cmd.BaseOp(conn, "HashExists", key, fields, value, groupId)
	if err !=nil {
		return 0, err
	}

	res := r.GetExist()
	if res {
		ret = 1
	} else {
		ret = 0
	}
	return ret, nil
}


func (cmd *KvHash) HIncrBy(conn **grpc.ClientConn, key string, field string, num int, groupId int) (int, error) {
    fields := []string{
		field,
	}

	value, err := cmd.HIncrByBase(conn, "HashIncr", key, fields, num, groupId)
	if err !=nil {
        log.Printf("HIncrBy error %s", err.Error())
		return 0, err
	}

    log.Printf("aaaaaa %s", value)
	ret, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	return ret, nil
}

func (cmd *KvHash) HIncrByFloat(conn **grpc.ClientConn, key string, field string, num float32, groupId int) (float32, error) {
	fields := []string{
		field,
	}

	value, err := cmd.HIncrByBase(conn, "HashIncrFloat", key, fields, num, groupId)
	if err !=nil {
		return 0, err
	}

	ret, err := strconv.ParseFloat(value, 32)
	if err != nil {
		return 0.0, err
	}
	return float32(ret), nil
}