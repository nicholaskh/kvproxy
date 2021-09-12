// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"strconv"
	"errors"
	"fmt"

    "google.golang.org/grpc"
    "git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/proxy/kv"
	"git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/proxy/redis"
	"git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/utils/log"
)

type forwardKv struct {
}


func (d *forwardKv) Do(r *Request, conn **grpc.ClientConn) (error) {
	var err error
	cmd := string(r.Multi[0].Value)
	switch cmd {
		case "del":
			err = d.Del(r, conn)
		case "set":
			err = d.Set(r, conn)
		case "get":
			err = d.Get(r, conn)
		case "mset":
			err = d.MSet(r, conn)
		case "mget":
			err = d.MGet(r, conn)

		case "hdel":
			err = d.HDel(r, conn)
		case "hexists":
			err = d.HExists(r, conn)
		case "hget":
			err = d.HGet(r, conn)
//		case "hgetall":
//			d.HGetAll(r, conn)
		case "hincrby":
			err = d.HIncrBy(r, conn)
		case "hincrbyfloat":
			err = d.HIncrByFloat(r, conn)
		case "hkeys":
			err =d.HKeys(r, conn)
		case "hlen":
			err =d.HLen(r, conn)
//		case "hmget":
//			d.HMGet(r, conn)
//		case "hmset":
//			d.HMSet(r, conn)
		case "hset":
			err = d.HSet(r, conn)
//		case "hsetnx":
//			d.HSetNx(r, conn)
		case "hvals":
			err =d.HVals(r, conn)
//		case "hscan":
//			d.HScan(r, conn)
	}
	return err
}

func (d *forwardKv) del(r *Request, conn **grpc.ClientConn) (error) {
//	for _, val := range r.Multi {
//            log.Warnf("######## %s", string(val.Value))
//        }
	key := string(r.Multi[1].Value)
	groupId := 0

	kv := new(kv.KvString)
	res, err:=kv.Del(conn, key, groupId)
	if err != nil {
	}
	
	if res == 0 {
		r.Resp = redis.NewInt([]byte(strconv.Itoa(1)))
	}
	log.Warnf("aaaa %+v", r.Resp )
	return nil
}

func (d *forwardKv) Del(r *Request, conn **grpc.ClientConn) (error) {
	var nkeys = len(r.Multi)-1
	log.Warnf("MGet %d", nkeys)
	switch {
	case nkeys == 0:
		return errors.New("ERR wrong number of arguments for 'MGET' command")
		return nil
//	case nkeys == 1:
//		return d.del(r, conn)
	}

	var sub = r.MakeSubRequest(nkeys)
	var n int
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i+1],
		}

		if err := d.del(&sub[i], conn); err != nil {
			log.Warnf("dddd %s", err.Error())
			return err
		}
	
		log.Warnf("del type %d", sub[i].Type)
		switch resp := sub[i].Resp; {
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsInt() && len(resp.Value) == 1:
				n += int(resp.Value[0] - '0')
			default:
				msg := fmt.Sprintf("bad del resp: %s value.len = %d", resp.Type, len(resp.Value))
				return errors.New(msg)
		}
	}
	
	r.Resp = redis.NewInt(strconv.AppendInt(nil, int64(n), 10))
	return nil
}

func (d *forwardKv) Set(r *Request, conn **grpc.ClientConn) (error) {
//	for _, val := range r.Multi {
//            log.Warnf("######## %s", string(val.Value))
//        }
	key := string(r.Multi[1].Value)
	val := string(r.Multi[2].Value)
	groupId := 0

	kv := new(kv.KvString)
	res, err:=kv.Set(conn, key, val, groupId)
	if err != nil {
	}
	
	if res == 0 {
		r.Resp = redis.NewString([]byte("OK"))
	}
	log.Warnf("aaaa %+v", r.Resp )
	return nil
}

func (d *forwardKv) Get(r *Request, conn **grpc.ClientConn) (error) {
	key := string(r.Multi[1].Value)

	kv := new(kv.KvString)
	res, err := kv.Get(conn, key)
	if err != nil {
		
	}
	r.Resp = redis.NewBulkBytes([]byte(res))
	log.Warnf("aaaa %+v", r.Resp )
	return nil
}

func (d *forwardKv) MSet(r *Request, conn **grpc.ClientConn) (error) {
	var nblks = len(r.Multi) - 1
	switch {
	case nblks == 0 || nblks%2 != 0:
		return errors.New("ERR wrong number of arguments for 'MSET' command")
	case nblks == 2:
		return d.Set(r, conn)
	}

	var sub = r.MakeSubRequest(nblks / 2)
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i*2+1],
			r.Multi[i*2+2],
		}
		if err := d.Set(&sub[i], conn); err != nil {
			return err
		}
	}
	
	res := "OK"
	r.Resp = redis.NewString([]byte(res))
	log.Warnf("aaaa %+v", r.Resp )
	return nil
}

func (d *forwardKv) MGet(r *Request, conn **grpc.ClientConn) (error) {
	var nkeys = len(r.Multi)-1
	log.Warnf("MGet %d", nkeys)
	switch {
	case nkeys == 0:
		return errors.New("ERR wrong number of arguments for 'MGET' command")
		return nil
//	case nkeys == 1:
//		return d.Get(r, conn)
	}

	var sub = r.MakeSubRequest(nkeys)
	var array = make([]*redis.Resp, len(sub))
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i+1],
		}

		if err := d.Get(&sub[i], conn); err != nil {
			log.Warnf("dddd %s", err.Error())
			return err
		}

		switch resp := sub[i].Resp; {
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsBulkBytes():
				array[i] = resp
			default:
				msg := fmt.Sprintf("bad mget resp: %s array.len = %d", resp.Type, len(resp.Array))
				return errors.New(msg)
		}
	}
	
	r.Resp = redis.NewArray(array)
	return nil
}


func (d *forwardKv) HDel(r *Request, conn **grpc.ClientConn) (error) {
	key := string(r.Multi[1].Value)
	field := string(r.Multi[2].Value)
	groupId :=1

	kv := new(kv.KvHash)
	res, err:= kv.HDel(conn, key, field, groupId)
	if err != nil {
	}

	r.Resp = redis.NewInt([]byte(strconv.Itoa(res)))
	log.Warnf("aaaa %+v", r.Resp)
	return nil
}

func (d *forwardKv) HExists(r *Request, conn **grpc.ClientConn) (error) {
	key := string(r.Multi[1].Value)
	field := string(r.Multi[2].Value)

	kv := new(kv.KvHash)
	res, err:=kv.HExists(conn, key, field)
	if err != nil {
	}

	r.Resp = redis.NewInt([]byte(strconv.Itoa(res)))
	log.Warnf("aaaa %+v", r.Resp)
	return nil
}

func (d *forwardKv) HGet(r *Request, conn **grpc.ClientConn) (error) {
	key := string(r.Multi[1].Value)
	field := string(r.Multi[2].Value)

	kv := new(kv.KvHash)
	res, err := kv.HGet(conn, key, field)
	if err != nil {
		
	}
	r.Resp = redis.NewString([]byte(res))
	log.Warnf("aaaa %+v", r.Resp)
	return nil
	
}
//
//func (d *forwardKv) HGetAll(r *Request, conn **grpc.ClientConn) ([]string, error) {
//	return []string ,nil
//}
//
func (d *forwardKv) HIncrBy(r *Request, conn **grpc.ClientConn) (error) {
	key := string(r.Multi[1].Value)
	field := string(r.Multi[2].Value)
	num, _ := strconv.Atoi(string(r.Multi[3].Value))
	groupId := 1

	kv := new(kv.KvHash)
	res, err := kv.HIncrBy(conn, key, field, num, groupId)
	if err != nil {
	}

	r.Resp = redis.NewInt([]byte(strconv.Itoa(res)))
	log.Warnf("aaaa %+v", r.Resp)
	return nil

}

func (d *forwardKv) HIncrByFloat(r *Request, conn **grpc.ClientConn) (error) {
	key := string(r.Multi[1].Value)
	field := string(r.Multi[2].Value)
	num, err := strconv.ParseFloat(string(r.Multi[3].Value), 32)

	kv := new(kv.KvHash)
	res, err := kv.HIncrByFloat(conn, key, field, float32(num), 1)
	if err != nil {
	}

	r.Resp = redis.NewString([]byte(strconv.FormatFloat(float64(res),'E',-1, 32)))
	log.Warnf("aaaa %+v", r.Resp)
	return nil
}

func (d *forwardKv) HKeys(r *Request, conn **grpc.ClientConn) (error) {
	key := string(r.Multi[1].Value)

	kv := new(kv.KvHash)
	res, err := kv.HKeys(conn, key)
	if err != nil {
	}

	var array = make([]*redis.Resp, 1)
	for k, v := range res {
		array[k] = redis.NewString([]byte(v))
	}

	r.Resp = redis.NewArray(array)
	log.Warnf("aaaa %+v", r.Resp)
	return nil

}

func (d *forwardKv) HLen(r *Request, conn **grpc.ClientConn) (error) {
	key := string(r.Multi[1].Value)

	kv := new(kv.KvHash)
	res, err := kv.HLen(conn, key)
	if err != nil {
	}

	r.Resp = redis.NewInt([]byte(strconv.Itoa(res)))
	log.Warnf("aaaa %+v", r.Resp)
	return nil

}
//
//func (d *forwardKv) HMGet(r *Request, conn **grpc.ClientConn) ([]string, error) {
//	return []string ,nil
//}
//
//func (d *forwardKv) HMSet(r *Request, conn **grpc.ClientConn) (int, error) {
//	return 1 ,nil
//}
//


func (d *forwardKv) HSet(r *Request, conn **grpc.ClientConn) (error) {
	key := string(r.Multi[1].Value)
	field := string(r.Multi[2].Value)
	value := string(r.Multi[3].Value)

	kv := new(kv.KvHash)
	res, err := kv.HSet(conn, key, field, value, 1)
	log.Warnf("FFFFFFFFFFFFFFFFFF")
	if err != nil {
	}

	r.Resp = redis.NewInt([]byte(strconv.Itoa(res)))
	log.Warnf("aaaa %+v", r.Resp)
	return nil
}

//func (d *forwardKv) HSetNx(r *Request, conn **grpc.ClientConn) (int, error) {
//	key := string(r.Multi[1].Value)
//	field := string(r.Multi[2].Value)
//	value := string(r.Multi[3].Value)
//
//	kv := new(kv.KvHash)
//	return kv.HSet(conn, key, field, value, 1)
//}
//
func (d *forwardKv) HVals(r *Request, conn **grpc.ClientConn) (error) {
	key := string(r.Multi[1].Value)

	kv := new(kv.KvHash)
	res, err := kv.HVals(conn, key)
	if err != nil {
	}

	var array = make([]*redis.Resp, 1)
	for k, v := range res {
		array[k] = redis.NewString([]byte(v))
	}

	r.Resp = redis.NewArray(array)
	log.Warnf("aaaa %+v", r.Resp)
	return nil
}
//
//func (d *forwardKv) HScan(r *Request, conn **grpc.ClientConn) ([]string, error) {
//	return []string ,nil
//}