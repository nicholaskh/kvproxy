package kv

import (
     "git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/utils/log"
    pb "git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/proxy/kv/phxkv"
    "golang.org/x/net/context"
    "google.golang.org/grpc"
	"fmt"
    "errors"
)

type KvString struct {
}

func (cmd *KvString) KvOpParams(key string, val string, groupId int) (*pb.KVOperator) {
    op := &pb.KVOperator{}
	op.Key=key
    op.Operator = 2

    if val != "" {
	    op.Value = []byte(val)
        op.Groupid=uint32(groupId)
    }
	
    return op
}

func (cmd *KvString) KvReConn(r *pb.KVResponse, conn **grpc.ClientConn) (error) {
    var directIp string
    var directPort int
    group :=r.GetSubMap().SubGroup
    for _, val:= range group {
        if val.Groupid == 1 {
            directIp = string(val.Masterip)
            directPort = int(val.Masterport)
            break
        }
    }

    //(*conn).Close()
    newConn, err := grpc.Dial(fmt.Sprintf("%s:%d", directIp, directPort), grpc.WithInsecure())
    if err != nil {
        log.Warnf("did not connect: %v", err)
        return err
    }
    
    (*conn).Close()
    (*conn) = newConn
    //log.Warnf("directIp %s, directPort %d", directIp, directPort)
    return nil
}

func (cmd *KvString) KvReConnBatch(r *pb.KvBatchGetResponse, conn **grpc.ClientConn) (error) {
    var directIp string
    var directPort int
    group :=r.GetSubMap().SubGroup
    for _, val:= range group {
        if val.Groupid == 1 {
            directIp = string(val.Masterip)
            directPort = int(val.Masterport)
            break
        }
    }

    //(*conn).Close()
    newConn, err := grpc.Dial(fmt.Sprintf("%s:%d", directIp, directPort), grpc.WithInsecure())
    if err != nil {
        log.Warnf("did not connect: %v", err)
        return err
    }
    
    (*conn).Close()
    (*conn) = newConn
    log.Warnf("directIp %s, directPort %d", directIp, directPort)
    return nil
}

func (cmd *KvString) Del(conn **grpc.ClientConn, key string, groupId int) (int, error) {
	var ret int
	val := ""
    op := cmd.KvOpParams(key, val, groupId)
	for i:=0; i<3; i++ {
		client := pb.NewPhxKVServerClient(*conn)
		r, err := client.KvDelete(context.Background(), op)
		if err != nil {
			log.Warnf("could not greet: %v", err)
		}
		
		ret = int(r.GetRet())
		if ret == 0 {
			return ret, nil
		} else if ret == 10 {
		    err := cmd.KvReConn(r, conn)
            if err != nil {
            }
		}
	}
	return 0, errors.New("error")
}

func (cmd *KvString) Set(conn **grpc.ClientConn, key string, val string, groupId int) (int, error) {
	var ret int

//	op := pb.KVOperator{}
//	op.Key=key
//	op.Value = []byte(val)
//	op.Operator = 2
//	op.Groupid=uint32(groupId)
    
    op := cmd.KvOpParams(key, val, groupId)
	for i:=0; i<3; i++ {
		client := pb.NewPhxKVServerClient(*conn)
		r, err := client.KvPut(context.Background(), op)
		if err != nil {
			log.Warnf("could not greet: %v", err)
		}
		
		ret = int(r.GetRet())
		if ret == 0 {
			return ret, nil
		} else if ret == 10 {
		    err := cmd.KvReConn(r, conn)
            if err != nil {
            }
		}
	}
	return 0, errors.New("error")
}

func (cmd *KvString) Get(conn **grpc.ClientConn, key string) (string, error) {
	var ret int
    val :=  ""
    groupId := 0
	op := cmd.KvOpParams(key, val, groupId)

	for i:=0; i<3; i++ {
		client := pb.NewPhxKVServerClient(*conn)
		r, err := client.KvGet(context.Background(), op)
		if err != nil {
			log.Warnf("could not greet: %v", err)
		}
		
		ret = int(r.GetRet())
		if ret == 0 {
			return string(r.Data.Value), nil
		} else if ret == 10 {
	        err := cmd.KvReConn(r, conn)
            if err != nil {
            }
		}
	}
	return "", nil
}

func (cmd *KvString) MGet(conn **grpc.ClientConn, key ...string) ([]string, error) {
	var ret int
    var result []string
    
    op := new(pb.KvBatchGetRequest)
    for _, v := range key {
      subRequest := new(pb.KvBatchGetSubRequest)
      subRequest.Key = []byte(v)
      op.Subs = append(op.Subs, subRequest)
   }

	for i:=0; i<3; i++ {
		client := pb.NewPhxKVServerClient(*conn)
		r, err := client.KvBatchGet(context.Background(), op)
		if err != nil {
			log.Warnf("could not greet: %v", err)
		}
		
		ret = int(r.GetRet())
		if ret == 0 {
            tmp := r.GetValues()
            if tmp != nil {
                for _, v := range tmp {
                    tmp3 := v.GetValue()
                    if tmp3 != nil {
                        result = append(result, string(tmp3))
                    }
                }
            }
			return result, nil
		} else if ret == 10 {
	        err := cmd.KvReConnBatch(r, conn)
            if err != nil {
            }
		}
	}
	return result, errors.New("error")
}

//func (cmd *KvString) KvMSet(conn **grpc.ClientConn, key ...string, val ...string) (bool, error) {
//	var ret int
//    var result []string
//    
//    op := new(pb.KvBatchPutRequest)
//
//    var valSlice []string
//    for _, v := range val {
//        valSlice = append(valSlice, v)
//    }
//
//    for k, v := range key {
//      subRequest := new(pb.KvBatchPutSubRequest)
//      subRequest.Key = []byte(v)
//      subRequest.Value = []byte(valSlice[k])
//      op.Subs = append(op.Subs, subRequest)
//   }
//
//	for i:=0; i<3; i++ {
//		client := pb.NewPhxKVServerClient(*conn)
//		r, err := client.KvBatchPut(context.Background(), op)
//		if err != nil {
//			log.Warnf("could not greet: %v", err)
//		}
//		
//		ret = int(r.GetRet())
//		if ret == 0 {
//			return true, nil
//		} else if ret == 10 {
//	        err := cmd.KvReConnBatch(r, conn)
//            if err != nil {
//            }
//		}
//	}
//	return result, errors.New("error")
//}