// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"time"

	"git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/models/etcd"
	"git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/models/fs"
	"git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/models/zk"
	"git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/utils/errors"
)

type Client interface {
	Create(path string, data []byte) error
	Update(path string, data []byte) error
	Delete(path string) error

	Read(path string, must bool) ([]byte, error)
	List(path string, must bool) ([]string, error)

	Close() error

	WatchInOrder(path string) (<-chan struct{}, []string, error)

	CreateEphemeral(path string, data []byte) (<-chan struct{}, error)
	CreateEphemeralInOrder(path string, data []byte) (<-chan struct{}, string, error)
}

func NewClient(coordinator string, addrlist string, auth string, timeout time.Duration) (Client, error) {
	switch coordinator {
	case "zk", "zookeeper":
		return zkclient.New(addrlist, auth, timeout)
	case "etcd":
		return etcdclient.New(addrlist, auth, timeout)
	case "fs", "filesystem":
		return fsclient.New(addrlist)
	}
	return nil, errors.Errorf("invalid coordinator name = %s", coordinator)
}
