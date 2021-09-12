// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"encoding/json"

	"git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/utils/errors"
	"git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/utils/log"
)

func jsonEncode(v interface{}) []byte {
	b, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		log.PanicErrorf(err, "encode to json failed")
	}
	return b
}

func jsonDecode(v interface{}, b []byte) error {
	if err := json.Unmarshal(b, v); err != nil {
		return errors.Trace(err)
	}
	return nil
}
