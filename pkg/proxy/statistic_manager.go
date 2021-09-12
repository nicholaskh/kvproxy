package proxy

import (
	"net/http"
	"time"

	"git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/stats"
	"git.100tal.com/wangxiao_jichujiagou_common/viktor-proxy/pkg/stats/prometheus"
)

const (
	statsLabelCluster       = "Cluster"
	statsLabelOperation     = "Operation"
	statsLabelNamespace     = "Namespace"
	statsLabelFingerprint   = "Fingerprint"
	statsLabelFlowDirection = "Flowdirection"
	statsLabelSlice         = "Slice"
	statsLabelIPAddr        = "IPAddr"
)

// StatisticManager statistics manager
type StatisticManager struct {
	//manager     *Manager
	//clusterName string

	statsType string // 监控后端类型
	handlers  map[string]http.Handler

	sqlTimings *stats.MultiTimings // SQL耗时统计
	//	sqlFingerprintSlowCounts  *stats.CountersWithMultiLabels // 慢SQL指纹数量统计
	//	sqlErrorCounts            *stats.CountersWithMultiLabels // SQL错误数统计
	//	sqlFingerprintErrorCounts *stats.CountersWithMultiLabels // SQL指纹错误数统计
	//	sqlForbidenCounts         *stats.CountersWithMultiLabels // SQL黑名单请求统计
	//	flowCounts                *stats.CountersWithMultiLabels // 业务流量统计
	//	sessionCounts             *stats.GaugesWithMultiLabels   // 前端会话数统计
	//
	//	backendSQLTimings                *stats.MultiTimings            // 后端SQL耗时统计
	//	backendSQLFingerprintSlowCounts  *stats.CountersWithMultiLabels // 后端慢SQL指纹数量统计
	//	backendSQLErrorCounts            *stats.CountersWithMultiLabels // 后端SQL错误数统计
	//	backendSQLFingerprintErrorCounts *stats.CountersWithMultiLabels // 后端SQL指纹错误数统计
	//	backendConnectPoolIdleCounts     *stats.GaugesWithMultiLabels   //后端空闲连接数统计
	//	backendConnectPoolInUseCounts    *stats.GaugesWithMultiLabels   //后端正在使用连接数统计
	//	backendConnectPoolWaitCounts     *stats.GaugesWithMultiLabels   //后端等待队列统计

	//slowSQLTime int64
	closeChan chan bool
}

// NewStatisticManager return empty StatisticManager
func NewStatisticManager() *StatisticManager {
	return &StatisticManager{}
}

// CreateStatisticManager create StatisticManager
func CreateStatisticManager() (*StatisticManager, error) {
	mgr := NewStatisticManager()

	var err error
	if err = mgr.Init(); err != nil {
		return nil, err
	}
	//	if mgr.generalLogger, err = initGeneralLogger(cfg); err != nil {
	//		return nil, err
	//	}
	return mgr, nil
}

// Init init StatisticManager
func (s *StatisticManager) Init() error {
	s.closeChan = make(chan bool, 0)
	s.handlers = make(map[string]http.Handler)
	//	s.slowSQLTime = cfg.SlowSQLTime
	//	statsCfg, err := parseProxyStatsConfig(cfg)
	//	if err != nil {
	//		return err
	//	}

	if err := s.initBackend(); err != nil {
		return err
	}

	s.sqlTimings = stats.NewMultiTimings("SqlTimings",
		"gaea proxy sql sqlTimings", []string{statsLabelCluster, statsLabelNamespace, statsLabelOperation})
	//	s.sqlFingerprintSlowCounts = stats.NewCountersWithMultiLabels("SqlFingerprintSlowCounts",
	//		"gaea proxy sql fingerprint slow counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelFingerprint})
	//	s.sqlErrorCounts = stats.NewCountersWithMultiLabels("SqlErrorCounts",
	//		"gaea proxy sql error counts per error type", []string{statsLabelCluster, statsLabelNamespace, statsLabelOperation})
	//	s.sqlFingerprintErrorCounts = stats.NewCountersWithMultiLabels("SqlFingerprintErrorCounts",
	//		"gaea proxy sql fingerprint error counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelFingerprint})
	//	s.sqlForbidenCounts = stats.NewCountersWithMultiLabels("SqlForbiddenCounts",
	//		"gaea proxy sql error counts per error type", []string{statsLabelCluster, statsLabelNamespace, statsLabelFingerprint})
	//	s.flowCounts = stats.NewCountersWithMultiLabels("FlowCounts",
	//		"gaea proxy flow counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelFlowDirection})
	//	s.sessionCounts = stats.NewGaugesWithMultiLabels("SessionCounts",
	//		"gaea proxy session counts", []string{statsLabelCluster, statsLabelNamespace})
	//
	//	s.backendSQLTimings = stats.NewMultiTimings("BackendSqlTimings",
	//		"gaea proxy backend sql sqlTimings", []string{statsLabelCluster, statsLabelNamespace, statsLabelOperation})
	//	s.backendSQLFingerprintSlowCounts = stats.NewCountersWithMultiLabels("BackendSqlFingerprintSlowCounts",
	//		"gaea proxy backend sql fingerprint slow counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelFingerprint})
	//	s.backendSQLErrorCounts = stats.NewCountersWithMultiLabels("BackendSqlErrorCounts",
	//		"gaea proxy backend sql error counts per error type", []string{statsLabelCluster, statsLabelNamespace, statsLabelOperation})
	//	s.backendSQLFingerprintErrorCounts = stats.NewCountersWithMultiLabels("BackendSqlFingerprintErrorCounts",
	//		"gaea proxy backend sql fingerprint error counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelFingerprint})
	//	s.backendConnectPoolIdleCounts = stats.NewGaugesWithMultiLabels("backendConnectPoolIdleCounts",
	//		"gaea proxy backend idle connect counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelSlice, statsLabelIPAddr})
	//	s.backendConnectPoolInUseCounts = stats.NewGaugesWithMultiLabels("backendConnectPoolInUseCounts",
	//		"gaea proxy backend in-use connect counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelSlice, statsLabelIPAddr})
	//	s.backendConnectPoolWaitCounts = stats.NewGaugesWithMultiLabels("backendConnectPoolWaitCounts",
	//		"gaea proxy backend wait connect counts", []string{statsLabelCluster, statsLabelNamespace, statsLabelSlice, statsLabelIPAddr})

	s.startClearTask()
	return nil
}

// Close close proxy stats
func (s *StatisticManager) Close() {
	close(s.closeChan)
}

// GetHandlers return specific handler of stats
func (s *StatisticManager) GetHandlers() map[string]http.Handler {
	return s.handlers
}

func (s *StatisticManager) initBackend() error {
	prometheus.Init()
	s.handlers = prometheus.GetHandlers()
	return nil
}

// clear data to prevent
func (s *StatisticManager) startClearTask() {
	go func() {
		t := time.NewTicker(time.Hour)
		for {
			select {
			case <-s.closeChan:
				return
			case <-t.C:
				s.clearLargeCounters()
			}
		}
	}()
}

func (s *StatisticManager) clearLargeCounters() {
	//	s.sqlErrorCounts.ResetAll()
	//	s.sqlFingerprintSlowCounts.ResetAll()
	//	s.sqlFingerprintErrorCounts.ResetAll()
	//
	//	s.backendSQLErrorCounts.ResetAll()
	//	s.backendSQLFingerprintSlowCounts.ResetAll()
	//	s.backendSQLFingerprintErrorCounts.ResetAll()
}

//func (s *StatisticManager) recordSessionSlowSQLFingerprint(namespace string, md5 string) {
//	fingerprintStatsKey := []string{s.clusterName, namespace, md5}
//	s.sqlFingerprintSlowCounts.Add(fingerprintStatsKey, 1)
//}
//
//func (s *StatisticManager) recordSessionErrorSQLFingerprint(namespace string, operation string, md5 string) {
//	fingerprintStatsKey := []string{s.clusterName, namespace, md5}
//	operationStatsKey := []string{s.clusterName, namespace, operation}
//	s.sqlErrorCounts.Add(operationStatsKey, 1)
//	s.sqlFingerprintErrorCounts.Add(fingerprintStatsKey, 1)
//}

func (s *StatisticManager) recordSessionSQLTiming(operation string, startTime time.Time) {
	operationStatsKey := []string{operation}
	s.sqlTimings.Record(operationStatsKey, startTime)
}

// millisecond duration
//func (s *StatisticManager) isBackendSlowSQL(startTime time.Time) bool {
//	duration := time.Since(startTime).Nanoseconds() / int64(time.Millisecond)
//	return duration > s.slowSQLTime || s.slowSQLTime == 0
//}
//
//func (s *StatisticManager) recordBackendSlowSQLFingerprint(namespace string, md5 string) {
//	fingerprintStatsKey := []string{s.clusterName, namespace, md5}
//	s.backendSQLFingerprintSlowCounts.Add(fingerprintStatsKey, 1)
//}
//
//func (s *StatisticManager) recordBackendErrorSQLFingerprint(namespace string, operation string, md5 string) {
//	fingerprintStatsKey := []string{s.clusterName, namespace, md5}
//	operationStatsKey := []string{s.clusterName, namespace, operation}
//	s.backendSQLErrorCounts.Add(operationStatsKey, 1)
//	s.backendSQLFingerprintErrorCounts.Add(fingerprintStatsKey, 1)
//}
//
//func (s *StatisticManager) recordBackendSQLTiming(namespace string, operation string, startTime time.Time) {
//	operationStatsKey := []string{s.clusterName, namespace, operation}
//	s.backendSQLTimings.Record(operationStatsKey, startTime)
//}
//
//// RecordSQLForbidden record forbidden sql
//func (s *StatisticManager) RecordSQLForbidden(fingerprint, namespace string) {
//	md5 := mysql.GetMd5(fingerprint)
//	s.sqlForbidenCounts.Add([]string{s.clusterName, namespace, md5}, 1)
//}
//
//// IncrSessionCount incr session count
//func (s *StatisticManager) IncrSessionCount(namespace string) {
//	statsKey := []string{s.clusterName, namespace}
//	s.sessionCounts.Add(statsKey, 1)
//}
//
//// DescSessionCount decr session count
//func (s *StatisticManager) DescSessionCount(namespace string) {
//	statsKey := []string{s.clusterName, namespace}
//	s.sessionCounts.Add(statsKey, -1)
//}
//
//// AddReadFlowCount add read flow count
//func (s *StatisticManager) AddReadFlowCount(namespace string, byteCount int) {
//	statsKey := []string{s.clusterName, namespace, "read"}
//	s.flowCounts.Add(statsKey, int64(byteCount))
//}
//
//// AddWriteFlowCount add write flow count
//func (s *StatisticManager) AddWriteFlowCount(namespace string, byteCount int) {
//	statsKey := []string{s.clusterName, namespace, "write"}
//	s.flowCounts.Add(statsKey, int64(byteCount))
//}
//
////record idle connect count
//func (s *StatisticManager) recordConnectPoolIdleCount(namespace string, slice string, addr string, count int64) {
//	statsKey := []string{s.clusterName, namespace, slice, addr}
//	s.backendConnectPoolIdleCounts.Set(statsKey, count)
//}
//
////record in-use connect count
//func (s *StatisticManager) recordConnectPoolInuseCount(namespace string, slice string, addr string, count int64) {
//	statsKey := []string{s.clusterName, namespace, slice, addr}
//	s.backendConnectPoolInUseCounts.Set(statsKey, count)
//}
//
////record wait queue length
//func (s *StatisticManager) recordConnectPoolWaitCount(namespace string, slice string, addr string, count int64) {
//	statsKey := []string{s.clusterName, namespace, slice, addr}
//	s.backendConnectPoolWaitCounts.Set(statsKey, count)
//}
