// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v4"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

const (
	defaultWriteSocketSize = 64 * 1024
)

// IsTempError returns a boolean value indicating whether the specified error
// is a temp error that worth to be retried, e.g. timeouts, temp network
// issues. Non-temp error caused by program logics rather than some external
// factors.
func IsTempError(err error) bool {
	return isTempError(err)
}

var NewLogRecord = pb.NewLogRecord
var NewUserLogRecord = pb.NewUserLogRecord

type ClientFactory func() (Client, error)

// Client is the Log Service Client interface exposed to the DN.
type Client interface {
	// Close closes the client.
	Close() error
	// Config returns the specified configuration when creating the client.
	Config() ClientConfig
	// UpdateLeaseholderID updates the leaseholder ID for the shard.
	UpdateLeaseholderID(ctx context.Context, leaseholderID uint64) error
	// GetLogRecord returns a new LogRecord instance with its Data field enough
	// to hold payloadLength bytes of payload. The layout of the Data field is
	// 4 bytes of record type (pb.UserEntryUpdate) + 8 bytes TN replica ID +
	// payloadLength bytes of actual payload.
	GetLogRecord(payloadLength int) pb.LogRecord
	// Append appends the specified LogRecord into the Log Service. On success, the
	// assigned Lsn will be returned. For the specified LogRecord, only its Data
	// field is used with all other fields ignored by Append(). Once returned, the
	// pb.LogRecord can be reused.
	Append(ctx context.Context, rec pb.LogRecord) (Lsn, error)
	// Read reads the Log Service from the specified Lsn position until the
	// returned LogRecord set reaches the specified maxSize in bytes. The returned
	// Lsn indicates the next Lsn to use to resume the read, or it means
	// everything available has been read when it equals to the specified Lsn.
	// The returned pb.LogRecord records will have their Lsn and Type fields set,
	// the Lsn field is the Lsn assigned to the record while the Type field tells
	// whether the record is an internal record generated by the Log Service itself
	// or appended by the user.
	Read(ctx context.Context, firstLsn Lsn, maxSize uint64) ([]pb.LogRecord, Lsn, error)
	// ReadLsn reads the LSN from the archived log files according to the specified
	// timestamp, return the log file's first lsn whose ts is less than the specified one.
	// If there are no log files found in the archived storage, an error is returned.
	ReadLsn(ctx context.Context, ts time.Time) (Lsn, error)
	// Truncate truncates the Log Service log at the specified Lsn with Lsn
	// itself included. This allows the Log Service to free up storage capacities
	// for future appends, all future reads must start after the specified Lsn
	// position.
	Truncate(ctx context.Context, lsn Lsn) error
	// GetTruncatedLsn returns the largest Lsn value that has been specified for
	// truncation.
	GetTruncatedLsn(ctx context.Context) (Lsn, error)
	// GetTSOTimestamp requests a total of count unique timestamps from the TSO and
	// return the first assigned such timestamp, that is TSO timestamps
	// [returned value, returned value + count] will be owned by the caller.
	GetTSOTimestamp(ctx context.Context, count uint64) (uint64, error)
	// GetLatestLsn returns the latest Lsn.
	GetLatestLsn(ctx context.Context) (Lsn, error)
	// SetRequiredLsn updates the required Lsn.
	SetRequiredLsn(ctx context.Context, lsn Lsn) error
	// GetRequiredLsn returns the required Lsn, which is required by other shards or
	// module. It is only a mark that other shards will read log entries from this value.
	// If the log entries after it are removed, other shards read data from S3 storage.
	GetRequiredLsn(ctx context.Context) (Lsn, error)
}

type StandbyClient interface {
	Client
	GetLeaderID(ctx context.Context) (uint64, error)
}

type managedClient struct {
	sid    string
	cfg    ClientConfig
	client *client
}

var _ Client = (*managedClient)(nil)
var _ StandbyClient = (*managedClient)(nil)

// NewClient creates a Log Service client. Each returned client can be used
// to synchronously issue requests to the Log Service. To send multiple requests
// to the Log Service in parallel, multiple clients should be created and used
// to do so.
func NewClient(
	ctx context.Context,
	sid string,
	cfg ClientConfig,
) (Client, error) {
	if err := cfg.ValidateClient(); err != nil {
		return nil, err
	}
	client, err := newClient(ctx, sid, cfg)
	if err != nil {
		return nil, err
	}
	return &managedClient{cfg: cfg, client: client, sid: sid}, nil
}

// NewStandbyClient creates a Log Service client which is used by standby shard.
func NewStandbyClient(ctx context.Context, sid string, cfg ClientConfig) (StandbyClient, error) {
	if err := cfg.ValidateStandbyClient(); err != nil {
		return nil, err
	}
	client, err := newClient(ctx, sid, cfg)
	if err != nil {
		return nil, err
	}
	return &managedClient{cfg: cfg, client: client, sid: sid}, nil
}

// NewStandbyClientWithRetry creates a Log Service client with retry.
func NewStandbyClientWithRetry(
	ctx context.Context, sid string, cfg ClientConfig,
) StandbyClient {
	var c StandbyClient
	createFn := func() error {
		ctx, cancel := context.WithTimeoutCause(ctx, time.Second*5, moerr.CauseNewStandbyClientWithRetry)
		defer cancel()
		lc, err := NewStandbyClient(ctx, sid, cfg)
		if err != nil {
			err = moerr.AttachCause(ctx, err)
			logutil.Errorf("failed to create logservice client: %v", err)
			return err
		}
		c = lc
		return nil
	}
	timer := time.NewTimer(time.Minute * 3)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil

		case <-timer.C:
			panic("failed to create logservice client")

		default:
			if err := createFn(); err != nil {
				time.Sleep(time.Second * 3)
				continue
			}
			return c
		}
	}
}

func (c *managedClient) Close() error {
	if c.client != nil {
		return c.client.close()
	}
	return nil
}

func (c *managedClient) Config() ClientConfig {
	return c.cfg
}

// UpdateLeaseholderID implements the Client interface.
func (c *managedClient) UpdateLeaseholderID(ctx context.Context, leaseholderID uint64) error {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return err
		}
		err := c.client.updateLeaseholderID(ctx, leaseholderID)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return err
	}
}

func (c *managedClient) GetLogRecord(payloadLength int) pb.LogRecord {
	return NewUserLogRecord(c.cfg.TNReplicaID, payloadLength)
}

func (c *managedClient) Append(ctx context.Context, rec pb.LogRecord) (Lsn, error) {
	start := time.Now()
	defer func() {
		v2.LogServiceAppendDurationHistogram.Observe(time.Since(start).Seconds())
		v2.LogServiceAppendCounter.Inc()
		v2.LogServiceAppendBytesHistogram.Observe(float64(len(rec.Data)))
	}()
	for {
		if err := c.prepareClient(ctx); err != nil {
			return 0, err
		}
		v, err := c.client.append(ctx, rec)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return v, err
	}
}

func (c *managedClient) Read(ctx context.Context,
	firstLsn Lsn, maxSize uint64) ([]pb.LogRecord, Lsn, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return nil, 0, err
		}
		recs, v, err := c.client.read(ctx, firstLsn, maxSize)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return recs, v, err
	}
}

func (c *managedClient) ReadLsn(ctx context.Context, ts time.Time) (Lsn, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return 0, err
		}
		v, err := c.client.readLsn(ctx, ts)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return v, err
	}
}

func (c *managedClient) Truncate(ctx context.Context, lsn Lsn) error {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return err
		}
		err := c.client.truncate(ctx, lsn)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return err
	}
}

func (c *managedClient) GetTruncatedLsn(ctx context.Context) (Lsn, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return 0, err
		}
		v, err := c.client.getTruncatedLsn(ctx)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return v, err
	}
}

func (c *managedClient) GetTSOTimestamp(ctx context.Context, count uint64) (uint64, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return 0, err
		}
		v, err := c.client.getTSOTimestamp(ctx, count)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return v, err
	}
}

func (c *managedClient) GetLatestLsn(ctx context.Context) (Lsn, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return 0, err
		}
		lsn, err := c.client.getLatestLsn(ctx)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return lsn, err
	}
}

func (c *managedClient) SetRequiredLsn(ctx context.Context, lsn Lsn) error {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return err
		}
		err := c.client.setRequiredLsn(ctx, lsn)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return err
	}
}

func (c *managedClient) GetRequiredLsn(ctx context.Context) (Lsn, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return 0, err
		}
		lsn, err := c.client.getRequiredLsn(ctx)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return lsn, err
	}
}

func (c *managedClient) GetLeaderID(ctx context.Context) (uint64, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return 0, err
		}
		leaderID, err := c.client.getLeaderID(ctx)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return leaderID, err
	}
}

func (c *managedClient) isRetryableError(err error) bool {
	/*
		old code, obviously strange
		if errors.Is(err, dragonboat.ErrTimeout) {
			return false
		}
		return errors.Is(err, dragonboat.ErrShardNotFound)
	*/

	// Dragonboat error leaked here
	if errors.Is(err, dragonboat.ErrShardNotFound) {
		return true
	}
	return moerr.IsMoErrCode(err, moerr.ErrDragonboatShardNotFound)
}

func (c *managedClient) resetClient() {
	if c.client != nil {
		cc := c.client
		c.client = nil
		if err := cc.close(); err != nil {
			logutil.Error("failed to close client", zap.Error(err))
		}
	}
}

func (c *managedClient) prepareClient(ctx context.Context) error {
	if c.client != nil {
		return nil
	}
	cc, err := newClient(ctx, c.sid, c.cfg)
	if err != nil {
		return err
	}
	c.client = cc
	return nil
}

type client struct {
	cfg      ClientConfig
	client   morpc.RPCClient
	addr     string
	pool     *sync.Pool
	respPool *sync.Pool
}

func newClient(
	ctx context.Context,
	sid string,
	cfg ClientConfig,
) (*client, error) {
	var c *client
	var err error
	// If the discovery address is configured, we used it first.
	if len(cfg.DiscoveryAddress) > 0 {
		c, err = connectToLogServiceByReverseProxy(ctx, sid, cfg.DiscoveryAddress, cfg)
		if c != nil && err == nil {
			return c, nil
		}
	} else if len(cfg.ServiceAddresses) > 0 {
		c, err = connectToLogService(ctx, sid, cfg.ServiceAddresses, cfg)
		if c != nil && err == nil {
			return c, nil
		}
	}
	if err != nil {
		return nil, err
	}
	return nil, moerr.NewLogServiceNotReady(ctx)
}

func connectToLogServiceByReverseProxy(
	ctx context.Context,
	sid string,
	discoveryAddress string,
	cfg ClientConfig,
) (*client, error) {
	si, ok, err := GetShardInfo(sid, discoveryAddress, cfg.LogShardID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, moerr.NewLogServiceNotReady(ctx)
	}
	addresses := make([]string, 0)
	leaderAddress, ok := si.Replicas[si.ReplicaID]
	if ok {
		addresses = append(addresses, leaderAddress)
	}
	for replicaID, address := range si.Replicas {
		if replicaID != si.ReplicaID {
			addresses = append(addresses, address)
		}
	}
	return connectToLogService(ctx, sid, addresses, cfg)
}

func connectToLogService(
	ctx context.Context,
	sid string,
	targets []string,
	cfg ClientConfig,
) (*client, error) {
	if len(targets) == 0 {
		return nil, nil
	}

	pool := &sync.Pool{}
	pool.New = func() interface{} {
		return &RPCRequest{pool: pool}
	}
	respPool := &sync.Pool{}
	respPool.New = func() interface{} {
		return &RPCResponse{pool: respPool}
	}
	c := &client{
		cfg:      cfg,
		pool:     pool,
		respPool: respPool,
	}
	var e error
	addresses := append([]string{}, targets...)
	rand.Shuffle(len(cfg.ServiceAddresses), func(i, j int) {
		addresses[i], addresses[j] = addresses[j], addresses[i]
	})
	for _, addr := range addresses {
		cc, err := getRPCClient(
			ctx,
			sid,
			addr,
			c.respPool,
			c.cfg.MaxMessageSize,
			cfg.EnableCompress,
			0,
			cfg.Tag,
		)
		if err != nil {
			e = err
			continue
		}
		c.addr = addr
		c.client = cc
		if cfg.ReadOnly {
			if err := c.connectReadOnly(ctx); err == nil {
				return c, nil
			} else {
				if err := c.close(); err != nil {
					logutil.Error("failed to close the client", zap.Error(err))
				}
				e = err
			}
		} else {
			// TODO: add a test to check whether it works when there is no truncated
			// LSN known to the logservice.
			if err := c.connectReadWrite(ctx); err == nil {
				return c, nil
			} else {
				if err := c.close(); err != nil {
					logutil.Error("failed to close the client", zap.Error(err))
				}
				e = err
			}
		}
	}
	return nil, e
}

func (c *client) close() error {
	return c.client.Close()
}

func (c *client) updateLeaseholderID(ctx context.Context, leaseholderID uint64) error {
	return c.doUpdateLeaseholderID(ctx, leaseholderID)
}

func (c *client) append(ctx context.Context, rec pb.LogRecord) (Lsn, error) {
	if c.readOnly() {
		return 0, moerr.NewInvalidInput(ctx, "incompatible client")
	}
	// TODO: check piggybacked hint on whether we are connected to the leader node
	return c.doAppend(ctx, rec)
}

func (c *client) read(ctx context.Context,
	firstLsn Lsn, maxSize uint64) ([]pb.LogRecord, Lsn, error) {
	return c.doRead(ctx, firstLsn, maxSize)
}

func (c *client) readLsn(ctx context.Context, ts time.Time) (Lsn, error) {
	return c.doReadLsn(ctx, ts)
}

func (c *client) truncate(ctx context.Context, lsn Lsn) error {
	if c.readOnly() {
		return moerr.NewInvalidInput(ctx, "incompatible client")
	}
	return c.doTruncate(ctx, lsn)
}

func (c *client) getTruncatedLsn(ctx context.Context) (Lsn, error) {
	return c.doGetTruncatedLsn(ctx)
}

func (c *client) getTSOTimestamp(ctx context.Context, count uint64) (uint64, error) {
	return c.tsoRequest(ctx, count)
}

func (c *client) getLatestLsn(ctx context.Context) (Lsn, error) {
	return c.doGetLatestLsn(ctx)
}

func (c *client) setRequiredLsn(ctx context.Context, lsn Lsn) error {
	return c.doSetRequiredLsn(ctx, lsn)
}

func (c *client) getRequiredLsn(ctx context.Context) (Lsn, error) {
	return c.doGetRequiredLsn(ctx)
}

func (c *client) getLeaderID(ctx context.Context) (uint64, error) {
	return c.doGetLeaderID(ctx)
}

func (c *client) readOnly() bool {
	return c.cfg.ReadOnly
}

func (c *client) connectReadWrite(ctx context.Context) error {
	if c.readOnly() {
		panic(moerr.NewInvalidInput(ctx, "incompatible client"))
	}
	return c.connect(ctx, pb.CONNECT)
}

func (c *client) connectReadOnly(ctx context.Context) error {
	return c.connect(ctx, pb.CONNECT_RO)
}

func (c *client) request(ctx context.Context,
	mt pb.MethodType, payload []byte, lsn Lsn,
	maxSize uint64, ts time.Time) (pb.Response, []pb.LogRecord, error) {
	ctx, span := trace.Debug(ctx, "client.request")
	defer span.End()
	tnID := c.cfg.TNReplicaID
	if mt == pb.UPDATE_LEASEHOLDER_ID {
		// when the method is pb.UPDATE_LEASEHOLDER_ID, the lsn is the leaseholder ID.
		tnID = lsn
	}
	req := pb.Request{
		Method: mt,
		LogRequest: pb.LogRequest{
			ShardID: c.cfg.LogShardID,
			TNID:    tnID,
			Lsn:     lsn,
			MaxSize: maxSize,
			TS:      ts,
		},
	}
	r := c.pool.Get().(*RPCRequest)
	defer r.Release()
	r.Request = req
	r.payload = payload
	future, err := c.client.Send(ctx, c.addr, r)
	if err != nil {
		return pb.Response{}, nil, err
	}
	defer future.Close()
	msg, err := future.Get()
	if err != nil {
		return pb.Response{}, nil, err
	}
	response, ok := msg.(*RPCResponse)
	if !ok {
		panic("unexpected response type")
	}
	resp := response.Response
	defer response.Release()
	var recs pb.LogRecordResponse
	if len(response.payload) > 0 {
		MustUnmarshal(&recs, response.payload)
	}
	err = toError(ctx, response.Response)
	if err != nil {
		return pb.Response{}, nil, err
	}
	return resp, recs.Records, nil
}

func (c *client) tsoRequest(ctx context.Context, count uint64) (uint64, error) {
	ctx, span := trace.Debug(ctx, "client.tsoRequest")
	defer span.End()
	req := pb.Request{
		Method: pb.TSO_UPDATE,
		TsoRequest: &pb.TsoRequest{
			Count: count,
		},
	}
	r := c.pool.Get().(*RPCRequest)
	r.Request = req
	future, err := c.client.Send(ctx, c.addr, r)
	if err != nil {
		return 0, err
	}
	defer future.Close()
	msg, err := future.Get()
	if err != nil {
		return 0, err
	}
	response, ok := msg.(*RPCResponse)
	if !ok {
		panic("unexpected response type")
	}
	resp := response.Response
	defer response.Release()
	err = toError(ctx, response.Response)
	if err != nil {
		return 0, err
	}
	return resp.TsoResponse.Value, nil
}

func (c *client) connect(ctx context.Context, mt pb.MethodType) error {
	_, _, err := c.request(ctx, mt, nil, 0, 0, time.Time{})
	return err
}

func (c *client) doUpdateLeaseholderID(ctx context.Context, leaseholderID uint64) error {
	_, _, err := c.request(ctx, pb.UPDATE_LEASEHOLDER_ID, nil, leaseholderID, 0, time.Time{})
	return err
}

func (c *client) doAppend(ctx context.Context, rec pb.LogRecord) (Lsn, error) {
	resp, _, err := c.request(ctx, pb.APPEND, rec.Data, 0, 0, time.Time{})
	if err != nil {
		return 0, err
	}
	return resp.LogResponse.Lsn, nil
}

func (c *client) doRead(ctx context.Context,
	firstLsn Lsn, maxSize uint64) ([]pb.LogRecord, Lsn, error) {
	resp, recs, err := c.request(ctx, pb.READ, nil, firstLsn, maxSize, time.Time{})
	if err != nil {
		return nil, 0, err
	}
	return recs, resp.LogResponse.LastLsn, nil
}

func (c *client) doReadLsn(ctx context.Context, ts time.Time) (Lsn, error) {
	resp, _, err := c.request(ctx, pb.READ_LSN, nil, 0, 0, ts)
	if err != nil {
		return 0, err
	}
	return resp.LogResponse.Lsn, nil
}

func (c *client) doTruncate(ctx context.Context, lsn Lsn) error {
	_, _, err := c.request(ctx, pb.TRUNCATE, nil, lsn, 0, time.Time{})
	return err
}

func (c *client) doGetTruncatedLsn(ctx context.Context) (Lsn, error) {
	resp, _, err := c.request(ctx, pb.GET_TRUNCATE, nil, 0, 0, time.Time{})
	if err != nil {
		return 0, err
	}
	return resp.LogResponse.Lsn, nil
}

func (c *client) doGetLatestLsn(ctx context.Context) (Lsn, error) {
	resp, _, err := c.request(ctx, pb.GET_LATEST_LSN, nil, 0, 0, time.Time{})
	if err != nil {
		return 0, err
	}
	return resp.LogResponse.Lsn, nil
}

func (c *client) doSetRequiredLsn(ctx context.Context, lsn Lsn) error {
	_, _, err := c.request(ctx, pb.SET_REQUIRED_LSN, nil, lsn, 0, time.Time{})
	return err
}

func (c *client) doGetRequiredLsn(ctx context.Context) (Lsn, error) {
	resp, _, err := c.request(ctx, pb.GET_REQUIRED_LSN, nil, 0, 0, time.Time{})
	if err != nil {
		return 0, err
	}
	return resp.LogResponse.Lsn, nil
}

func (c *client) doGetLeaderID(ctx context.Context) (Lsn, error) {
	resp, _, err := c.request(ctx, pb.GET_LEADER_ID, nil, 0, 0, time.Time{})
	if err != nil {
		return 0, err
	}
	return resp.LogResponse.LeaderID, nil
}

func getRPCClient(
	ctx context.Context,
	sid string,
	target string,
	pool *sync.Pool,
	maxMessageSize int,
	enableCompress bool,
	readTimeout time.Duration,
	tag ...string,
) (morpc.RPCClient, error) {
	mf := func() morpc.Message {
		return pool.Get().(*RPCResponse)
	}

	// construct morpc.BackendOption
	backendOpts := []morpc.BackendOption{
		morpc.WithBackendConnectTimeout(time.Second),
		morpc.WithBackendHasPayloadResponse(),
		morpc.WithBackendLogger(logutil.GetGlobalLogger().Named("hakeeper-client-backend")),
		morpc.WithBackendReadTimeout(readTimeout),
	}
	backendOpts = append(backendOpts, GetBackendOptions(ctx)...)

	// construct morpc.ClientOption
	clientOpts := []morpc.ClientOption{
		morpc.WithClientInitBackends([]string{target}, []int{1}),
		morpc.WithClientMaxBackendPerHost(1),
		morpc.WithClientLogger(logutil.GetGlobalLogger()),
	}
	clientOpts = append(clientOpts, GetClientOptions(ctx)...)

	var codecOpts []morpc.CodecOption
	codecOpts = append(codecOpts,
		morpc.WithCodecPayloadCopyBufferSize(defaultWriteSocketSize),
		morpc.WithCodecEnableChecksum(),
		morpc.WithCodecMaxBodySize(maxMessageSize))
	if enableCompress {
		codecOpts = append(codecOpts, morpc.WithCodecEnableCompress(malloc.GetDefault(nil)))
	}

	// we set connection timeout to a constant value so if ctx's deadline is much
	// larger, then we can ensure that all specified potential nodes have a chance
	// to be attempted
	codec := morpc.NewMessageCodec(sid, mf, codecOpts...)
	bf := morpc.NewGoettyBasedBackendFactory(codec, backendOpts...)
	return morpc.NewClient("logservice-client", bf, clientOpts...)
}
