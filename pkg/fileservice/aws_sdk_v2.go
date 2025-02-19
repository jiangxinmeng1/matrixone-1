// Copyright 2023 Matrix Origin
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

package fileservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"math"
	gotrace "runtime/trace"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

type AwsSDKv2 struct {
	name            string
	bucket          string
	client          *s3.Client
	perfCounterSets []*perfcounter.CounterSet
	listMaxKeys     int32
}

func NewAwsSDKv2(
	ctx context.Context,
	args ObjectStorageArguments,
	perfCounterSets []*perfcounter.CounterSet,
) (*AwsSDKv2, error) {

	if err := args.validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeoutCause(ctx, time.Minute, moerr.CauseNewAwsSDKv2)
	defer cancel()

	// options for loading configs
	loadConfigOptions := []func(*config.LoadOptions) error{
		config.WithLogger(logutil.GetS3Logger()),
		config.WithClientLogMode(
			aws.LogSigning |
				aws.LogRetries |
				aws.LogRequest |
				aws.LogResponse |
				aws.LogDeprecatedUsage |
				aws.LogRequestEventMessage |
				aws.LogResponseEventMessage,
		),
		config.WithHTTPClient(newHTTPClient(args)),
	}

	// shared config profile
	if args.SharedConfigProfile != "" {
		loadConfigOptions = append(loadConfigOptions,
			config.WithSharedConfigProfile(args.SharedConfigProfile),
		)
	}

	credentialProvider, err := args.credentialsProviderForAwsSDKv2(ctx)
	if err != nil {
		return nil, moerr.AttachCause(ctx, err)
	}

	// validate
	if credentialProvider != nil {
		_, err := credentialProvider.Retrieve(ctx)
		if err != nil {
			return nil, moerr.AttachCause(ctx, err)
		}
	}

	// load configs
	if credentialProvider != nil {
		loadConfigOptions = append(loadConfigOptions,
			config.WithCredentialsProvider(
				credentialProvider,
			),
		)
	}

	// http client
	httpClient := newHTTPClient(args)
	loadConfigOptions = append(loadConfigOptions, config.WithHTTPClient(httpClient))

	config, err := config.LoadDefaultConfig(ctx, loadConfigOptions...)
	if err != nil {
		return nil, moerr.AttachCause(ctx, err)
	}

	// options for s3 client
	s3Options := []func(*s3.Options){
		func(opts *s3.Options) {
			opts.Retryer = newAWSRetryer()
			opts.HTTPClient = httpClient
		},
	}

	// credential provider for s3 client
	if credentialProvider != nil {
		s3Options = append(s3Options,
			func(opt *s3.Options) {
				opt.Credentials = credentialProvider
			},
		)
	}

	// endpoint for s3 client
	if args.Endpoint != "" {
		s3Options = append(s3Options, func(opts *s3.Options) {
			opts.BaseEndpoint = aws.String(args.Endpoint)
		})
		if args.IsMinio {
			// special handling for MinIO
			s3Options = append(s3Options,
				func(opts *s3.Options) {
					opts.EndpointOptions.ResolvedRegion = args.Region
				},
			)
		}
	}

	// region for s3 client
	if args.Region != "" {
		s3Options = append(s3Options,
			func(opt *s3.Options) {
				opt.Region = args.Region
			},
		)
	}

	// new s3 client
	client := s3.NewFromConfig(
		config,
		s3Options...,
	)

	logutil.Info("new object storage",
		zap.Any("sdk", "aws v2"),
		zap.Any("arguments", args),
	)

	if !args.NoBucketValidation {
		// head bucket to validate
		_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: ptrTo(args.Bucket),
		})
		if err != nil {
			err = moerr.AttachCause(ctx, err)
			return nil, moerr.NewInternalErrorNoCtxf("bad s3 config: %v", err)
		}
	}

	return &AwsSDKv2{
		name:            args.Name,
		bucket:          args.Bucket,
		client:          client,
		perfCounterSets: perfCounterSets,
	}, nil

}

var _ ObjectStorage = new(AwsSDKv2)

func (a *AwsSDKv2) List(
	ctx context.Context,
	prefix string,
) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		select {
		case <-ctx.Done():
			yield(nil, ctx.Err())
			return
		default:
		}

		var marker *string

	loop1:
		for {
			output, err := a.listObjects(
				ctx,
				&s3.ListObjectsInput{
					Bucket:    ptrTo(a.bucket),
					Delimiter: ptrTo("/"),
					Prefix:    zeroToNil(prefix),
					Marker:    marker,
					MaxKeys:   zeroToNil(a.listMaxKeys),
				},
			)
			if err != nil {
				yield(nil, err)
				return
			}

			for _, obj := range output.Contents {
				if !yield(&DirEntry{
					Name: *obj.Key,
					Size: *obj.Size,
				}, nil) {
					break loop1
				}
			}

			for _, prefix := range output.CommonPrefixes {
				if !yield(&DirEntry{
					IsDir: true,
					Name:  *prefix.Prefix,
				}, nil) {
					break loop1
				}
			}

			if !*output.IsTruncated {
				break
			}
			marker = output.NextMarker
		}

	}
}

func (a *AwsSDKv2) Stat(
	ctx context.Context,
	key string,
) (
	size int64,
	err error,
) {

	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	default:
	}

	output, err := a.headObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket: ptrTo(a.bucket),
			Key:    ptrTo(key),
		},
	)
	if err != nil {
		var httpError *http.ResponseError
		if errors.As(err, &httpError) {
			if httpError.Response.StatusCode == 404 {
				err = moerr.NewFileNotFound(ctx, key)
				return
			}
		}
		return
	}

	size = *output.ContentLength

	return
}

func (a *AwsSDKv2) Exists(
	ctx context.Context,
	key string,
) (
	bool,
	error,
) {
	output, err := a.headObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket: ptrTo(a.bucket),
			Key:    ptrTo(key),
		},
	)
	if err != nil {
		var httpError *http.ResponseError
		if errors.As(err, &httpError) {
			if httpError.Response.StatusCode == 404 {
				return false, nil
			}
		}
		return false, err
	}
	return output != nil, nil
}

func (a *AwsSDKv2) Write(
	ctx context.Context,
	key string,
	r io.Reader,
	size int64,
	expire *time.Time,
) (
	err error,
) {

	_, err = a.putObject(
		ctx,
		&s3.PutObjectInput{
			Bucket:        ptrTo(a.bucket),
			Key:           ptrTo(key),
			Body:          r,
			ContentLength: zeroToNil(size),
			Expires:       expire,
		},
	)
	if err != nil {
		return err
	}

	return
}

func (a *AwsSDKv2) Read(
	ctx context.Context,
	key string,
	min *int64,
	max *int64,
) (
	r io.ReadCloser,
	err error,
) {

	if max == nil {
		// read to end
		r, err := a.getObject(
			ctx,
			min,
			nil,
			&s3.GetObjectInput{
				Bucket: ptrTo(a.bucket),
				Key:    ptrTo(key),
			},
		)
		err = a.mapError(err, key)
		if err != nil {
			return nil, err
		}
		return r, nil
	}

	r, err = a.getObject(
		ctx,
		min,
		max,
		&s3.GetObjectInput{
			Bucket: ptrTo(a.bucket),
			Key:    ptrTo(key),
		},
	)
	err = a.mapError(err, key)
	if err != nil {
		return nil, err
	}
	return &readCloser{
		r:         io.LimitReader(r, int64(*max-*min)),
		closeFunc: r.Close,
	}, nil
}

func (a *AwsSDKv2) Delete(
	ctx context.Context,
	keys ...string,
) (
	err error,
) {

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		return a.deleteSingle(ctx, keys[0])
	}

	objs := make([]types.ObjectIdentifier, 0, 1000)
	for _, key := range keys {
		objs = append(objs, types.ObjectIdentifier{Key: ptrTo(key)})
		if len(objs) == 1000 {
			if err := a.deleteMultiObj(ctx, objs); err != nil {
				return err
			}
			objs = objs[:0]
		}
	}
	if err := a.deleteMultiObj(ctx, objs); err != nil {
		return err
	}
	return nil
}

func (a *AwsSDKv2) deleteSingle(ctx context.Context, key string) error {
	ctx, span := trace.Start(ctx, "AwsSDKv2.deleteSingle")
	defer span.End()
	_, err := a.deleteObject(
		ctx,
		&s3.DeleteObjectInput{
			Bucket: ptrTo(a.bucket),
			Key:    ptrTo(key),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (a *AwsSDKv2) deleteMultiObj(ctx context.Context, objs []types.ObjectIdentifier) error {
	ctx, span := trace.Start(ctx, "AwsSDKv2.deleteMultiObj")
	defer span.End()
	output, err := a.deleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: ptrTo(a.bucket),
		Delete: &types.Delete{
			Objects: objs,
			// In quiet mode the response includes only keys where the delete action encountered an error.
			Quiet: ptrTo(true),
		},
	})
	// delete api failed
	if err != nil {
		return err
	}
	// delete api success, but with delete file failed.
	message := strings.Builder{}
	if len(output.Errors) > 0 {
		for _, Error := range output.Errors {
			if *Error.Code == (*types.NoSuchKey)(nil).ErrorCode() {
				continue
			}
			message.WriteString(fmt.Sprintf("%s: %s, %s;", *Error.Key, *Error.Code, *Error.Message))
		}
	}
	if message.Len() > 0 {
		return moerr.NewInternalErrorNoCtxf("S3 Delete failed: %s", message.String())
	}
	return nil
}

func (a *AwsSDKv2) listObjects(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv2.listObjects")
	defer task.End()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.List.Add(1)
	}, a.perfCounterSets...)
	return DoWithRetry(
		"s3 list objects",
		func() (*s3.ListObjectsOutput, error) {
			return a.client.ListObjects(ctx, params, optFns...)
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *AwsSDKv2) headObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv2.headObject")
	defer task.End()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Head.Add(1)
	}, a.perfCounterSets...)
	return DoWithRetry(
		"s3 head object",
		func() (*s3.HeadObjectOutput, error) {
			return a.client.HeadObject(ctx, params, optFns...)
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *AwsSDKv2) putObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv2.putObject")
	defer task.End()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Put.Add(1)
	}, a.perfCounterSets...)
	// not retryable because Reader may be half consumed
	return a.client.PutObject(ctx, params, optFns...)
}

func (a *AwsSDKv2) getObject(ctx context.Context, min *int64, max *int64, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (io.ReadCloser, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv2.getObject")
	defer task.End()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Get.Add(1)
	}, a.perfCounterSets...)
	if min == nil {
		min = ptrTo[int64](0)
	}
	r, err := newRetryableReader(
		func(offset int64) (io.ReadCloser, error) {
			LogEvent(ctx, str_retryable_reader_new_reader_begin, offset)
			defer LogEvent(ctx, str_retryable_reader_new_reader_end)
			var rang string
			if max != nil {
				rang = fmt.Sprintf("bytes=%d-%d", offset, *max)
			} else {
				rang = fmt.Sprintf("bytes=%d-", offset)
			}
			params.Range = &rang
			output, err := DoWithRetry(
				"s3 get object",
				func() (*s3.GetObjectOutput, error) {
					LogEvent(ctx, str_awssdkv2_get_object_begin)
					defer LogEvent(ctx, str_awssdkv2_get_object_end)
					return a.client.GetObject(ctx, params, optFns...)
				},
				maxRetryAttemps,
				IsRetryableError,
			)
			if err != nil {
				return nil, err
			}
			return output.Body, nil
		},
		*min,
		IsRetryableError,
	)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (a *AwsSDKv2) deleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv2.deleteObject")
	defer task.End()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Delete.Add(1)
	}, a.perfCounterSets...)
	return DoWithRetry(
		"s3 delete object",
		func() (*s3.DeleteObjectOutput, error) {
			return a.client.DeleteObject(ctx, params, optFns...)
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *AwsSDKv2) deleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv2.deleteObjects")
	defer task.End()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.DeleteMulti.Add(1)
	}, a.perfCounterSets...)
	return DoWithRetry(
		"s3 delete objects",
		func() (*s3.DeleteObjectsOutput, error) {
			return a.client.DeleteObjects(ctx, params, optFns...)
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *AwsSDKv2) mapError(err error, path string) error {
	if err == nil {
		return nil
	}
	var httpError *http.ResponseError
	if errors.As(err, &httpError) {
		if httpError.Response.StatusCode == 404 {
			return moerr.NewFileNotFoundNoCtx(path)
		}
	}
	return err
}

func (o ObjectStorageArguments) credentialsProviderForAwsSDKv2(
	ctx context.Context,
) (
	ret aws.CredentialsProvider,
	err error,
) {

	// cache
	defer func() {
		if ret != nil {
			ret = aws.NewCredentialsCache(ret)
		}
	}()

	defer func() {
		// handle assume role
		if o.RoleARN == "" {
			return
		}

		logutil.Info("setting assume role provider")
		// load default options
		awsConfig, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			panic(err)
		}
		if ret != nil {
			logutil.Info("using upstream credential provider for assume role",
				zap.Any("type", fmt.Sprintf("%T", ret)),
			)
			awsConfig.Credentials = ret
		}

		stsSvc := sts.NewFromConfig(awsConfig, func(options *sts.Options) {
			if o.Region == "" {
				options.Region = "ap-northeast-1"
			} else {
				options.Region = o.Region
			}
		})
		provider := stscreds.NewAssumeRoleProvider(
			stsSvc,
			o.RoleARN,
			func(opts *stscreds.AssumeRoleOptions) {
				if o.ExternalID != "" {
					opts.ExternalID = &o.ExternalID
				}
			},
		)
		_, err = provider.Retrieve(ctx)
		if err != nil {
			// not good
			logutil.Info("bad assume role provider",
				zap.Any("err", err),
			)
			return
		}

		// set to assume role provider
		ret = provider
	}()

	// static credential
	if o.KeyID != "" && o.KeySecret != "" {
		// static
		logutil.Info("static credential")
		return credentials.NewStaticCredentialsProvider(o.KeyID, o.KeySecret, o.SessionToken), nil
	}

	if !o.shouldLoadDefaultCredentials() {
		return nil, moerr.NewInvalidInputNoCtx(
			"no valid credentials",
		)
	}

	return
}

type awsRetryer struct {
	upstream aws.Retryer
}

func newAWSRetryer() aws.Retryer {
	retryer := aws.Retryer(retry.NewStandard(
		func(opts *retry.StandardOptions) {
			opts.RateLimiter = ratelimit.NewTokenRateLimit(math.MaxInt)
			opts.RetryCost = 1
			opts.RetryTimeoutCost = 1
		},
	))
	return &awsRetryer{
		upstream: retryer,
	}
}

var _ aws.Retryer = new(awsRetryer)

func (a *awsRetryer) GetInitialToken() (releaseToken func(error) error) {
	return a.upstream.GetInitialToken()
}

func (a *awsRetryer) GetRetryToken(ctx context.Context, opErr error) (releaseToken func(error) error, err error) {
	return a.upstream.GetRetryToken(ctx, opErr)
}

func (a *awsRetryer) IsErrorRetryable(err error) (ret bool) {
	defer func() {
		if ret {
			logutil.Info("file service retry",
				zap.Error(err),
			)
		}
	}()
	return a.upstream.IsErrorRetryable(err)
}

func (a *awsRetryer) MaxAttempts() int {
	return a.upstream.MaxAttempts()
}

func (a *awsRetryer) RetryDelay(attempt int, opErr error) (time.Duration, error) {
	return a.upstream.RetryDelay(attempt, opErr)
}
