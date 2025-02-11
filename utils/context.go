package utils

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type awsctxkey struct{}

func WithAwsConfig(ctx context.Context, cfg *aws.Config) context.Context {
	return context.WithValue(ctx, awsctxkey{}, cfg)
}

func GetAwsConfig(ctx context.Context) *aws.Config {
	if v := ctx.Value(awsctxkey{}); v != nil {
		return v.(*aws.Config)
	}
	return nil
}
