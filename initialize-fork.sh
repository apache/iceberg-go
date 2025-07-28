#!/bin/zsh

if [ ! -d "./go-cloud" ]; then
    # Update the version if it gets updated upstream
    git subtree add --prefix=go-cloud git@github.com:google/go-cloud.git v0.43.0
    # Remove the go module definition to make go-cloud dev a sub-package
    git rm go-cloud/go.*
fi

# Allow iceberg-go to compile by pointing the self-dependencies to this fork
go mod edit -replace github.com/apache/iceberg-go=.

# Replace all gocloud.dev import paths to the inlined subtree of the repo (effectivement vendoring gocloud.dev)
go run github.com/sirkon/go-imports-rename@latest --save 'gocloud.dev => github.com/apache/iceberg-go/go-cloud'

# Tidy up the dependencies
go mod tidy
