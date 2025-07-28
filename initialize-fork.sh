#!/bin/zsh

if [ ! -d "./go-cloud" ]; then
    # Update the version if it gets updated upstream
    git subtree add --prefix=go-cloud git@github.com:google/go-cloud.git v0.43.0
    # Remove the go module definition to make go-cloud dev a sub-package
    git rm go-cloud/go.*
fi

# Delete some workflows because that don't apply to the fork
git rm .github/workflows/rc.yml
git rm .github/workflows/license_check.yml
git rm .github/workflows/go-release-docs.yml
git rm .github/workflows/labeler.yml
git rm .github/labeler.yml

# Rename module and update all self-referencing imports 
go mod edit -module github.com/DataDog/iceberg-go
go run github.com/sirkon/go-imports-rename@latest --save 'github.com/apache/iceberg-go => github.com/DataDog/iceberg-go'

# Apply a patch for a special link name
git apply ./initialize-fork.patch

# Replace all gocloud.dev import paths to the inlined subtree of the repo (effectivement vendoring gocloud.dev)
go run github.com/sirkon/go-imports-rename@latest --save 'gocloud.dev => github.com/DataDog/iceberg-go/go-cloud'

# Tidy up the dependencies
go mod tidy
