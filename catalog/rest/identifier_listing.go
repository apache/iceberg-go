// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package rest

import (
	"context"
	"net/http"
	"net/url"
	"strconv"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

type pagedIdentifierResponse[T any] struct {
	Identifiers   []T    `json:"identifiers"`
	NextPageToken string `json:"next-page-token,omitempty"`
}

// paginationQueryParams constructs the query shared by all paged REST listings.
// Non-positive page sizes and empty tokens are intentionally omitted.
func paginationQueryParams(pageToken string, pageSize int) url.Values {
	params := url.Values{}
	if pageSize > 0 {
		params.Set("pageSize", strconv.Itoa(pageSize))
	}
	if pageToken != "" {
		params.Set("pageToken", pageToken)
	}

	return params
}

// listCatalogObjectsPage handles common REST listing behavior while keeping the
// table/view and function identifier wire shapes distinct through T.
func listCatalogObjectsPage[T any](
	r *Catalog, ctx context.Context, ep endpoint, namespace table.Identifier,
	pageToken string, pageSize int, convert func(T) table.Identifier,
) ([]table.Identifier, string, error) {
	if err := r.checkValidNamespace(namespace); err != nil {
		return nil, "", err
	}
	// Missing advertised listing endpoints produce an empty result.
	if !r.endpoints.allowed(ep) {
		return nil, "", nil
	}

	path, err := ep.reqPath(r.encodeNamespace(namespace))
	if err != nil {
		return nil, "", err
	}
	uri := r.baseURI.JoinPath(path...)
	uri.RawQuery = paginationQueryParams(pageToken, pageSize).Encode()

	rsp, err := doGet[pagedIdentifierResponse[T]](ctx, uri, nil, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace})
	if err != nil {
		return nil, "", err
	}

	out := make([]table.Identifier, len(rsp.Identifiers))
	for i, raw := range rsp.Identifiers {
		out[i] = convert(raw)
	}

	return out, rsp.NextPageToken, nil
}

func objectIdentifier(id identifier) table.Identifier {
	return append(id.Namespace, id.Name)
}

func functionIdentifier(levels []string) table.Identifier {
	return table.Identifier(levels)
}
