package io

import "strings"

func propertiesWithPrefix(props map[string]string, prefix string) map[string]string {
	result := map[string]string{}
	for k, v := range props {
		if strings.HasPrefix(k, prefix) {
			result[strings.TrimPrefix(k, prefix)] = v
		}
	}
	return result
}
