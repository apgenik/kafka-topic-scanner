package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type KafkaTopic struct {
	Cluster   string `json:"cluster"`
	Bootstrap string `json:"bootstrap"`
	Type      string `json:"type"`  // producer / consumer
	Topic     string `json:"topic"` // topic name
	Path      string `json:"path"`  // where found
}

var results []KafkaTopic

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: kafka-scan <yamlfile>")
		return
	}

	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	var root interface{}

	if err := yaml.Unmarshal(data, &root); err != nil {
		panic(err)
	}

	scanNode(root, "root", "", "")

	out, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(out))
}

func scanNode(node interface{}, path string, cluster string, bootstrap string) {
	switch v := node.(type) {

	case map[string]interface{}:
		// detect bootstrap
		for key, val := range v {
			lkey := strings.ToLower(key)
			if lkey == "bootstrap" || lkey == "bootstrap-servers" {
				if s, ok := val.(string); ok {
					bootstrap = s
				}
			}
		}

		// recursive scan
		for key, val := range v {
			lkey := strings.ToLower(key)

			// detect cluster name
			newCluster := cluster
			if cluster == "" {
				newCluster = key
			}

			// detect producers
			if strings.Contains(lkey, "producer") {
				extractTopics(val, newCluster, bootstrap, "producer", path+"."+key)
			}

			// detect consumers
			if strings.Contains(lkey, "consumer") {
				extractTopics(val, newCluster, bootstrap, "consumer", path+"."+key)
			}

			// recursive
			scanNode(val, path+"."+key, newCluster, bootstrap)
		}

	case []interface{}:
		for i, item := range v {
			scanNode(item, fmt.Sprintf("%s[%d]", path, i), cluster, bootstrap)
		}
	}
}

func extractTopics(node interface{}, cluster string, bootstrap string, kind string, path string) {
	switch v := node.(type) {

	case map[string]interface{}:
		for key, val := range v {

			// topic mapping: key: topicName
			if s, ok := val.(string); ok {
				results = append(results, KafkaTopic{
					Cluster:   cluster,
					Bootstrap: bootstrap,
					Type:      kind,
					Topic:     s,
					Path:      path + "." + key,
				})
			}

			// recursion
			extractTopics(val, cluster, bootstrap, kind, path+"."+key)
		}

	case []interface{}:
		for i, item := range v {
			if s, ok := item.(string); ok {
				// list of topic names
				results = append(results, KafkaTopic{
					Cluster:   cluster,
					Bootstrap: bootstrap,
					Type:      kind,
					Topic:     s,
					Path:      fmt.Sprintf("%s[%d]", path, i),
				})
			} else {
				extractTopics(item, cluster, bootstrap, kind, fmt.Sprintf("%s[%d]", path, i))
			}
		}
	}
}
