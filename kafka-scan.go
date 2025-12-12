package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
		fmt.Println("Usage: kafka-scan <yamlfile> [output.json]")
		return
	}

	inputFile := os.Args[1]

	outputFile := autoOutputName(inputFile)
	if len(os.Args) >= 3 {
		outputFile = os.Args[2]
	}

	data, err := os.ReadFile(inputFile)
	if err != nil {
		fatal("Failed to read file: %v", err)
	}

	var root interface{}
	if err := yaml.Unmarshal(data, &root); err != nil {
		fatal("YAML Unmarshal error: %v", err)
	}

	scanNode(root, "root", "", "")

	writeJSON(outputFile)
}

// ----------------------------------------------------

func scanNode(node interface{}, path, cluster, bootstrap string) {
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

		// recursive handling
		for key, val := range v {
			lkey := strings.ToLower(key)

			newCluster := cluster
			if cluster == "" {
				newCluster = key
			}

			if strings.Contains(lkey, "producer") {
				extractTopics(val, newCluster, bootstrap, "producer", path+"."+key)
			}

			if strings.Contains(lkey, "consumer") {
				extractTopics(val, newCluster, bootstrap, "consumer", path+"."+key)
			}

			scanNode(val, path+"."+key, newCluster, bootstrap)
		}

	case []interface{}:
		for i, item := range v {
			scanNode(item, fmt.Sprintf("%s[%d]", path, i), cluster, bootstrap)
		}
	}
}

// ----------------------------------------------------

func extractTopics(node interface{}, cluster, bootstrap, kind, path string) {
	switch v := node.(type) {

	case map[string]interface{}:
		for key, val := range v {
			if s, ok := val.(string); ok {
				results = append(results, KafkaTopic{
					Cluster:   cluster,
					Bootstrap: bootstrap,
					Type:      kind,
					Topic:     s,
					Path:      path + "." + key,
				})
			} else {
				extractTopics(val, cluster, bootstrap, kind, path+"."+key)
			}
		}

	case []interface{}:
		for i, item := range v {
			if s, ok := item.(string); ok {
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

// ----------------------------------------------------
// Save JSON
// ----------------------------------------------------

func writeJSON(outputFile string) {
	out, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		fatal("JSON marshal error: %v", err)
	}

	// ensure directories exist
	dir := filepath.Dir(outputFile)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fatal("Failed to create directory %s: %v", dir, err)
		}
	}

	if err := os.WriteFile(outputFile, out, 0644); err != nil {
		fatal("Failed to write output file: %v", err)
	}

	fmt.Printf("✔ Kafka topics saved to: %s\n", outputFile)
	fmt.Printf("✔ Total topics found: %d\n", len(results))
}

// ----------------------------------------------------
// Utils
// ----------------------------------------------------

func fatal(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	fmt.Printf("❌ %s\n", msg)
	os.Exit(1)
}

func autoOutputName(input string) string {
	base := filepath.Base(input)
	clean := strings.TrimSuffix(base, filepath.Ext(base))
	return clean + "_kafka_topics.json"
}
