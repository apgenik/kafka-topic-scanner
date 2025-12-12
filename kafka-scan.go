// Package main реализует небольшую CLI-утилиту, которая сканирует YAML (или
// JSON) конфигурационные файлы в поиске имён Kafka-топиков и связанных
// метаданных, и записывает найденные топики в JSON-файл.
//
// Использование:
//
//	go run kafka-scan.go <input.yaml> [output.json]
//
// Сканер использует эвристики: ключи, содержащие "producer" или "consumer",
// считаются местами объявления топиков; ключи "bootstrap" или
// "bootstrap-servers" используются для определения bootstrap-серверов Kafka.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type KafkaTopic struct {
	// Cluster — предполагаемое имя кластера или ближайший ключ в YAML-структуре,
	// который группирует конфигурацию, где найден топик.
	Cluster string `json:"cluster"`
	// Bootstrap — адрес(а) bootstrap-серверов (значение ключа "bootstrap" или
	// "bootstrap-servers", если присутствует рядом с определением).
	Bootstrap string `json:"bootstrap"`
	// Type — "producer" или "consumer" в зависимости от контекста, где был
	// найден топик.
	Type string `json:"type"`
	// Topic — имя топика, найденное в конфигурации.
	Topic string `json:"topic"`
	// Path — путь в структуре YAML в точечной нотации (с индексами массивов),
	// указывающий местоположение найденного значения.
	Path string `json:"path"`
}

var results []KafkaTopic

// results накапливает найденные топики в процессе сканирования.

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: kafka-scan <yamlfile> [output.json]")
		return
	}

	inputFile := os.Args[1]
	outputFile := "kafka_topics.json"
	if len(os.Args) >= 3 {
		outputFile = os.Args[2]
	}

	data, err := os.ReadFile(inputFile)
	if err != nil {
		panic(err)
	}

	var root interface{}
	if err := yaml.Unmarshal(data, &root); err != nil {
		panic(err)
	}

	scanNode(root, "root", "", "")

	out, _ := json.MarshalIndent(results, "", "  ")

	// Сохраняем в файл
	if err := os.WriteFile(outputFile, out, 0644); err != nil {
		panic(err)
	}

	fmt.Printf("Kafka topics saved to %s\n", outputFile)
}

// ----------------------------------------------------

func scanNode(node interface{}, path string, cluster string, bootstrap string) {
	switch v := node.(type) {

	case map[string]interface{}:
		// обнаружение bootstrap
		for key, val := range v {
			lkey := strings.ToLower(key)
			if lkey == "bootstrap" || lkey == "bootstrap-servers" {
				if s, ok := val.(string); ok {
					bootstrap = s
				}
			}
		}

		// рекурсивное сканирование
		for key, val := range v {
			lkey := strings.ToLower(key)

			// detect cluster name
			// определение имени кластера
			newCluster := cluster
			if cluster == "" {
				newCluster = key
			}

			// detect producers
			// обнаружение producer'ов
			if strings.Contains(lkey, "producer") {
				extractTopics(val, newCluster, bootstrap, "producer", path+"."+key)
			}

			// detect consumers
			// обнаружение consumer'ов
			if strings.Contains(lkey, "consumer") {
				extractTopics(val, newCluster, bootstrap, "consumer", path+"."+key)
			}

			// рекурсивный вызов
			scanNode(val, path+"."+key, newCluster, bootstrap)
		}

	case []interface{}:
		for i, item := range v {
			scanNode(item, fmt.Sprintf("%s[%d]", path, i), cluster, bootstrap)
		}
	}
}

// ----------------------------------------------------

func extractTopics(node interface{}, cluster string, bootstrap string, kind string, path string) {
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
