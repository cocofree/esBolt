package main

import (
	"baseEsBolt"
	"encoding/json"
	"fmt"
)

type ParseCdnLogBolt struct {
}

func (this *ParseCdnLogBolt) Process(line string) (map[string]interface{}, error) {
	var doc map[string]interface{}
	err := json.Unmarshal([]byte(line), &doc)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return doc, nil
}

func main() {
	var processer baseEsBolt.BoltProcesser = new(ParseCdnLogBolt)
	bolt := baseEsBolt.NewEsBolt("test-parse-cdn-log", processer)
	bolt.Run()
}
