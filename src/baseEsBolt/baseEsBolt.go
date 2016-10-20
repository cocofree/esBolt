package baseEsBolt

import (
	"bufio"
	"fmt"
	"github.com/olivere/elastic"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

type BaseEsBolt struct {
	sync.Mutex
	reader    *bufio.Reader
	client    *elastic.Client
	logName   string
	processer BoltProcesser

	curBulkRequest *elastic.BulkService
}

type BoltProcesser interface {
	Process(line string) (map[string]interface{}, error)
}

const DefaultBulkSize = 1000
const DefaultCommitInterval = 10 * time.Second

func NewEsBolt(logName string, processer BoltProcesser) *BaseEsBolt {
	client, err := elastic.NewClient(elastic.SetURL("10.10.30.96", "10.10.30.97"))
	if err != nil {
		// Handle error
		panic(err)
	}
	esBolt := &BaseEsBolt{
		reader:         bufio.NewReader(os.Stdin),
		client:         client,
		logName:        logName,
		curBulkRequest: nil,
		processer:      processer,
	}
	return esBolt
}

func (this *BaseEsBolt) Run() {
	ticker := time.NewTicker(DefaultCommitInterval)
	defer ticker.Stop()
	go func() {
		for {
			<-ticker.C
			this.commit()
		}
	}()

	for {
		line, err := this.reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		this.process(line)
	}
}

func (this *BaseEsBolt) add(doc interface{}) {
	if this.curBulkRequest == nil {
		this.curBulkRequest = this.client.Bulk()
	}

	index := fmt.Sprintf("es-%v-%v", this.logName, time.Now().Format("20060102"))

	req := elastic.NewBulkIndexRequest().Index(index).Type("log").Doc(doc)

	this.curBulkRequest.Add(req)
	if this.curBulkRequest.NumberOfActions() >= DefaultBulkSize {
		this.commit()
	}
}

func (this *BaseEsBolt) commit() {
	tmpBulkRequest := this.curBulkRequest

	this.Lock()
	this.curBulkRequest = this.client.Bulk()
	this.Unlock()

	fmt.Println("--提交bulk:", tmpBulkRequest.NumberOfActions())
	_, err := tmpBulkRequest.Do()
	if err != nil {
		fmt.Println(err)
	}
}

func (this *BaseEsBolt) process(line string) {
	doc, err := this.processer.Process(line)
	if err != nil {
		return
	}
	this.add(doc)
}
