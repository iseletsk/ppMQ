package ppmq

import (
	"fmt"
	"reflect"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	expectedConfig := Config{
		ServerSocket: "/tmp/imunify/mq.sock",
		DataPath:     "mem",
		Subscribers: []Subscriber{
			{Id: 0, Name: "rt-aibolit", BatchSize: 100, Socket: "/tmp/imunify/rt-aibolit.sock"},
			{Id: 0, Name: "agent", BatchSize: 10, Socket: "/tmp/imunify/agent.sock"},
			{Id: 0, Name: "iptables-ips", BatchSize: 0, Socket: "/tmp/imunify/iptables-ips.sock"},
			{Id: 0, Name: "sharedmem-ips", BatchSize: 0, Socket: "/tmp/imunify/pam-ips.sock"}},
		Topics: []MessageTopic{
			{Name: "filescan", AckRetry: 300000000000, Dedupe: true, Priorities: 2, Expire: true, Subscriber: []string{"rt-aibolit"}},
			{Name: "ips", AckRetry: 180000000000, Dedupe: false, Priorities: 1, Expire: true, Subscriber: []string{"iptables-ips", "sharedmem-ips"}}}}

	type args struct {
		filename string
	}
	tests := []struct {
		name       string
		args       args
		wantConfig Config
		wantErr    bool
	}{
		{"Test Config", args{"../../configs/ppmq_test_config.yaml"}, expectedConfig, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotConfig, err := loadConfig(tt.args.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("loadConfig() error = %v, wantErr %v", err, tt.wantErr)

				return
			}
			if !reflect.DeepEqual(gotConfig, tt.wantConfig) {
				t.Errorf("loadConfig() gotConfig = %#v, want %#v", gotConfig, tt.wantConfig)
				fmt.Printf("%#v", gotConfig)
			}
		})
	}
}

func (mq MessageTopic) testEqual(queue MessageTopic) bool {
	return reflect.DeepEqual(mq.Subscriber, queue.Subscriber) &&
		mq.Name == queue.Name &&
		mq.AckRetry == queue.AckRetry &&
		mq.Dedupe == queue.Dedupe &&
		mq.Priorities == queue.Priorities &&
		mq.Expire == queue.Expire
}

func TestConfig_Init(t *testing.T) {
	cMapExpected := map[string]Subscriber{
		"agent":         {Id: 1, Name: "agent", BatchSize: 10, Socket: "/tmp/imunify/agent.sock"},
		"iptables-ips":  {Id: 2, Name: "iptables-ips", BatchSize: 50, Socket: "/tmp/imunify/iptables-ips.sock"},
		"rt-aibolit":    {Id: 0, Name: "rt-aibolit", BatchSize: 100, Socket: "/tmp/imunify/rt-aibolit.sock"},
		"sharedmem-ips": {Id: 3, Name: "sharedmem-ips", BatchSize: 50, Socket: "/tmp/imunify/pam-ips.sock"}}

	qMapExpected := map[string]MessageTopic{
		"filescan": {Name: "filescan", AckRetry: 300000000000, Dedupe: true, Priorities: 2, Expire: true, Subscriber: []string{"rt-aibolit"}},
		"ips":      {Name: "ips", AckRetry: 180000000000, Dedupe: false, Priorities: 1, Expire: true, Subscriber: []string{"iptables-ips", "sharedmem-ips"}}}
	c1, err := loadConfig("../../configs/ppmq_test_config.yaml")
	if err != nil {
		t.Errorf("Error loading config %v", err)
	}
	err = c1.Init()

	if !reflect.DeepEqual(c1.subscriberMap, cMapExpected) {
		t.Errorf("SubscriberMap didn't correctly initalized: %#v", c1.subscriberMap)
	}
	if len(qMapExpected) != len(c1.topicMap) {
		t.Errorf("TopicMap didn't correctly initialize: %#v", c1.topicMap)
	} else {
		for k, v := range qMapExpected {
			if !v.testEqual(c1.topicMap[k]) {
				t.Errorf("TopicMap didn't correctly initialize: %#v", c1.topicMap[k])
				break
			}
		}
	}
	if err != nil {
		t.Errorf("Got unexecpted error: %v", err)
	}

	c1, err = loadConfig("../../configs/ppmq_test_config.yaml")
	if err != nil {
		t.Errorf("Error loading config %v", err)
	}
	c1.Topics[0].Subscriber = append(c1.Topics[0].Subscriber, "NoSuchSubscriber")
	err = c1.Init()
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}
