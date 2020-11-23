package ppmq

import (
	"fmt"
	"log"
	"os"
	"testing"
)

var TopicFileScan = "filescan"
var SubscriberAibolit = "rt-aibolit"

func NewBasicMessage(key MessageKey, p MessagePayload) Message {
	return Message{Key: key, Payload: p, Priority: 0}
}

// initEngine used as a convenience function for tests to init engine from ppmq_test_config
func initEngine(qName string, mCount int) *EngineImpl {
	conf, err := LoadConfig("../../configs/ppmq_test_config.yaml")
	if err != nil {
		log.Fatal("unable to load config", err)
	}

	engine := &EngineImpl{}
	if err := engine.Init(conf.topicMap[qName], &MemoryFactory{}); err != nil {
		log.Fatal(err)
	}
	for i := 1; i <= mCount; i++ {
		s := fmt.Sprintf("%d", i)
		err := engine.Add([]Message{
			NewBasicMessage(MessageKey(s), MessagePayload("Message "+s))},
		)
		if err != nil {
			log.Fatal(err)
		}
	}
	return engine
}

func TestMemoryEngine_Expire(t *testing.T) {
	cqName := SubscriberAibolit
	engine := initEngine(TopicFileScan, 3)
	if e := engine.TestState(cqName, []MessageKey{"1", "2", "3"}, []AckPair{}); e != nil {
		t.Error(e)
		return
	}

}

func TestMemoryEngine_Init(t *testing.T) {
	cqName := SubscriberAibolit

	engine := initEngine(TopicFileScan, 3)
	if e := engine.TestState(cqName, []MessageKey{"1", "2", "3"}, []AckPair{}); e != nil {
		t.Error(e)
		return
	}
}

func TestMemoryEngine_ResetAck(t *testing.T) {
	cqName := SubscriberAibolit
	engine := initEngine(TopicFileScan, 3)

	if _, e := engine.NextBatch(cqName, 1); e != nil {
		t.Error(e)
		return
	}

	if e := engine.ResetAck(cqName); e != nil {
		t.Error(e)
		return
	}

	if e := engine.TestState(cqName, []MessageKey{"1", "2", "3"}, []AckPair{}); e != nil {
		t.Error(e)
		return
	}

}

type QueueTestArgs struct {
	name  string
	mq    MessageTopic
	setup string
	state string
}

var basicTest = []QueueTestArgs{
	{"Basic Topic 1", testBasicQueue, "m0:0.1.2.3.4.5.6.7.8.9,r0:5:0.1.2.3.4,a0:1.2.3", "0:5.6.7.8.9:0.4"},
	{"Basic Topic 2", testBasicQueue, "m0:0.1.2.3.4.5.6.7.8.9,r0:5:0.1.2.3.4,a0:1.2.3,a0:0.4,r0:10:5.6.7.8.9", "0:.:5.6.7.8.9"},
	{"Basic Topic 3", testBasicQueue, "m0:0.1.2.3.4,a0:1.2,m0:5.6.7,r0:10:0.1.2.3.4.5.6.7,a0:0.2.1.3.4.5.6.7:r0:10:", "0:.:."},
}
var priorityTests = []QueueTestArgs{
	{"Priority Topic 1", testPriorityMultiCQueue,
		"m1:0.1.2.3,m0:4.5.6,r0:5:4.5.6.0.1,a0:4.5.6.0,r1:2:4.5,a1:4,m0:7.8,r0:5:7.8.2.3,r1:5:6.7.8.0.1,a1:5.6",
		"0:.:1.7.8.2.3|1:2.3:7.8.0.1"},
}
var dedupeTests = []QueueTestArgs{
	{"Dedupe Priority Topic 1", testDedupePriorityMultiQueue,
		"m1:0.1,m0:2.4,r0:3:2.4.0,m0:3,a0:2,m1:2.3,m0:0",
		"0:3.0.1.2:4|1:2.4.3.0.1:."},
}

var expiringTests = []QueueTestArgs{
	{"Expired Message Priority Topic 1", testDedupePriorityMultiQueue,
		"m1:0.1,m0:2.4+5s,c+2s,r0:3:2.4.0,c+8s,r1:3:2.0.1",
		"0:1:2.0|1:.:2.0.1"},
}

func TestMemory_QueueTestsBasic(t *testing.T) {
	QueueTests(t, &EngineImpl{}, &MemoryFactory{}, "mem: ", "", basicTest)
}

func TestMemory_QueueTestsPriority(t *testing.T) {
	QueueTests(t, &EngineImpl{}, &MemoryFactory{}, "mem: ", "", priorityTests)
}

func TestMemory_QueueTestsDedupe(t *testing.T) {
	QueueTests(t, &EngineImpl{}, &MemoryFactory{}, "mem: ", "", dedupeTests)
}

func TestMemory_QueueTestsExpire(t *testing.T) {
	QueueTests(t, &EngineImpl{}, &MemoryFactory{}, "mem: ", "", expiringTests)
}

var testDir = "/tmp/imunify/test/store"

func TestLevelDB_QueueTestsBasic(t *testing.T) {
	QueueTests(t, &EngineImpl{}, &PersistentFactory{Path: testDir}, "disk: ", testDir, basicTest)
	_ = os.RemoveAll(testDir)
}

func TestLevelDB_QueueTestsPriority(t *testing.T) {
	QueueTests(t, &EngineImpl{}, &PersistentFactory{Path: testDir}, "disk: ", testDir, priorityTests)
	_ = os.RemoveAll(testDir)
}

func TestLevelDB_QueueTestsDedupe(t *testing.T) {
	QueueTests(t, &EngineImpl{}, &PersistentFactory{Path: testDir}, "disk: ", testDir, dedupeTests)
	_ = os.RemoveAll(testDir)
}

func TestLevelDB_QueueTestsExpire(t *testing.T) {
	QueueTests(t, &EngineImpl{}, &PersistentFactory{Path: testDir}, "disk: ", testDir, expiringTests)
	_ = os.RemoveAll(testDir)
}

func TestLevelDB_Restart(t *testing.T) {
	t.Run("LevelDB Restart", func(t *testing.T) {
		_ = os.RemoveAll(testDir)
		engine := &EngineImpl{}
		factory := &PersistentFactory{Path: testDir}
		if err := engine.Init(testDedupePriorityMultiQueue, factory); err != nil {
			t.Error(err)
		}
		setup1 := "m1:0.1,m0:2.4+5s,c+2s,r0:3:2.4.0"
		setup2 := "c+8s,r1:3:2.0.1"
		state := "0:1:2.0|1:.:2.0.1"
		if err := processSetup(engine, setup1); err != nil {
			t.Error(err)
		}
		// Let's shut down engine now (emitating software shutdown)
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
		// And re-initialize everything a new.
		engine = &EngineImpl{}
		factory = &PersistentFactory{Path: testDir}
		if err := engine.Init(testDedupePriorityMultiQueue, factory); err != nil {
			t.Error(err)
		}
		if err := processSetupAndState(engine, setup2, state); err != nil {
			t.Error(err)
		}
		if err := engine.Close(); err != nil {
			t.Errorf("Unable to close engine %v", err)
		}
	})
}
