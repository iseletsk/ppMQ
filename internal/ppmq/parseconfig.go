package ppmq

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

// DefaultBatchSize to use if not specified in config
const DefaultBatchSize = 50

// Config for the ppMQ
type Config struct {
	ServerSocket  string                  `yaml:"ServerSocket"` // socket for MQ app, to send commands, messages & ack
	DataPath      string                  `yaml:"DataPath"`     // parent directory to store messages/queue/ack for each message topic
	Subscribers   []Subscriber            `yaml:"Subscribers"`  // list of subscribers
	Topics        []MessageTopic          `yaml:"Topics"`       // list of message topics
	subscriberMap map[string]Subscriber   // map of subscriber name to Subscriber struct
	topicMap      map[string]MessageTopic // map of topic names to MessageTopic struct
	factory       StorageFactory          // StorageFactory to use to persist MessageTopic
}

// Init configuration includes initializing MessageTopic
func (conf *Config) Init() error {
	conf.subscriberMap = make(map[string]Subscriber)
	conf.topicMap = make(map[string]MessageTopic)
	if conf.DataPath == "mem" {
		conf.factory = &MemoryFactory{}
	} else {
		conf.factory = &PersistentFactory{Path: conf.DataPath}
	}
	for i, c := range conf.Subscribers {
		c.Id = i
		if c.BatchSize == 0 {
			c.BatchSize = DefaultBatchSize
		}
		conf.subscriberMap[c.Name] = c
	}

	for _, q := range conf.Topics {
		if err := q.Init(conf.factory); err != nil {
			return err
		}
		conf.topicMap[q.Name] = q
		for _, c := range q.Subscriber {
			_, ok := conf.subscriberMap[c]
			if !ok {
				return fmt.Errorf("unknown subscriber: %s", c)
			}
		}
	}
	return nil
}

// loadConfig loads config from filename YAML file
func loadConfig(filename string) (config Config, err error) {
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}
	err = yaml.Unmarshal(file, &config)
	return
}

// LoadConfig is exported method that loads config from YAML file and initializes all underling structures
func LoadConfig(filename string) (config Config, err error) {
	config, err = loadConfig(filename)
	if err != nil {
		return
	}
	err = config.Init()
	return
}
