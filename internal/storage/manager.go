package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
)

type Manager struct {
	dataDir string
	mu      sync.Mutex
	logs    map[string]*Log
}

func NewManager(dataDir string) *Manager {
	return &Manager{
		dataDir: dataDir,
		logs:    map[string]*Log{},
	}
}

func (m *Manager) Log(topic string, partition int32) (*Log, error) {
	key := logKey(topic, partition)

	m.mu.Lock()
	defer m.mu.Unlock()

	if log, ok := m.logs[key]; ok {
		return log, nil
	}

	log, err := Open(filepath.Join(m.dataDir, key))
	if err != nil {
		return nil, err
	}
	m.logs[key] = log
	return log, nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var err error
	for key, log := range m.logs {
		err = errors.Join(err, log.Close())
		delete(m.logs, key)
	}
	return err
}

func logKey(topic string, partition int32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}
