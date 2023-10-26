package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

func newJSONDatabase(filename string) *jsonDatabase {
	return &jsonDatabase{filename: filename}
}

type jsonDatabase struct {
	filename string
}

func (db *jsonDatabase) GetOffset(
	_ context.Context,
	topic string,
	partition int,
) (int64, error) {
	content, err := os.ReadFile(filepath.Clean(db.filename))
	if errors.Is(err, os.ErrNotExist) {
		return 0, sql.ErrNoRows
	}
	if err != nil {
		return 0, err
	}

	offsets := make(map[string]map[int]int64)
	err = json.Unmarshal(content, &offsets)
	if err != nil {
		return 0, err
	}

	if _, ok := offsets[topic]; !ok {
		return 0, sql.ErrNoRows
	}

	if _, ok := offsets[topic][partition]; !ok {
		return 0, sql.ErrNoRows
	}

	return offsets[topic][partition], nil
}

func (db *jsonDatabase) SaveOffset(
	_ context.Context,
	topic string,
	partition int,
	offset int64,
) error {
	offsets := make(map[string]map[int]int64)
	content, err := os.ReadFile(filepath.Clean(db.filename))
	if err == nil {
		_ = json.Unmarshal(content, &offsets)
	}

	if _, ok := offsets[topic]; !ok {
		offsets[topic] = make(map[int]int64)
	}
	offsets[topic][partition] = offset

	content, err = json.Marshal(offsets)
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Clean(db.filename), content, 0600)
}
