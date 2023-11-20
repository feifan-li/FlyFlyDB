package utils

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"os"
)

func ReadProtobufFromBinaryFile(filename string, message proto.Message) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("cannot read binary data from file: %w", err)
	}
	err = proto.Unmarshal(data, message)
	if err != nil {
		return fmt.Errorf("cannot unmarshall: %w", err)
	}
	return nil
}
