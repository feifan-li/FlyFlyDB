package utils

import (
	pb2 "FlyFlyDB/StorageEngine/src/main/utils/pb"
	"strconv"
)

func MatchesFilter(meta *pb2.TableMeta, record *pb2.Record, filters [][]string) bool {
	if record.DeleteMark {
		return false
	}
	if len(filters) == 0 {
		return true
	}
	for _, filter := range filters {
		if len(filter) != 3 {
			return false
		}

		fieldName, operator, value := filter[0], filter[1], filter[2]

		// Check if fieldName is valid and get the field value from the record
		fieldValue, valid := GetFieldValue(meta, record, fieldName)
		if !valid {
			return false
		}

		// Apply the filter based on the operator
		if !applyFilter(fieldValue, operator, value) {
			return false
		}
	}
	return true
}

func GetFieldValue(meta *pb2.TableMeta, record *pb2.Record, fieldName string) (string, bool) {
	if fieldName == meta.PartitionKeyName {
		return record.PartitionKeyValue, true
	} else if fieldName == meta.SortKeyName {
		return record.SortKeyValue, true
	}

	for i, name := range meta.OtherFieldsNames {
		if name == fieldName {
			return record.OtherFieldsValues[i], true
		}
	}

	return "", false // Field not found
}

func applyFilter(fieldValue, operator, value string) bool {
	switch operator {
	case "=", "is":
		return fieldValue == value
	case "!=", "is not":
		return fieldValue != value
	case ">", ">=", "<", "<=":
		return applyNumericFilter(fieldValue, operator, value)
	}

	return false
}

func applyNumericFilter(fieldValue, operator, value string) bool {
	// Convert string to float for comparison
	fieldValFloat, err1 := strconv.ParseFloat(fieldValue, 64)
	valueFloat, err2 := strconv.ParseFloat(value, 64)

	// If either value is not a valid float, return false
	if err1 != nil || err2 != nil {
		return false
	}

	switch operator {
	case ">":
		return fieldValFloat > valueFloat
	case ">=":
		return fieldValFloat >= valueFloat
	case "<":
		return fieldValFloat < valueFloat
	case "<=":
		return fieldValFloat <= valueFloat
	}

	return false
}
