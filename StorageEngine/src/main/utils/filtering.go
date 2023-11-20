package utils

import (
	pb2 "FlyFlyDB/StorageEngine/src/main/utils/pb"
	"strconv"
	"strings"
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

func JoinRecordsMatchFilter(t1Name string, meta1 *pb2.TableMeta, r1 *pb2.Record,
	t2Name string, meta2 *pb2.TableMeta, r2 *pb2.Record, filters [][]string) bool {
	if len(filters) == 0 {
		return true
	}
	for _, filter := range filters {
		if strings.HasPrefix(filter[0], t1Name+".") && strings.HasPrefix(filter[2], t2Name+".") {
			t1FieldName := strings.Split(filter[0], ".")[1]
			t2FieldName := strings.Split(filter[2], ".")[1]
			t1FieldValue, success1 := GetFieldValue(meta1, r1, t1FieldName)
			t2FieldValue, success2 := GetFieldValue(meta2, r2, t2FieldName)
			if !success1 || !success2 || t1FieldValue != t2FieldValue {
				return false
			}
		} else if strings.HasPrefix(filter[0], t2Name+".") && strings.HasPrefix(filter[2], t1Name+".") {
			t1FieldName := strings.Split(filter[2], ".")[1]
			t2FieldName := strings.Split(filter[0], ".")[1]
			t1FieldValue, success1 := GetFieldValue(meta1, r1, t1FieldName)
			t2FieldValue, success2 := GetFieldValue(meta2, r2, t2FieldName)
			if !success1 || !success2 || t1FieldValue != t2FieldValue {
				return false
			}
		}
	}
	return true
}
