package agg

import (
	pb2 "FlyFlyDB/StorageEngine/src/main/utils/pb"
	"fmt"
	"math"
	"strconv"
)

type aggFuncType func(meta *pb2.TableMeta, records []pb2.Record, aggField string) (string, error)

var aggFuncMap = map[string]aggFuncType{
	"sum":   SumAggregation,
	"SUM":   SumAggregation,
	"count": CountAggregation,
	"COUNT": CountAggregation,
	"min":   MinAggregation,
	"MIN":   MinAggregation,
	"max":   MaxAggregation,
	"MAX":   MaxAggregation,
	"avg":   AvgAggregation,
	"AVG":   AvgAggregation}

func GroupAndAggregate(meta *pb2.TableMeta, records []pb2.Record, groupBy string, aggField string, aggFunc string) (
	map[string][]pb2.Record, map[string]string, error) {

	if groupBy == "" && aggFunc == "" && aggField == "" {
		return nil, nil, nil
	}

	groupedRecords := make(map[string][]pb2.Record)
	aggregatedResults := make(map[string]string)

	// Grouping records
	for _, record := range records {
		groupKey, err := createGroupKey(record, meta, groupBy)
		if err != nil {
			fmt.Printf("error creating group key: %v\n", err)
			return nil, nil, fmt.Errorf("error creating group key: %v", err)
		}
		groupedRecords[groupKey] = append(groupedRecords[groupKey], record)
	}
	// If no aggregation required
	if aggFunc == "" && aggField == "" {
		return groupedRecords, nil, nil
	}
	// Applying aggregation function
	for key, group := range groupedRecords {
		aggResult, err := aggFuncMap[aggFunc](meta, group, aggField)
		if err != nil {
			fmt.Printf("error applying aggregation function: %v", err)
			return nil, nil, fmt.Errorf("error applying aggregation function: %v", err)
		}
		aggregatedResults[key] = aggResult
	}

	return groupedRecords, aggregatedResults, nil
}
func createGroupKey(record pb2.Record, meta *pb2.TableMeta, groupBy string) (string, error) {
	// Extract the value of the groupBy field
	value, err := getFieldValueFromRecord(record, groupBy, meta)
	if err != nil {
		fmt.Printf("error extracting field value: %v\n", err)
		return "", fmt.Errorf("error extracting field value: %v", err)
	}

	// Use the field value as the group key
	return value, nil
}

func getFieldValueFromRecord(record pb2.Record, fieldName string, meta *pb2.TableMeta) (string, error) {
	// Check if the fieldName is a special field like Partition or Sort key, or in OtherFields
	switch fieldName {
	case meta.PartitionKeyName:
		return record.PartitionKeyValue, nil
	case meta.SortKeyName:
		return record.SortKeyValue, nil
	default:
		for i, name := range meta.OtherFieldsNames {
			if name == fieldName {
				if i < len(record.OtherFieldsValues) {
					return record.OtherFieldsValues[i], nil
				} else {
					fmt.Printf("index out of range for OtherFieldsValues")
					return "", fmt.Errorf("index out of range for OtherFieldsValues")
				}
			}
		}
	}
	fmt.Printf("field %s not found", fieldName)
	return "", fmt.Errorf("field %s not found", fieldName)
}

// SumAggregation is an aggregation function that computes the sum of values for a given field.
func SumAggregation(meta *pb2.TableMeta, records []pb2.Record, aggField string) (string, error) {
	sum := 0.0
	for _, record := range records {
		valueStr, err := getFieldValueFromRecord(record, aggField, meta)
		if err != nil {
			return "", fmt.Errorf("failed to get field value: %v", err)
		}

		value, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return "", fmt.Errorf("failed to parse field value as float: %v", err)
		}

		sum += value
	}

	return strconv.FormatFloat(sum, 'f', 2, 64), nil
}

// CountAggregation is an aggregation function that computes the count of values for a given field.
func CountAggregation(meta *pb2.TableMeta, records []pb2.Record, aggField string) (string, error) {
	var count int64
	count = int64(len(records))
	return strconv.FormatInt(count, 64), nil
}

// MinAggregation calculates the minimum value for the specified field.
func MinAggregation(meta *pb2.TableMeta, records []pb2.Record, aggField string) (string, error) {

	minValue := math.MaxFloat64
	for _, record := range records {
		valueStr, err := getFieldValueFromRecord(record, aggField, meta)
		if err != nil {
			return "", err
		}

		value, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return "", err
		}

		if value < minValue {
			minValue = value
		}
	}

	return strconv.FormatFloat(minValue, 'f', 2, 64), nil
}

// MaxAggregation calculates the maximum value for the specified field.
func MaxAggregation(meta *pb2.TableMeta, records []pb2.Record, aggField string) (string, error) {

	maxValue := -math.MaxFloat64
	for _, record := range records {
		valueStr, err := getFieldValueFromRecord(record, aggField, meta)
		if err != nil {
			return "", err
		}

		value, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return "", err
		}

		if value > maxValue {
			maxValue = value
		}
	}

	return strconv.FormatFloat(maxValue, 'f', 2, 64), nil
}

// AvgAggregation is an aggregation function that computes the avg of values for a given field.
func AvgAggregation(meta *pb2.TableMeta, records []pb2.Record, aggField string) (string, error) {

	sum := 0.0
	count := 0
	for _, record := range records {
		valueStr, err := getFieldValueFromRecord(record, aggField, meta)
		if err != nil {
			return "", fmt.Errorf("failed to get field value: %v", err)
		}

		value, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return "", fmt.Errorf("failed to parse field value as float: %v", err)
		}

		sum += value
		count += 1
	}

	return strconv.FormatFloat(sum/float64(count), 'f', 2, 64), nil
}
