package dml

import (
	"FlyFlyDB/Parser/src/main/style"
	"FlyFlyDB/StorageEngine/src/main/agg"
	utils2 "FlyFlyDB/StorageEngine/src/main/utils"
	pb2 "FlyFlyDB/StorageEngine/src/main/utils/pb"
	"FlyFlyDB/globals"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

func SelectFromTable(tName string, projection []string, filters [][]string,
	groupBy string, aggField string, aggFunc string, sortBy string, limit string) (
	map[string][]pb2.Record, map[string]string, error) {

	if globals.WorkingDatabasePosition == "" {
		fmt.Printf("Please choose a database\n")
		return nil, nil, fmt.Errorf("Please choose a database\n")
	}

	/*read meta data*/
	tableDir := globals.WorkingDatabasePosition + "/" + tName
	meta := &pb2.TableMeta{}
	err := utils2.ReadProtobufFromBinaryFile(tableDir+"/"+tName+".meta", meta)
	if err != nil {
		fmt.Printf(err.Error())
		return nil, nil, fmt.Errorf("Cannot read meta data\n")
	}

	/*results should be filtered*/
	var results []pb2.Record
	var pid int64
	for pid = 1; pid <= meta.Partitions; pid++ {
		table := &pb2.Table{}
		err = utils2.ReadProtobufFromBinaryFile(tableDir+"/"+tName+"-"+strconv.FormatInt(pid, 10)+".data", table)
		if err != nil {
			fmt.Printf("Failed to read data for table %s, partition %d: %v\n", tName, pid, err)
			return nil, nil, fmt.Errorf("Failed to read data for table %s, partition %d: %v\n", tName, pid, err)
		}
		for _, record := range table.Records {
			if utils2.MatchesFilter(meta, record, filters) {
				results = append(results, *record)
			}
		}
	}
	// Group by partitionKey by default, and Aggregate
	var groupedRecords map[string][]pb2.Record
	var aggregatedResults map[string]string
	if groupBy == "" || len(groupBy) == 0 {
		groupedRecords, aggregatedResults, err = agg.GroupAndAggregate(meta, results, meta.PartitionKeyName, aggField, aggFunc)
		if err != nil {
			fmt.Printf("Failed to group and aggregate: %v\n", err)
			return nil, nil, fmt.Errorf("Failed to group and aggregate: %v\n", err)
		}
	} else {
		groupedRecords, aggregatedResults, err = agg.GroupAndAggregate(meta, results, groupBy, aggField, aggFunc)
		if err != nil {
			fmt.Printf("Failed to group and aggregate: %v\n", err)
			return nil, nil, fmt.Errorf("Failed to group and aggregate: %v\n", err)
		}
	}

	//TODO:Sort each group, sort by sortKey by default, otherwise use sortBy
	if sortBy == "" {
		if groupBy != meta.SortKeyName {
			for _, records := range groupedRecords {
				sortRecords(meta, records, meta.SortKeyName)
			}
		}
	} else {
		if groupBy != sortBy {
			for _, records := range groupedRecords {
				sortRecords(meta, records, sortBy)
			}
		}
	}

	style.RenderGroupedRecords(meta, groupBy, groupedRecords, projection, aggField, aggFunc, aggregatedResults, limit)
	return groupedRecords, aggregatedResults, nil
}

func sortRecords(meta *pb2.TableMeta, records []pb2.Record, sortBy string) error {
	isNumeric := false
	for i, name := range meta.OtherFieldsNames {
		if name == sortBy {
			if meta.OtherFieldsTypes[i] != "string" {
				isNumeric = true
			} else {
				isNumeric = false
			}
			break
		}
	}
	if meta.PartitionKeyName == sortBy {
		if meta.PartitionKeyType != "string" {
			isNumeric = true
		} else {
			isNumeric = false
		}
	}
	if meta.SortKeyName == sortBy {
		if meta.SortKeyType != "string" {
			isNumeric = true
		} else {
			isNumeric = false
		}
	}
	sort.Slice(records, func(i, j int) bool {
		valI, err := utils2.GetFieldValue(meta, &records[i], sortBy)
		if !err {
			return false
		}
		var valINumeric float64
		if isNumeric {
			valINumeric, _ = strconv.ParseFloat(valI, 64)
		}

		valJ, err := utils2.GetFieldValue(meta, &records[j], sortBy)
		if !err {
			return false
		}
		var valJNumeric float64
		if isNumeric {
			valJNumeric, _ = strconv.ParseFloat(valJ, 64)
		}
		if isNumeric {
			return valINumeric < valJNumeric
		}
		return valI < valJ
	})
	return nil
}

func InsertIntoTable(tName string, fieldAndValues [][]string) string {
	if globals.WorkingDatabasePosition == "" {
		return "Please choose a database"
	}
	// Check if the table directory exists
	tableDir := globals.WorkingDatabasePosition + "/" + tName
	if _, err := os.Stat(tableDir); os.IsNotExist(err) {
		return fmt.Sprintf("table %s does not exist", tName)
	}
	// Read the table's metadata
	meta := &pb2.TableMeta{}
	err := utils2.ReadProtobufFromBinaryFile(tableDir+"/"+tName+".meta", meta)
	if err != nil {
		return fmt.Sprintf("failed to read metadata for table %s: %v", tName, err)
	}
	// Create a new record
	record := &pb2.Record{
		DeleteMark:        false,
		OtherFieldsValues: make([]string, len(meta.OtherFieldsNames)),
	}
	// Map the field names to their values
	for _, fv := range fieldAndValues {
		if len(fv) != 2 {
			return "Invalid field and value pair"
		}
		fieldName, fieldValue := fv[0], fv[1]

		// Assign values to the record based on the metadata
		switch fieldName {
		case meta.PartitionKeyName:
			record.PartitionKeyValue = fieldValue
		case meta.SortKeyName:
			record.SortKeyValue = fieldValue
		default:
			// Find the index of the field in OtherFieldsNames
			index := -1
			for i, name := range meta.OtherFieldsNames {
				if name == fieldName {
					index = i
					break
				}
			}
			if index == -1 {
				return fmt.Sprintf("Field %s not found in table %s", fieldName, tName)
			}
			record.OtherFieldsValues[index] = fieldValue
		}
	}

	// Determine the partition ID for the record
	partitionId := utils2.GetPartitionId(record.PartitionKeyValue, meta.Partitions)
	if partitionId < 1 || partitionId > meta.Partitions {
		return "Invalid partition ID"
	}

	// Read the table's current data
	tableDataFile := fmt.Sprintf("%s/%s-%d.data", tableDir, tName, partitionId)
	table := &pb2.Table{}
	err = utils2.ReadProtobufFromBinaryFile(tableDataFile, table)
	if err != nil {
		return fmt.Sprintf("Failed to read data for table %s: %v", tName, err)
	}

	// Append the record to the table's data
	table.Records = append(table.Records, record)
	table.RecordsNumsAll = int64(len(table.Records))
	// Write the updated table data back to the file
	err = utils2.WriteProtobufToBinaryFile(table, tableDataFile)
	if err != nil {
		return fmt.Sprintf("Failed to write updated data to table %s: %v", tName, err)
	}

	return fmt.Sprintf("inserted 1 record")
}

func UpdateTable(tName string, filterFieldsOperationsValues [][]string, fieldNames []string, newValues []string) string {
	if len(fieldNames) == 0 || len(newValues) == 0 {
		return "No records updated\n"
	}
	if globals.WorkingDatabasePosition == "" {
		return "Please choose a database"
	}
	tableDir := globals.WorkingDatabasePosition + "/" + tName
	if _, err := os.Stat(tableDir); os.IsNotExist(err) {
		return "Table does not exist"
	}

	// Read table metadata
	meta := &pb2.TableMeta{}
	err := utils2.ReadProtobufFromBinaryFile(filepath.Join(tableDir, tName+".meta"), meta)
	if err != nil {
		return fmt.Sprintf("Failed to read metadata for table %s: %v", tName, err)
	}
	// Check if partition key is being updated
	isPartitionKeyUpdated := false
	for _, fieldName := range fieldNames {
		if fieldName == meta.PartitionKeyName {
			isPartitionKeyUpdated = true
			break
		}
	}
	var recordsNeedToReInsert []*pb2.Record
	for pid := int64(1); pid <= meta.Partitions; pid++ {
		partitionFile := filepath.Join(tableDir, fmt.Sprintf("%s-%d.data", tName, pid))
		table := &pb2.Table{}
		err = utils2.ReadProtobufFromBinaryFile(partitionFile, table)
		if err != nil {
			return fmt.Sprintf("Failed to read data for table %s, partition %d: %v", tName, pid, err)
		}

		for _, record := range table.Records {
			if utils2.MatchesFilter(meta, record, filterFieldsOperationsValues) {
				if isPartitionKeyUpdated {
					record.DeleteMark = true
					table.RecordsNumsPendingDel += 1
					updatedRecord := createUpdatedRecord(meta, record, fieldNames, newValues)
					recordsNeedToReInsert = append(recordsNeedToReInsert, updatedRecord)
				} else {
					updateRecordInPlace(meta, record, fieldNames, newValues)
				}
			}
		}

		err = utils2.WriteProtobufToBinaryFile(table, partitionFile)
		if err != nil {
			return fmt.Sprintf("Failed to write updated data to table %s, partition %d: %v", tName, pid, err)
		}
	}

	for _, record := range recordsNeedToReInsert {
		// Insert each record into the correct partition
		// The logic for insertIntoCorrectPartition should determine the correct partition and append the record to it
		err = insertIntoCorrectPartition(meta, record, tableDir, tName)
		if err != nil {
			return fmt.Sprintf("Failed to re-insert updated record into table %s: %v", tName, err)
		}
	}

	return "Table updated\n"
}

func updateRecordInPlace(meta *pb2.TableMeta, record *pb2.Record, fieldNames []string, newValues []string) {
	if len(fieldNames) != len(newValues) {
		// Error handling: The number of fields and values should be equal
		return
	}

	for i, fieldName := range fieldNames {
		newValue := newValues[i]

		// Update the field value based on the field name
		switch fieldName {
		case meta.PartitionKeyName:
			record.PartitionKeyValue = newValue
		case meta.SortKeyName:
			record.SortKeyValue = newValue
		default:
			// For other fields, find the corresponding field in OtherFieldsValues
			for j, name := range meta.OtherFieldsNames {
				if name == fieldName {
					record.OtherFieldsValues[j] = newValue
					break
				}
			}
		}
	}
}

func createUpdatedRecord(meta *pb2.TableMeta, originalRecord *pb2.Record, fieldNames []string, newValues []string) *pb2.Record {
	if len(fieldNames) != len(newValues) {
		// Error handling: The number of fields and values should be equal
		return nil
	}

	// Create a new record as a copy of the original
	newRecord := &pb2.Record{
		DeleteMark:        false,
		PartitionKeyValue: originalRecord.PartitionKeyValue,
		SortKeyValue:      originalRecord.SortKeyValue,
		OtherFieldsValues: make([]string, len(meta.OtherFieldsNames)),
	}
	copy(newRecord.OtherFieldsValues, originalRecord.OtherFieldsValues)

	// Update the new record with provided field values
	for i, fieldName := range fieldNames {
		newValue := newValues[i]

		switch fieldName {
		case meta.PartitionKeyName:
			newRecord.PartitionKeyValue = newValue
		case meta.SortKeyName:
			newRecord.SortKeyValue = newValue
		default:
			// For other fields, find and update the corresponding field in OtherFieldsValues
			for j, name := range meta.OtherFieldsNames {
				if name == fieldName {
					newRecord.OtherFieldsValues[j] = newValue
					break
				}
			}
		}
	}

	return newRecord
}

func insertIntoCorrectPartition(meta *pb2.TableMeta, record *pb2.Record, tableDir string, tName string) error {
	// Determine the correct partition ID
	partitionId := utils2.GetPartitionId(record.PartitionKeyValue, meta.Partitions)
	if partitionId < 1 || partitionId > meta.Partitions {
		return fmt.Errorf("invalid partition ID calculated for record")
	}

	// Construct the file name for the appropriate partition
	tableFile := filepath.Join(tableDir, fmt.Sprintf("%s-%d.data", tName, partitionId))

	// Read the current data from the partition
	table := &pb2.Table{}
	err := utils2.ReadProtobufFromBinaryFile(tableFile, table)
	if err != nil {
		return fmt.Errorf("failed to read partition data for partition %d: %v", partitionId, err)
	}

	// Append the new record to the partition data
	table.Records = append(table.Records, record)
	table.RecordsNumsAll = int64(len(table.Records))
	// Write the updated partition data back to the file
	err = utils2.WriteProtobufToBinaryFile(table, tableFile)
	if err != nil {
		return fmt.Errorf("failed to write updated data to partition %d: %v", partitionId, err)
	}

	return nil
}

func DeleteFromTable(tName string, filterFieldsOperationsValues [][]string) string {
	if globals.WorkingDatabasePosition == "" {
		return fmt.Sprintf("Please choose a database")
	}
	// Check if the table directory exists
	tableDir := globals.WorkingDatabasePosition + "/" + tName
	if _, err := os.Stat(tableDir); os.IsNotExist(err) {
		return fmt.Sprintf("Table %s does not exist", tName)
	}
	// Read the table's metadata
	meta := &pb2.TableMeta{}
	err := utils2.ReadProtobufFromBinaryFile(filepath.Join(tableDir, tName+".meta"), meta)
	if err != nil {
		return fmt.Sprintf("Failed to read metadata for table %s: %v", tName, err)
	}
	// Process each partition file
	for pid := int64(1); pid <= meta.Partitions; pid++ {
		partitionFile := filepath.Join(tableDir, fmt.Sprintf("%s-%d.data", tName, pid))

		// Read the current data from the partition
		table := &pb2.Table{}
		err = utils2.ReadProtobufFromBinaryFile(partitionFile, table)
		if err != nil {
			return fmt.Sprintf("Failed to read data for table %s, partition %d: %v", tName, pid, err)
		}

		// Update delete mark for records that match the filter
		for _, record := range table.Records {
			if utils2.MatchesFilter(meta, record, filterFieldsOperationsValues) {
				record.DeleteMark = true
			}
			table.RecordsNumsPendingDel += 1
		}

		// Write back the updated data
		err = utils2.WriteProtobufToBinaryFile(table, partitionFile)
		if err != nil {
			return fmt.Sprintf("Failed to write updated data to table %s, partition %d: %v", tName, pid, err)
		}
	}

	return fmt.Sprintf("Records deleted in table %s", tName)
}

func TruncateTable(tName string) string {
	if globals.WorkingDatabasePosition == "" {
		return fmt.Sprintf("Please choose a database")
	}

	// Check if the table directory exists
	tableDir := globals.WorkingDatabasePosition + "/" + tName
	if _, err := os.Stat(tableDir); os.IsNotExist(err) {
		return fmt.Sprintf("table %s does not exist", tName)
	}

	// Read the table's metadata to find out the number of partitions
	meta := &pb2.TableMeta{}
	err := utils2.ReadProtobufFromBinaryFile(filepath.Join(tableDir, tName+".meta"), meta)
	if err != nil {
		return fmt.Sprintf("failed to read metadata for table %s: %v", tName, err)
	}
	// Truncate each partition file
	for pid := int64(1); pid <= meta.Partitions; pid++ {
		partitionFile := filepath.Join(tableDir, fmt.Sprintf("%s-%d.data", tName, pid))
		err := os.Truncate(partitionFile, 0)
		if err != nil {
			return fmt.Sprintf("failed to truncate partition %d of table %s: %v", pid, tName, err)
		}

		// Recreate the file with initial structure if needed
		table := &pb2.Table{
			Records:               []*pb2.Record{},
			PartitionId:           pid,
			RecordsNumsAll:        0,
			RecordsNumsPendingDel: 0,
			Sorted:                true,
		}
		err = utils2.WriteProtobufToBinaryFile(table, partitionFile)
		if err != nil {
			return fmt.Sprintf("failed to recreate partition %d of table %s: %v", pid, tName, err)
		}
	}
	return "Truncated " + tName + "\n"
}
