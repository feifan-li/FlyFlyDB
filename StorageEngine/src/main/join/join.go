package join

import (
	"FlyFlyDB/StorageEngine/src/main/ddl"
	"FlyFlyDB/StorageEngine/src/main/dml"
	utils2 "FlyFlyDB/StorageEngine/src/main/utils"
	pb2 "FlyFlyDB/StorageEngine/src/main/utils/pb"
	"FlyFlyDB/globals"
	"fmt"
	"github.com/google/uuid"
	"path/filepath"
	"strconv"
)

func JoinTwoTables(tables []string, onFilters [][]string, selectFilters [][]string) (string, error) {
	if globals.WorkingDatabasePosition == "" {
		fmt.Printf("Please choose a database\n")
		return "", fmt.Errorf("Please choose a database\n")
	}
	if len(tables) != 2 {
		return "", fmt.Errorf("FlyFlyDB does not support joining more than two tables")
	}
	t1Name := tables[0]
	t2Name := tables[1]

	/****read meta data****/
	table1Dir := globals.WorkingDatabasePosition + "/" + t1Name
	meta1 := &pb2.TableMeta{}
	err := utils2.ReadProtobufFromBinaryFile(table1Dir+"/"+t1Name+".meta", meta1)
	if err != nil {
		fmt.Printf(err.Error())
		return "", fmt.Errorf("Cannot read meta data\n")
	}
	table2Dir := globals.WorkingDatabasePosition + "/" + t2Name
	meta2 := &pb2.TableMeta{}
	err = utils2.ReadProtobufFromBinaryFile(table2Dir+"/"+t2Name+".meta", meta2)
	if err != nil {
		fmt.Printf(err.Error())
		return "", fmt.Errorf("Cannot read meta data\n")
	}

	/****Create a temp table****/
	tempName := t1Name + "_" + t2Name + "_" + uuid.New().String()[:8]
	tempPartitionKey := []string{meta1.PartitionKeyType, t1Name + "." + meta1.PartitionKeyName}
	tempSortKey := []string{meta1.SortKeyType, t1Name + "." + meta1.SortKeyName}
	var tempOtherFields [][]string
	//otherFields in t1
	for i, fieldType := range meta1.OtherFieldsTypes {
		tempOtherFields = append(tempOtherFields, []string{fieldType, t1Name + "." + meta1.OtherFieldsNames[i]})
	}
	//partionKey in t2
	tempOtherFields = append(tempOtherFields, []string{meta2.PartitionKeyType, t2Name + "." + meta2.PartitionKeyName})
	//sortKey in t2
	tempOtherFields = append(tempOtherFields, []string{meta2.SortKeyType, t2Name + "." + meta2.SortKeyName})
	//otherFields in t2
	for i, fieldType := range meta2.OtherFieldsTypes {
		tempOtherFields = append(tempOtherFields, []string{fieldType, t2Name + "." + meta2.OtherFieldsNames[i]})
	}
	ddl.CreateTable(tempName, tempPartitionKey, tempSortKey, tempOtherFields, "2")
	/****get temp table's meta data****/
	tempMeta := &pb2.TableMeta{}
	tempTableDir := globals.WorkingDatabasePosition + "/" + tempName
	err = utils2.ReadProtobufFromBinaryFile(filepath.Join(tempTableDir, tempName+".meta"), tempMeta)
	if err != nil {
		return fmt.Sprintf("Failed to read metadata for table %s: %v", tempName, err), err
	}
	/****read out records1,records2****/
	var records1 []pb2.Record
	var records2 []pb2.Record
	var pid int64
	for pid = 1; pid <= meta1.Partitions; pid++ {
		table1 := &pb2.Table{}
		err = utils2.ReadProtobufFromBinaryFile(table1Dir+"/"+t1Name+"-"+strconv.FormatInt(pid, 10)+".data", table1)
		if err != nil {
			return tempName, fmt.Errorf("Failed to read data for table %s, partition %d: %v\n", t1Name, pid, err)
		}
		for _, record := range table1.Records {
			records1 = append(records1, *record)
		}
	}
	for pid = 1; pid <= meta2.Partitions; pid++ {
		table2 := &pb2.Table{}
		err = utils2.ReadProtobufFromBinaryFile(table2Dir+"/"+t2Name+"-"+strconv.FormatInt(pid, 10)+".data", table2)
		if err != nil {
			return tempName, fmt.Errorf("Failed to read data for table %s, partition %d: %v\n", t2Name, pid, err)
		}
		for _, record := range table2.Records {
			records2 = append(records2, *record)
		}
	}

	for _, r1 := range records1 {
		for _, r2 := range records2 {
			if utils2.JoinRecordsMatchFilter(t1Name, meta1, &r1, t2Name, meta2, &r2, onFilters) {
				var fieldAndValues [][]string
				fieldAndValues = append(fieldAndValues, []string{t1Name + "." + meta1.PartitionKeyName, r1.PartitionKeyValue})
				fieldAndValues = append(fieldAndValues, []string{t1Name + "." + meta1.SortKeyName, r1.SortKeyValue})
				for i, otherFieldsName := range meta1.OtherFieldsNames {
					fieldAndValues = append(fieldAndValues, []string{t1Name + "." + otherFieldsName, r1.OtherFieldsValues[i]})
				}
				fieldAndValues = append(fieldAndValues, []string{t2Name + "." + meta2.PartitionKeyName, r2.PartitionKeyValue})
				fieldAndValues = append(fieldAndValues, []string{t2Name + "." + meta2.SortKeyName, r2.SortKeyValue})
				for i, otherFieldsName := range meta2.OtherFieldsNames {
					fieldAndValues = append(fieldAndValues, []string{t2Name + "." + otherFieldsName, r2.OtherFieldsValues[i]})
				}
				/****if also Matches With Select Filter****/
				if len(selectFilters) == 0 {
					dml.InsertIntoTable(tempName, fieldAndValues)
				}
				for _, filter := range selectFilters {
					if len(filter) != 3 {
						break
					}
					fieldName, operator, value := filter[0], filter[1], filter[2]
					valid := false
					var fieldValue string
					//fieldName should appear in fieldAndValues
					for _, fieldAndValue := range fieldAndValues {
						if fieldName == fieldAndValue[0] {
							fieldValue = fieldAndValue[1]
							valid = true
							break
						}
					}
					if !valid || !utils2.ApplyFilter(fieldValue, operator, value) {
						break
					} else {
						dml.InsertIntoTable(tempName, fieldAndValues)
					}
				}
			}
		}
	}
	return tempName, nil
}
