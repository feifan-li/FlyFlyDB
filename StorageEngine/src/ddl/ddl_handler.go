package ddl

import (
	utils "FlyFlyDB/StorageEngine/src/utils"
	pb "FlyFlyDB/StorageEngine/src/utils/pb"
	"FlyFlyDB/globals"
	"fmt"
	"os"
	"strconv"
)

func CreateDatabase(dbName string) string {

	err := os.Mkdir("./DB/"+dbName, 0755)

	if err != nil {
		fmt.Println(err)
		return "Failed to create a new database " + dbName
	} else {
		fmt.Println("success")
		return "Created a new database " + dbName
	}
}

func DropDatabase(dbName string) string {
	err := os.RemoveAll("./DB/" + dbName)
	if err != nil {
		fmt.Println(err)
		return "Failed to drop database " + dbName
	} else {
		fmt.Println("success")
		return "Dropped a database " + dbName
	}
}

func SwitchDatabase(dbName string) string {
	globals.WorkingDatabasePosition = "./DB/" + dbName
	return dbName
}

func CreateTable(tName string, partitionKey []string, sortKey []string, otherFields [][]string, partitions string) string {
	if globals.WorkingDatabasePosition == "" {
		return "Please choose a database"
	}
	err := os.Mkdir(globals.WorkingDatabasePosition+"/"+tName, 0755)
	if err != nil {
		fmt.Println(err)
		return "Cannot create table " + tName
	}
	err = os.Mkdir(globals.WorkingDatabasePosition+"/"+tName+"/tmp", 0755)
	if err != nil {
		fmt.Println(err)
		return "Cannot create table " + tName
	}
	meta := &pb.TableMeta{
		Partitions:       1,
		TotalFields:      int32(len(otherFields) + 2),
		PartitionKeyName: partitionKey[1],
		PartitionKeyType: partitionKey[0],
		SortKeyName:      sortKey[1],
		SortKeyType:      sortKey[0],
		OtherFieldsNames: []string{},
		OtherFieldsTypes: []string{},
	}
	if partitions != "" {
		meta.Partitions, _ = strconv.ParseInt(partitions, 10, 32)
	}

	for _, field := range otherFields {
		if len(field) != 2 {
			return "Syntax Error"
		}
		fieldType := field[0] //fieldType
		fieldName := field[1] //fieldName
		meta.OtherFieldsTypes = append(meta.OtherFieldsTypes, fieldType)
		meta.OtherFieldsNames = append(meta.OtherFieldsNames, fieldName)
	}

	//create a meta file per table
	err = utils.WriteProtobufToBinaryFile(meta, globals.WorkingDatabasePosition+"/"+tName+"/"+tName+".meta")
	if err != nil {
		fmt.Println(err)
		return "Cannot create table " + tName
	}

	//create data files, # = partitions
	var pid int64
	for pid = 1; pid <= meta.Partitions; pid++ {
		table := &pb.Table{
			Records:               []*pb.Record{},
			PartitionId:           pid,
			RecordsNumsAll:        0,
			RecordsNumsPendingDel: 0,
			Sorted:                true,
		}
		err = utils.WriteProtobufToBinaryFile(table, globals.WorkingDatabasePosition+"/"+tName+"/"+tName+"-"+strconv.FormatInt(pid, 10)+".data")
		if err != nil {
			fmt.Println(err)
			return "Cannot create table " + tName
		}
	}

	return fmt.Sprintf("created table %s\n", tName)
}

func DropTable(tName string) string {
	if globals.WorkingDatabasePosition == "" {
		return "Please choose a database"
	}
	err := os.RemoveAll(globals.WorkingDatabasePosition + "/" + tName)
	if err != nil {
		fmt.Println(err)
		return "Cannot drop table " + tName
	}
	return "Dropped table " + tName
}
