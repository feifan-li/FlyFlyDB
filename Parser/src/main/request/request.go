package request

import (
	"FlyFlyDB/StorageEngine/src/main/agg"
	"FlyFlyDB/StorageEngine/src/main/ddl"
	"FlyFlyDB/StorageEngine/src/main/dml"
	"FlyFlyDB/StorageEngine/src/main/join"
	"encoding/json"
	"fmt"
	"strings"
)

type CreateDatabaseReq struct {
	Database string `json:"database"`
}
type SwitchDatabaseReq struct {
	Database string `json:"database"`
}
type DropDatabaseReq struct {
	Database string `json:"database"`
}
type CreateTableReq struct {
	Table        string   `json:"table"`
	PartitionKey string   `json:"partition_key"`
	SortKey      string   `json:"sort_key"`
	Fields       []string `json:"fields"`
	Partitions   string   `json:"partitions"`
}
type TruncateTableReq struct {
	Table string `json:"table"`
}
type DropTableReq struct {
	Table string `json:"table"`
}
type JoinReq struct {
	Tables []string `json:"tables"`
	On     []string `json:"on"`
}
type SelectTableReq struct {
	Join       *JoinReq `json:"join,omitempty"` // Pointer to allow absence
	Table      string   `json:"table,omitempty"`
	Projection []string `json:"projection"`
	Filter     []string `json:"filter,omitempty"`
	GroupBy    string   `json:"group_by,omitempty"`
	SortBy     string   `json:"sort_by,omitempty"`
	Limit      string   `json:"limit,omitempty"`
}
type InsertTableReq struct {
	Table  string   `json:"table"`
	Values []string `json:"values"`
}
type UpdateRecordReq struct {
	Table  string   `json:"table"`
	Filter []string `json:"filter,omitempty"`
	Fields []string `json:"fields"`
	Values []string `json:"values"`
}
type DeleteRecordReq struct {
	Table  string   `json:"table"`
	Filter []string `json:"filter,omitempty"`
}

func HandleRequest(reqStr string) string {
	if strings.HasPrefix(reqStr, "create:") {
		reqStr = strings.TrimPrefix(reqStr, "create:")
		//create db
		var createDatabaseReq CreateDatabaseReq
		err := json.Unmarshal([]byte(reqStr), &createDatabaseReq)
		if err == nil {
			if createDatabaseReq.Database != "" {
				return HandleCreateDatabaseRequest(createDatabaseReq)
			}
		}
		//or create table
		var createTableReq CreateTableReq
		err = json.Unmarshal([]byte(reqStr), &createTableReq)
		if err == nil {
			if createTableReq.Table != "" {
				return HandleCreateTableRequest(createTableReq)
			}
		}
	} else if strings.HasPrefix(reqStr, "drop:") {
		reqStr = strings.TrimPrefix(reqStr, "drop:")
		//drop db
		var dropDatabaseReq DropDatabaseReq
		err := json.Unmarshal([]byte(reqStr), &dropDatabaseReq)
		if err == nil {
			if dropDatabaseReq.Database != "" {
				return HandleDropDatabaseRequest(dropDatabaseReq)
			}
		}
		//or drop table
		var dropTableReq DropTableReq
		err = json.Unmarshal([]byte(reqStr), &dropTableReq)
		if err == nil {
			if dropTableReq.Table != "" {
				return HandleDropTableRequest(dropTableReq)
			}
		}
	} else if strings.HasPrefix(reqStr, "use:") {
		reqStr = strings.TrimPrefix(reqStr, "use:")
		//switch db
		var switchDatabaseReq SwitchDatabaseReq
		err := json.Unmarshal([]byte(reqStr), &switchDatabaseReq)
		if err == nil {
			return HandleSwitchDatabaseRequest(switchDatabaseReq)
		}
	} else if strings.HasPrefix(reqStr, "clear:") {
		reqStr = strings.TrimPrefix(reqStr, "clear:")
		//truncate table
		var truncateTableReq TruncateTableReq
		err := json.Unmarshal([]byte(reqStr), &truncateTableReq)
		if err == nil {
			return HandleTruncateTableRequest(truncateTableReq)
		}
	} else if strings.HasPrefix(reqStr, "select:") {
		reqStr = strings.TrimPrefix(reqStr, "select:")
		var selectTableReq SelectTableReq
		err := json.Unmarshal([]byte(reqStr), &selectTableReq)
		if err == nil {
			if selectTableReq.Join == nil && selectTableReq.Table != "" {
				HandleSelectTableRequest(selectTableReq)
				return ""
			} else if selectTableReq.Join != nil && selectTableReq.Table == "" {
				tempName, err := HandleJoinTableRequest(selectTableReq)
				if err != nil {
					return err.Error()
				}
				//TODO:HandleSelectTableRequest
				var tempSelectTableReq SelectTableReq
				tempSelectTableReq.Join = nil
				tempSelectTableReq.Table = tempName
				tempSelectTableReq.Projection = selectTableReq.Projection
				tempSelectTableReq.Filter = selectTableReq.Filter
				tempSelectTableReq.SortBy = selectTableReq.SortBy
				tempSelectTableReq.Limit = selectTableReq.Limit
				HandleSelectTableRequest(tempSelectTableReq)
				HandleDropTableRequest(DropTableReq{tempName})
				return ""
			}
		}
	} else if strings.HasPrefix(reqStr, "insert:") {
		reqStr = strings.TrimPrefix(reqStr, "insert:")
		var insertTableReq InsertTableReq
		err := json.Unmarshal([]byte(reqStr), &insertTableReq)
		if err == nil {
			var fieldsAndValues [][]string
			for _, str := range insertTableReq.Values {
				fieldsAndValues = append(fieldsAndValues, strings.Split(str, "="))
			}
			return dml.InsertIntoTable(insertTableReq.Table, fieldsAndValues)
		}
	} else if strings.HasPrefix(reqStr, "update:") {
		reqStr = strings.TrimPrefix(reqStr, "update:")
		var updateRecordReq UpdateRecordReq
		err := json.Unmarshal([]byte(reqStr), &updateRecordReq)
		if err == nil {
			var filterFieldsOperationsValues [][]string
			for _, filter := range updateRecordReq.Filter {
				filterFieldsOperationsValues = append(filterFieldsOperationsValues, strings.Fields(filter))
			}
			return dml.UpdateTable(updateRecordReq.Table, filterFieldsOperationsValues, updateRecordReq.Fields, updateRecordReq.Values)
		}
	} else if strings.HasPrefix(reqStr, "delete:") {
		reqStr = strings.TrimPrefix(reqStr, "delete:")
		var deleteRecordReq DeleteRecordReq
		err := json.Unmarshal([]byte(reqStr), &deleteRecordReq)
		if err == nil {
			var filterFieldsOperationsValues [][]string
			for _, filter := range deleteRecordReq.Filter {
				filterFieldsOperationsValues = append(filterFieldsOperationsValues, strings.Fields(filter))
			}
			return dml.DeleteFromTable(deleteRecordReq.Table, filterFieldsOperationsValues)
		}
	}
	return "syntax error"
}

func HandleCreateDatabaseRequest(req CreateDatabaseReq) string {
	resp := ddl.CreateDatabase(req.Database)
	return resp
}
func HandleSwitchDatabaseRequest(req SwitchDatabaseReq) string {
	resp := ddl.SwitchDatabase(req.Database)
	return resp
}
func HandleDropDatabaseRequest(req DropDatabaseReq) string {
	resp := ddl.DropDatabase(req.Database)
	return resp
}
func HandleCreateTableRequest(req CreateTableReq) string {
	var otherFields [][]string
	for _, str := range req.Fields {
		otherFields = append(otherFields, strings.Fields(str))
	}
	resp := ddl.CreateTable(req.Table, strings.Fields(req.PartitionKey), strings.Fields(req.SortKey),
		otherFields, req.Partitions)
	return resp
}
func HandleTruncateTableRequest(req TruncateTableReq) string {
	resp := dml.TruncateTable(req.Table)
	return resp
}
func HandleDropTableRequest(req DropTableReq) string {
	resp := ddl.DropTable(req.Table)
	return resp
}
func HandleSelectTableRequest(req SelectTableReq) {
	var filters [][]string
	for _, str := range req.Filter {
		filters = append(filters, strings.Fields(str))
	}
	aggField := ""
	aggFunc := ""
	hasAggField := false
	for _, p := range req.Projection {
		if field, function, isAggField := agg.IsAggregationField(p); isAggField {
			aggField = field
			aggFunc = function
			hasAggField = true
			break
		}
	}
	if hasAggField {
		_, _, err := dml.SelectFromTable(req.Table, req.Projection, filters, req.GroupBy, aggField, aggFunc, req.SortBy, "1")
		if err != nil {
			fmt.Errorf("Select failed " + err.Error())
		}
	} else {
		_, _, err := dml.SelectFromTable(req.Table, req.Projection, filters, req.GroupBy, "", "", req.SortBy, req.Limit)
		if err != nil {
			fmt.Errorf("Select failed " + err.Error())
		}
	}
}
func HandleJoinTableRequest(req SelectTableReq) (string, error) {
	var joinFilters [][]string
	for _, filter := range req.Join.On {
		joinFilters = append(joinFilters, strings.Fields(filter))
	}
	var filters [][]string
	for _, str := range req.Filter {
		filters = append(filters, strings.Fields(str))
	}
	tempName, err := join.JoinTwoTables(req.Join.Tables, joinFilters, filters)
	if err != nil {
		return "", fmt.Errorf("Failed when joining two tables " + err.Error())
	}
	return tempName, nil
}
