package style

import (
	pb2 "FlyFlyDB/StorageEngine/src/main/utils/pb"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func RenderGroupedRecords(meta *pb2.TableMeta, groupBy string, groupedRecords map[string][]pb2.Record,
	projection []string, aggField string, aggFunc string, aggregatedResults map[string]string, limit string) {
	if limit == "" {
		limit = "2147483647"
	}
	if aggFunc != "" || aggField != "" || aggregatedResults != nil {
		//Aggregation Function is used:
		RenderAggregatedResults(groupBy, aggField, aggFunc, aggregatedResults)
	} else {
		//Aggregation Function is not used:

		//determine columns to render
		projectionMap := make(map[string]bool)
		projectionMap[meta.PartitionKeyName] = false
		projectionMap[meta.SortKeyName] = false
		for _, fieldName := range meta.OtherFieldsNames {
			projectionMap[fieldName] = false
		}
		if len(projection) == 0 {
			fmt.Println("please specify columns to select")
			return
		}
		if projection[0] == "*" {
			//projection for all fields
			for key, _ := range projectionMap {
				projectionMap[key] = true
			}
		} else {
			//projection for given fields
			for _, key := range projection {
				projectionMap[key] = true
			}
		}

		// Determine the maximum width for each column
		columnWidths := make(map[string]int)
		totalWidth := 0
		columnWidths[meta.PartitionKeyName] = len(meta.PartitionKeyName) + 3
		columnWidths[meta.SortKeyName] = len(meta.SortKeyName) + 3
		for _, fieldName := range meta.OtherFieldsNames {
			columnWidths[fieldName] = len(fieldName) + 3
		}
		for _, records := range groupedRecords {
			for _, record := range records {
				if len(record.PartitionKeyValue)+3 > columnWidths[meta.PartitionKeyName] {
					columnWidths[meta.PartitionKeyName] = len(record.PartitionKeyValue) + 3
				}
				if len(record.SortKeyValue)+3 > columnWidths[meta.SortKeyName] {
					columnWidths[meta.SortKeyName] = len(record.SortKeyValue) + 3
				}
				for fid, fieldValue := range record.OtherFieldsValues {
					if len(fieldValue)+3 > columnWidths[meta.OtherFieldsNames[fid]] {
						columnWidths[meta.OtherFieldsNames[fid]] = len(fieldValue) + 3
					}
				}
			}
		}
		for column, len := range columnWidths {
			if projectionMap[column] {
				totalWidth += len + 1
			}
		}

		//sort groupKeys
		var groupKeys []string
		for groupKey, _ := range groupedRecords {
			groupKeys = append(groupKeys, groupKey)
		}
		sort.Slice(groupKeys, func(i, j int) bool {
			valI := groupKeys[i]
			valJ := groupKeys[j]
			return valI < valJ
		})

		// Print header
		fmt.Println("*" + strings.Repeat("-", totalWidth-1) + "*")
		if projectionMap[meta.PartitionKeyName] {
			fmt.Printf("%-*s ", columnWidths[meta.PartitionKeyName], "| "+meta.PartitionKeyName)
		}
		if projectionMap[meta.SortKeyName] {
			fmt.Printf("%-*s ", columnWidths[meta.SortKeyName], "| "+meta.SortKeyName)
		}
		for _, fieldName := range meta.OtherFieldsNames {
			if projectionMap[fieldName] {
				fmt.Printf("%-*s ", columnWidths[fieldName], "| "+fieldName)
			}
		}
		fmt.Println("|")
		fmt.Println("*" + strings.Repeat("-", totalWidth-1) + "*")

		// Print rows, rows are first sorted by groupKey(partitionKey/groupBy) and then sorted By sortBy/sortKey
		// limit rows rendered for each group
		limitInt, _ := strconv.ParseInt(limit, 10, 32)
		for _, groupKey := range groupKeys {
			curGroupRenderedCount := 0
			for _, record := range groupedRecords[groupKey] {
				if int64(curGroupRenderedCount) >= limitInt {
					break
				}
				//projection here:
				if projectionMap[meta.PartitionKeyName] {
					fmt.Printf("%-*s ", columnWidths[meta.PartitionKeyName], "| "+record.PartitionKeyValue)
				}
				if projectionMap[meta.SortKeyName] {
					fmt.Printf("%-*s ", columnWidths[meta.SortKeyName], "| "+record.SortKeyValue)
				}
				for fid, fieldName := range meta.OtherFieldsNames {
					if projectionMap[fieldName] {
						fmt.Printf("%-*s ", columnWidths[fieldName], "| "+record.OtherFieldsValues[fid])
					}
				}
				fmt.Println("|")
				curGroupRenderedCount += 1
			}
		}
		fmt.Println("*" + strings.Repeat("-", totalWidth-1) + "*")
	}
}

func RenderAggregatedResults(groupBy string, aggField string, aggFunc string, aggregatedResults map[string]string) {
	// Determine the maximum width for each column
	columnWidths := make(map[string]int)
	totalWidth := 0
	columnWidths[groupBy] = len(groupBy) + 3
	columnWidths[aggFunc+"("+aggField+")"] = len(aggFunc+"("+aggField+")") + 3
	for key, _ := range aggregatedResults {
		if len(key)+3 > columnWidths[groupBy] {
			columnWidths[groupBy] = len(key) + 3
		}
	}
	for _, val := range aggregatedResults {
		if len(val)+3 > columnWidths[aggFunc+"("+aggField+")"] {
			columnWidths[aggFunc+"("+aggField+")"] = len(val) + 3
		}
	}
	for _, len := range columnWidths {
		totalWidth += len + 1
	}
	//sort groupKeys
	var groupKeys []string
	for groupKey, _ := range aggregatedResults {
		groupKeys = append(groupKeys, groupKey)
	}
	sort.Slice(groupKeys, func(i, j int) bool {
		valI := groupKeys[i]
		valJ := groupKeys[j]
		return valI < valJ
	})
	// Print header
	fmt.Println("*" + strings.Repeat("-", totalWidth-1) + "*")
	fmt.Printf("%-*s ", columnWidths[groupBy], "| "+groupBy)
	fmt.Printf("%-*s ", columnWidths[aggFunc+"("+aggField+")"], "| "+aggFunc+"("+aggField+")")
	fmt.Println("|")
	fmt.Println("*" + strings.Repeat("-", totalWidth-1) + "*")
	// Print rows, rows are first sorted by groupKey(partitionKey/groupBy) and then sorted By sortBy/sortKey
	for _, groupKey := range groupKeys {
		val := aggregatedResults[groupKey]
		fmt.Printf("%-*s ", columnWidths[groupBy], "| "+groupKey)
		fmt.Printf("%-*s ", columnWidths[aggFunc+"("+aggField+")"], "| "+val)
		fmt.Println("|")
	}

	fmt.Println("*" + strings.Repeat("-", totalWidth-1) + "*")
}
