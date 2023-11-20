package style

import (
	pb2 "FlyFlyDB/StorageEngine/src/main/utils/pb"
	"fmt"
	"sort"
	"strings"
)

func RenderGroupedRecords(meta *pb2.TableMeta, groupedRecords map[string][]pb2.Record,
	aggField string, aggFunc string, aggregatedResults map[string]string) {
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
			if len(record.PartitionKeyValue) > columnWidths[meta.PartitionKeyName] {
				columnWidths[meta.PartitionKeyName] = len(record.PartitionKeyValue) + 3
			}
			if len(record.SortKeyValue) > columnWidths[meta.SortKeyName] {
				columnWidths[meta.SortKeyName] = len(record.SortKeyValue) + 3
			}
			for fid, fieldValue := range record.OtherFieldsValues {
				if len(fieldValue) > columnWidths[meta.OtherFieldsNames[fid]] {
					columnWidths[meta.OtherFieldsNames[fid]] = len(fieldValue) + 3
				}
			}
		}
	}
	for _, len := range columnWidths {
		totalWidth += len + 1
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
	fmt.Printf("%-*s ", columnWidths[meta.PartitionKeyName], "| "+meta.PartitionKeyName)
	fmt.Printf("%-*s ", columnWidths[meta.SortKeyName], "| "+meta.SortKeyName)
	for _, fieldName := range meta.OtherFieldsNames {
		fmt.Printf("%-*s ", columnWidths[fieldName], "| "+fieldName)
	}
	fmt.Println("|")
	fmt.Println("*" + strings.Repeat("-", totalWidth-1) + "*")

	// Print rows, rows are first sorted by groupKey(partitionKey/groupBy) and then sorted By sortBy/sortKey
	for _, groupKey := range groupKeys {
		for _, record := range groupedRecords[groupKey] {
			fmt.Printf("%-*s ", columnWidths[meta.PartitionKeyName], "| "+record.PartitionKeyValue)
			fmt.Printf("%-*s ", columnWidths[meta.SortKeyName], "| "+record.SortKeyValue)
			for fid, fieldName := range meta.OtherFieldsNames {
				fmt.Printf("%-*s ", columnWidths[fieldName], "| "+record.OtherFieldsValues[fid])
			}
			fmt.Println("|")
		}
	}
	fmt.Println("*" + strings.Repeat("-", totalWidth-1) + "*")
	if aggFunc != "" || aggField != "" || aggregatedResults != nil {
		for key, val := range aggregatedResults {
			fmt.Printf(aggFunc + "(" + aggField + ")" + " of" + " group " + key + ": " + val + "\n")
		}
	}
}
