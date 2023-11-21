package engine_test

import (
	"FlyFlyDB/StorageEngine/src/main/ddl"
	"FlyFlyDB/StorageEngine/src/main/dml"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"time"
)

type YouTubeCategoryListResponse struct {
	Kind  string `json:"kind"`
	Etag  string `json:"etag"`
	Items []struct {
		Kind    string `json:"kind"`
		Etag    string `json:"etag"`
		ID      string `json:"id"`
		Snippet struct {
			ChannelId  string `json:"channelId"`
			Title      string `json:"title"`
			Assignable bool   `json:"assignable"`
		} `json:"snippet"`
	} `json:"items"`
}

func YoutubeDemoLoadData() {
	rand.Seed(time.Now().UnixNano())
	/*Create database and tables for demo*/
	ddl.DropDatabase("YoutubeDemo")
	ddl.CreateDatabase("YoutubeDemo")
	ddl.SwitchDatabase("YoutubeDemo")
	ddl.DropTable("category")
	ddl.DropTable("video")
	ddl.CreateTable("category", []string{"string", "country_code"}, []string{"int32", "id"},
		[][]string{{"string", "name"}}, "10")
	ddl.CreateTable("video", []string{"string", "country_code"}, []string{"string", "id"},
		[][]string{{"string", "title"}, {"string", "channel_title"}, {"int32", "category_id"},
			{"string", "publish_time"}, {"int64", "views"}, {"int64", "likes"}, {"int64", "dislikes"}}, "10")
	/*Read the category files*/
	countryCodes := []string{"US", "CA", "DE", "FR", "GB", "IN", "MX", "RU"}
	categorySources := []string{"./dataset/US_category_id.json", "./dataset/CA_category_id.json",
		"./dataset/DE_category_id.json", "./dataset/FR_category_id.json",
		"./dataset/GB_category_id.json", "./dataset/IN_category_id.json",
		"./dataset/MX_category_id.json", "./dataset/RU_category_id.json"}
	videoSources := []string{"./dataset/USvideos.csv", "./dataset/CAvideos.csv",
		"./dataset/DEvideos.csv", "./dataset/FRvideos.csv",
		"./dataset/GBvideos.csv", "./dataset/INvideos.csv",
		"./dataset/MXvideos.csv", "./dataset/RUvideos.csv"}
	for i, categorySource := range categorySources {
		countryCode := countryCodes[i]
		file, err := os.Open(categorySource)
		if err != nil {
			log.Fatalf("Error opening file: %v", err)
		}
		defer file.Close()
		byteValue, _ := io.ReadAll(file)
		var response YouTubeCategoryListResponse
		err = json.Unmarshal(byteValue, &response)
		if err != nil {
			log.Fatalf("Error unmarshalling JSON: %v", err)
		}
		for _, item := range response.Items {
			dml.InsertIntoTable("category",
				[][]string{{"country_code", countryCode}, {"id", item.ID}, {"name", item.Snippet.Title}})
		}
	}
	for i, videoSource := range videoSources {
		countryCode := countryCodes[i]
		file, err := os.Open(videoSource)
		if err != nil {
			log.Fatalf("Error opening file '%s': %v", videoSource, err)
		}
		defer file.Close()
		reader := csv.NewReader(file)
		_, err = reader.Read()
		if err != nil {
			log.Fatalf("Error reading header from file '%s': %v", videoSource, err)
		}
		cnt := 0
		// Iterate through the remaining records
		for {
			record, err := reader.Read()
			if err != nil {
				if err.Error() == "EOF" {
					break // End of file is reached
				}
			} else {
				dml.InsertIntoTable("video", [][]string{{"country_code", countryCode}, {"id", record[0]}, {"title", first128(record[2])},
					{"channel_title", first128(record[3])}, {"category_id", record[4]}, {"publish_time", record[5]},
					{"views", record[7]}, {"likes", record[8]}, {"dislikes", record[9]}})
				cnt += 1
				if cnt > 1000 {
					break
				}
			}
		}
		fmt.Println(videoSource + " loaded")
	}
}
func first128(s string) string {
	if len(s) > 128 {
		return s[:128]
	}
	return s
}
