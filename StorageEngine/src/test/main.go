package engine_test

import (
	"FlyFlyDB/StorageEngine/src/main/ddl"
	"FlyFlyDB/StorageEngine/src/main/dml"
)

func SimpleTest() {
	ddl.DropDatabase("dbtest2")
	ddl.CreateDatabase("dbtest2")
	ddl.SwitchDatabase("dbtest2")
	dml.TruncateTable("person")
	ddl.DropTable("person")
	ddl.CreateTable("person",
		[]string{"int32", "id"},
		[]string{"string", "name"},
		[][]string{{"string", "address"}, {"int32", "age"}},
		"2")
	dml.InsertIntoTable("person", [][]string{{"id", "0"}, {"name", "Alice"}, {"address", "1334 W 22nd St"}, {"age", "25"}})
	dml.InsertIntoTable("person", [][]string{{"id", "1"}, {"name", "Spencer"}, {"address", "2637 Ellendale"}, {"age", "23"}})
	dml.InsertIntoTable("person", [][]string{{"id", "5"}, {"name", "Feifan"}, {"address", "W 30th St"}, {"age", "23"}})
	dml.InsertIntoTable("person", [][]string{{"id", "2"}, {"name", "Yankang"}, {"address", "2623 Ellendale"}, {"age", "24"}})
	dml.InsertIntoTable("person", [][]string{{"id", "4"}, {"name", "Morty"}, {"address", "New York"}, {"age", "17"}})
	dml.InsertIntoTable("person", [][]string{{"id", "3"}, {"name", "Bob"}, {"address", "2637 Ellendale"}, {"age", "23"}})

	dml.DeleteFromTable("person", [][]string{{"id", "=", "1"}})
	dml.DeleteFromTable("person", [][]string{{"age", ">=", "25"}})

	dml.UpdateTable("person", [][]string{{"name", "is", "Morty"}, {"address", "=", "New York"}}, []string{"name"}, []string{"Morty Sanchez"})
	dml.UpdateTable("person", [][]string{{"id", "<=", "2"}}, []string{"id"}, []string{"1"})
	dml.UpdateTable("person", [][]string{}, []string{"address"}, []string{"University of Southern California"})

	dml.InsertIntoTable("person", [][]string{{"id", "6"}, {"name", "Rick"}, {"address", "Earth"}, {"age", "60"}})
	dml.InsertIntoTable("person", [][]string{{"id", "7"}, {"name", "Summer"}, {"address", "Spring Field"}, {"age", "24"}})
	dml.InsertIntoTable("person", [][]string{{"id", "100"}, {"name", "Thomas"}, {"address", "Spring Field"}, {"age", "24"}})

	dml.SelectFromTable("person", []string{}, [][]string{}, "age", "id", "max", "id", "")
}

func YoutubeTest() {
	ddl.DropDatabase("Youtube")
	ddl.CreateDatabase("Youtube")
	ddl.SwitchDatabase("Youtube")
	ddl.DropTable("category")
	ddl.DropTable("video")
	ddl.CreateTable("category", []string{"string", "country_code"}, []string{"int32", "id"},
		[][]string{{"string", "name"}}, "2")
	ddl.CreateTable("video", []string{"string", "country_code"}, []string{"string", "id"},
		[][]string{{"string", "title"}, {"string", "channel_title"}, {"int32", "category_id"},
			{"string", "publish_time"}, {"int64", "views"}, {"int64", "likes"}, {"int64", "dislikes"}}, "2")
	dml.InsertIntoTable("category", [][]string{{"country_code", "US"}, {"id", "1"}, {"name", "Film & Animation"}})
	dml.InsertIntoTable("category", [][]string{{"country_code", "US"}, {"id", "2"}, {"name", "Autos & Vehicles"}})
	dml.InsertIntoTable("category", [][]string{{"country_code", "US"}, {"id", "10"}, {"name", "Music"}})
	dml.InsertIntoTable("category", [][]string{{"country_code", "US"}, {"id", "15"}, {"name", "Pets & Animals"}})

	dml.InsertIntoTable("video", [][]string{{"country_code", "US"}, {"id", "2kyS6SvSYSE"}, {"title", "WE WANT TO TALK ABOUT OUR MARRIAGE"},
		{"channel_title", "CaseyNeistat"}, {"category_id", "22"}, {"publish_time", "2017-11-13T17:13:01.000Z"},
		{"views", "748374"}, {"likes", "57527"}, {"dislikes", "2966"}})

	dml.SelectFromTable("category", []string{"country_code,max(id)"}, [][]string{}, "name", "id", "max", "", "")
	dml.SelectFromTable("video", []string{"country_code", "title", "views"}, [][]string{}, "", "", "", "", "")

}

func YoutubeDemoLoadData() {

}
