package engine_test

import (
	"FlyFlyDB/StorageEngine/src/ddl"
	"FlyFlyDB/StorageEngine/src/dml"
	"fmt"
)

func EngineTest() {
	fmt.Println("Hello")
	ddl.DropDatabase("dbtest2")
	ddl.CreateDatabase("dbtest2")
	ddl.SwitchDatabase("dbtest2")
	dml.TruncateTable("person")
	ddl.DropTable("person")
	ddl.CreateTable("person",
		[]string{"string", "id"},
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

	dml.SelectFromTable("person", []string{}, [][]string{}, []string{})

	dml.DeleteFromTable("person", [][]string{})
	dml.SelectFromTable("person", []string{}, [][]string{}, []string{})
}
