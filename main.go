package main

import (
	cli "FlyFlyDB/CLI/src/main"
	"FlyFlyDB/StorageEngine/src/test"
)

func main() {
	//engine_test.SimpleTest()
	engine_test.YoutubeTest()
	cli.StartCLI()
}
