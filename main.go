package main

import (
	cli "FlyFlyDB/CLI/src/main"
	engine_test "FlyFlyDB/StorageEngine/src/test"
)

func main() {
	//engine_test.SimpleTest()
	//engine_test.YoutubeTest()
	engine_test.YoutubeDemoLoadData()
	cli.StartCLI()
}
