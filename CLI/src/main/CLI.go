package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func StartCLI() {
	for {
		scanner := bufio.NewScanner(os.Stdin)
		var lines []string
		for {
			if len(lines) == 0 {
				fmt.Printf("FlyFlyDB>")
			} else {
				fmt.Printf(".........")
			}
			scanner.Scan()
			line := scanner.Text()
			if line == "bye" {
				os.Exit(0)
			}
			if strings.HasSuffix(line, ";") {
				lines = append(lines, line)
				//Render output
				fmt.Println("ok")
				break
			}
			lines = append(lines, line)
		}
	}
}
