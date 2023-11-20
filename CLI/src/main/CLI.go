package cli

import (
	"FlyFlyDB/Parser/src/main/request"
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
				//pre-processing
				//remove the suffix ";"
				line = strings.TrimSuffix(line, ";")
				lines = append(lines, line)
				req := concatenateLines(lines)
				req = strings.Trim(req, " ")
				//send req to Parser, Render output
				fmt.Println()
				fmt.Println(request.HandleRequest(req))
				break
			}
			lines = append(lines, line)
		}
	}
}

func concatenateLines(lines []string) string {
	var res strings.Builder
	for _, l := range lines {
		res.WriteString(l)
	}
	return res.String()
}